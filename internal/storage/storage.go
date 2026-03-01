package storage

import (
	"errors"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Storage is in-memory key-value store with TTL
// Deletions use tombstone marks: DEL writes a tombstone so it replicates consistently across nodes.
// Tombstones are purged automatically after TombstoneGracePeriod.
// idemEntry caches result for idempotent retries (Netflix-style safe retry)
type idemEntry struct {
	result    int
	expiresAt time.Time
}

type Storage struct {
	mu                   sync.RWMutex
	data                 map[string]entry
	lists                map[string]*listEntry
	sets                 map[string]*setEntry
	hashes               map[string]*hashEntry
	zsets                map[string]*zsetEntry
	tombs                map[string]time.Time // key -> when tombstoned (for GC)
	idempotency          map[string]idemEntry // "(key,token)" -> result (Netflix: safe retry/hedge)
	TombstoneGracePeriod time.Duration       // tombstones older than this are purged
	IdempotencyTTL       time.Duration       // how long to cache idempotency results (default 5m)
}

type entry struct {
	value  []byte
	expire *time.Time
}

// listEntry uses head+tail for O(1) LPush/LPop/RPush/RPop.
// Logical list order: head[len-1]..head[0], tail[0]..tail[len-1].
// Head is stored reversed so LPUSH = append to head, LPOP = pop from head[len-1].
type listEntry struct {
	head   [][]byte // logical head elements, stored reversed (first element = head[len-1])
	tail   [][]byte // logical tail elements in order
	expire *time.Time
}

// New creates a storage engine
func New() *Storage {
	return &Storage{
		data:                 make(map[string]entry),
		lists:                make(map[string]*listEntry),
		sets:                 make(map[string]*setEntry),
		hashes:               make(map[string]*hashEntry),
		zsets:                make(map[string]*zsetEntry),
		tombs:                make(map[string]time.Time),
		idempotency:          make(map[string]idemEntry),
		TombstoneGracePeriod: 60 * time.Second, // default grace period
		IdempotencyTTL:       5 * time.Minute,  // Netflix: idempotency for safe retry/hedge
	}
}

func (s *Storage) key(k []byte) string { return string(k) }

// Get returns value or nil (tombstoned keys return nil)
func (s *Storage) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(key)
	if _, ok := s.tombs[k]; ok {
		return nil, nil
	}
	e, ok := s.data[k]
	if !ok {
		return nil, nil
	}
	if e.expire != nil && time.Now().After(*e.expire) {
		delete(s.data, k)
		return nil, nil
	}
	return e.value, nil
}

// Set stores key-value with optional TTL (clears tombstone if key was deleted)
func (s *Storage) Set(key, value []byte, ttlSeconds *int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(key)
	delete(s.tombs, k)
	e := entry{value: value}
	if ttlSeconds != nil && *ttlSeconds > 0 {
		t := time.Now().Add(time.Duration(*ttlSeconds) * time.Second)
		e.expire = &t
	}
	s.data[k] = e
	return nil
}

// SetTombstone marks a key as deleted (tombstone). Replicate to all replicas for consistent deletion.
// Returns true if the key existed (any type). Tombstoned keys are treated as non-existent by Get/Exists/etc.
func (s *Storage) SetTombstone(key []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(key)
	had := false
	if _, ok := s.data[k]; ok {
		delete(s.data, k)
		had = true
	}
	if _, ok := s.lists[k]; ok {
		delete(s.lists, k)
		had = true
	}
	if _, ok := s.sets[k]; ok {
		delete(s.sets, k)
		had = true
	}
	if _, ok := s.hashes[k]; ok {
		delete(s.hashes, k)
		had = true
	}
	if _, ok := s.zsets[k]; ok {
		delete(s.zsets, k)
		had = true
	}
	s.tombs[k] = time.Now()
	return had, nil
}

// RunTombstoneGC removes tombstones older than TombstoneGracePeriod. Call periodically.
func (s *Storage) RunTombstoneGC() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.TombstoneGracePeriod <= 0 {
		return 0
	}
	cutoff := time.Now().Add(-s.TombstoneGracePeriod)
	n := 0
	for k, ts := range s.tombs {
		if ts.Before(cutoff) {
			delete(s.tombs, k)
			n++
		}
	}
	return n
}

// Delete removes key immediately (no tombstone). Use for internal operations (e.g. rebalance).
// For user DEL, use SetTombstone and replicate to all replicas.
func (s *Storage) Delete(key []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(key)
	delete(s.tombs, k)
	had := false
	if _, ok := s.data[k]; ok {
		delete(s.data, k)
		had = true
	}
	if _, ok := s.lists[k]; ok {
		delete(s.lists, k)
		had = true
	}
	if _, ok := s.sets[k]; ok {
		delete(s.sets, k)
		had = true
	}
	if _, ok := s.hashes[k]; ok {
		delete(s.hashes, k)
		had = true
	}
	if _, ok := s.zsets[k]; ok {
		delete(s.zsets, k)
		had = true
	}
	return had, nil
}

// Exists returns 1 if key exists, 0 otherwise (tombstoned keys return 0)
func (s *Storage) Exists(key []byte) (int, error) {
	s.mu.Lock()
	s.maybeExpire(key)
	s.maybeExpireList(key)
	s.maybeExpireSet(key)
	s.maybeExpireHash(key)
	s.maybeExpireZSet(key)
	k := s.key(key)
	if _, ok := s.tombs[k]; ok {
		s.mu.Unlock()
		return 0, nil
	}
	_, inData := s.data[k]
	_, inLists := s.lists[k]
	_, inSets := s.sets[k]
	_, inHashes := s.hashes[k]
	_, inZsets := s.zsets[k]
	s.mu.Unlock()
	if inData || inLists || inSets || inHashes || inZsets {
		return 1, nil
	}
	return 0, nil
}

func (s *Storage) maybeExpire(key []byte) {
	k := s.key(key)
	e, ok := s.data[k]
	if !ok || e.expire == nil {
		return
	}
	if time.Now().After(*e.expire) {
		delete(s.data, k)
	}
}

// keysMatch returns true if key matches pattern. Supports * and ? like Redis KEYS.
func keysMatch(pattern, key string) bool {
	if pattern == "*" || pattern == key {
		return true
	}
	// Convert Redis glob to filepath: * and ? are the same
	matched, _ := filepath.Match(pattern, key)
	return matched
}

// Keys returns keys matching pattern (* = all, key:* = prefix, etc). Excludes tombstoned keys.
// Two-pass: pass 1 collects matches and total byte size; pass 2 builds single pre-sized buffer
// so all returned []byte slices point into one allocation (reduces allocations).
func (s *Storage) Keys(pattern []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	pat := string(pattern)
	allMatch := pat == "*"

	// Pass 1: collect matching keys (string refs, no copy) and total byte size
	var keys []string
	var totalLen int
	collect := func(k string) {
		if allMatch || keysMatch(pat, k) {
			keys = append(keys, k)
			totalLen += len(k)
		}
	}
	for k, e := range s.data {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if e.expire != nil && now.After(*e.expire) {
			delete(s.data, k)
			continue
		}
		collect(k)
	}
	for k, le := range s.lists {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if le.expire != nil && now.After(*le.expire) {
			delete(s.lists, k)
			continue
		}
		collect(k)
	}
	for k, se := range s.sets {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if se.expire != nil && now.After(*se.expire) {
			delete(s.sets, k)
			continue
		}
		collect(k)
	}
	for k, he := range s.hashes {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if he.expire != nil && now.After(*he.expire) {
			delete(s.hashes, k)
			continue
		}
		collect(k)
	}
	for k, ze := range s.zsets {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if ze.expire != nil && now.After(*ze.expire) {
			delete(s.zsets, k)
			continue
		}
		collect(k)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	// Pass 2: single pre-sized buffer, out slices point into it
	buf := make([]byte, totalLen)
	out := make([][]byte, 0, len(keys))
	off := 0
	for _, k := range keys {
		copy(buf[off:], k)
		out = append(out, buf[off:off+len(k)])
		off += len(k)
	}
	return out, nil
}

// Incr increments integer value (clears tombstone)
func (s *Storage) Incr(key []byte, delta int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	k := s.key(key)
	delete(s.tombs, k)
	e, ok := s.data[k]
	val := 0
	if ok {
		var err error
		val, err = strconv.Atoi(string(e.value))
		if err != nil {
			return 0, errors.New("value is not an integer or out of range")
		}
	}
	val += delta
	s.data[k] = entry{value: []byte(strconv.Itoa(val)), expire: e.expire}
	return val, nil
}

// TTL returns -2 if not exists, -1 if no expire, else seconds (tombstoned = -2)
func (s *Storage) TTL(key []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	s.maybeExpireList(key)
	s.maybeExpireSet(key)
	s.maybeExpireHash(key)
	s.maybeExpireZSet(key)
	k := s.key(key)
	if _, ok := s.tombs[k]; ok {
		return -2, nil
	}
	var exp *time.Time
	if e, ok := s.data[k]; ok {
		exp = e.expire
	} else if le, ok := s.lists[k]; ok {
		exp = le.expire
	} else if se, ok := s.sets[k]; ok {
		exp = se.expire
	} else if he, ok := s.hashes[k]; ok {
		exp = he.expire
	} else if ze, ok := s.zsets[k]; ok {
		exp = ze.expire
	} else {
		return -2, nil
	}
	if exp == nil {
		return -1, nil
	}
	secs := int(time.Until(*exp).Seconds())
	if secs < 0 {
		return -2, nil
	}
	return secs, nil
}

// Expire sets TTL for any key type
func (s *Storage) Expire(key []byte, seconds int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	s.maybeExpireList(key)
	s.maybeExpireSet(key)
	s.maybeExpireHash(key)
	s.maybeExpireZSet(key)
	k := s.key(key)
	t := time.Now().Add(time.Duration(seconds) * time.Second)
	if e, ok := s.data[k]; ok {
		s.data[k] = entry{value: e.value, expire: &t}
		return true, nil
	}
	if le, ok := s.lists[k]; ok {
		le.expire = &t
		return true, nil
	}
	if se, ok := s.sets[k]; ok {
		se.expire = &t
		return true, nil
	}
	if he, ok := s.hashes[k]; ok {
		he.expire = &t
		return true, nil
	}
	if ze, ok := s.zsets[k]; ok {
		ze.expire = &t
		return true, nil
	}
	return false, nil
}

// Strlen returns string length
func (s *Storage) Strlen(key []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	e, ok := s.data[s.key(key)]
	if !ok {
		return 0, nil
	}
	return len(e.value), nil
}

// Append appends to string, returns new length
func (s *Storage) Append(key, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	k := s.key(key)
	delete(s.tombs, k)
	e, ok := s.data[k]
	if !ok {
		s.data[k] = entry{value: append([]byte(nil), value...), expire: nil}
		return len(value), nil
	}
	e.value = append(e.value, value...)
	s.data[k] = e
	return len(e.value), nil
}

// GetRange returns substring [start:end] (inclusive)
func (s *Storage) GetRange(key []byte, start, end int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	e, ok := s.data[s.key(key)]
	if !ok {
		return []byte{}, nil
	}
	n := len(e.value)
	if n == 0 {
		return []byte{}, nil
	}
	if start < 0 {
		start = n + start
	}
	if end < 0 {
		end = n + end
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end {
		return []byte{}, nil
	}
	return append([]byte(nil), e.value[start:end+1]...), nil
}

// SetRange overwrites at offset, returns new length (clears tombstone)
func (s *Storage) SetRange(key []byte, offset int, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	k := s.key(key)
	delete(s.tombs, k)
	e, ok := s.data[k]
	if !ok {
		e = entry{value: make([]byte, offset+len(value)), expire: nil}
		copy(e.value[offset:], value)
		s.data[k] = e
		return len(e.value), nil
	}
	if offset+len(value) > len(e.value) {
		newLen := offset + len(value)
		newVal := make([]byte, newLen)
		copy(newVal, e.value)
		copy(newVal[offset:], value)
		e.value = newVal
	} else {
		copy(e.value[offset:], value)
	}
	s.data[k] = e
	return len(e.value), nil
}

// IncrBy increments by delta
func (s *Storage) IncrBy(key []byte, delta int) (int, error) {
	return s.Incr(key, delta)
}

// idemCacheKey builds cache key for (key, token) - null byte separates to avoid collisions.
func idemCacheKey(key, token []byte) string {
	return string(key) + "\x00" + string(token)
}

// IdempotencyGet returns cached result if (key, token) was recently applied (within IdempotencyTTL).
// Netflix-style: duplicate INCRBY with same token returns cached result, no double-count on retry.
func (s *Storage) IdempotencyGet(key, token []byte) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ck := idemCacheKey(key, token)
	e, ok := s.idempotency[ck]
	if !ok || time.Now().After(e.expiresAt) {
		if ok {
			delete(s.idempotency, ck)
		}
		return 0, false
	}
	return e.result, true
}

// IdempotencyPut stores (key, token) -> result for idempotent retries.
func (s *Storage) IdempotencyPut(key, token []byte, result int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ttl := s.IdempotencyTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	s.idempotency[idemCacheKey(key, token)] = idemEntry{result: result, expiresAt: time.Now().Add(ttl)}
}

// IncrByWithIdempotency applies delta with optional idempotency. If token given and cached, returns cached result (no over-count on retry).
func (s *Storage) IncrByWithIdempotency(key, token []byte, delta int) (int, error) {
	if len(token) > 0 {
		if r, ok := s.IdempotencyGet(key, token); ok {
			return r, nil
		}
	}
	v, err := s.IncrBy(key, delta)
	if err != nil {
		return 0, err
	}
	if len(token) > 0 {
		s.IdempotencyPut(key, token, v)
	}
	return v, nil
}

// SetNX sets only if not exists, returns 1 if set else 0 (clears tombstone when setting)
func (s *Storage) SetNX(key, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(key)
	if _, ok := s.data[k]; ok {
		return 0, nil
	}
	delete(s.tombs, k)
	s.data[k] = entry{value: append([]byte(nil), value...), expire: nil}
	return 1, nil
}

// GetSet sets and returns old value (clears tombstone)
func (s *Storage) GetSet(key, value []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	k := s.key(key)
	delete(s.tombs, k)
	old, _ := s.data[k]
	s.data[k] = entry{value: append([]byte(nil), value...), expire: old.expire}
	return old.value, nil
}

// Persist removes TTL for any key type
func (s *Storage) Persist(key []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpire(key)
	s.maybeExpireList(key)
	s.maybeExpireSet(key)
	s.maybeExpireHash(key)
	s.maybeExpireZSet(key)
	k := s.key(key)
	if e, ok := s.data[k]; ok {
		s.data[k] = entry{value: e.value, expire: nil}
		return true, nil
	}
	if le, ok := s.lists[k]; ok {
		le.expire = nil
		return true, nil
	}
	if se, ok := s.sets[k]; ok {
		se.expire = nil
		return true, nil
	}
	if he, ok := s.hashes[k]; ok {
		he.expire = nil
		return true, nil
	}
	if ze, ok := s.zsets[k]; ok {
		ze.expire = nil
		return true, nil
	}
	return false, nil
}

func (le *listEntry) listLen() int { return len(le.head) + len(le.tail) }

func (le *listEntry) listAt(i int) []byte {
	n := le.listLen()
	if i < 0 {
		i = n + i
	}
	if i < 0 || i >= n {
		return nil
	}
	hlen := len(le.head)
	if i < hlen {
		return le.head[hlen-1-i]
	}
	return le.tail[i-hlen]
}

// LPUSH inserts values at head of list, returns new length (clears tombstone) — O(1) per value
func (s *Storage) LPush(key []byte, values ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	k := s.key(key)
	delete(s.tombs, k)
	le := s.lists[k]
	if le == nil {
		le = &listEntry{}
		s.lists[k] = le
	}
	for i := len(values) - 1; i >= 0; i-- {
		le.head = append(le.head, values[i])
	}
	return le.listLen(), nil
}

// RPUSH appends values to list, returns new length (clears tombstone) — O(1) per value
func (s *Storage) RPush(key []byte, values ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	k := s.key(key)
	delete(s.tombs, k)
	le := s.lists[k]
	if le == nil {
		le = &listEntry{}
		s.lists[k] = le
	}
	le.tail = append(le.tail, values...)
	return le.listLen(), nil
}

// LLEN returns list length
func (s *Storage) LLen(key []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	le := s.lists[s.key(key)]
	if le == nil {
		return 0, nil
	}
	return le.listLen(), nil
}

// LRANGE returns elements from start to stop (inclusive, Redis semantics)
func (s *Storage) LRange(key []byte, start, stop int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	le := s.lists[s.key(key)]
	if le == nil {
		return nil, nil
	}
	n := le.listLen()
	if n == 0 {
		return [][]byte{}, nil
	}
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, le.listAt(i))
	}
	return out, nil
}

// LPOP removes and returns first element
func (s *Storage) LPop(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	k := s.key(key)
	le := s.lists[k]
	if le == nil || le.listLen() == 0 {
		return nil, nil
	}
	var val []byte
	if len(le.head) > 0 {
		n := len(le.head)
		val = le.head[n-1]
		le.head = le.head[:n-1]
	} else {
		val = le.tail[0]
		le.tail = le.tail[1:]
	}
	if le.listLen() == 0 {
		delete(s.lists, k)
	}
	return val, nil
}

// RPOP removes and returns last element
func (s *Storage) RPop(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	k := s.key(key)
	le := s.lists[k]
	if le == nil || le.listLen() == 0 {
		return nil, nil
	}
	var val []byte
	if len(le.tail) > 0 {
		n := len(le.tail)
		val = le.tail[n-1]
		le.tail = le.tail[:n-1]
	} else {
		val = le.head[0]
		le.head = le.head[1:]
	}
	if le.listLen() == 0 {
		delete(s.lists, k)
	}
	return val, nil
}

// LIndex returns element at index
func (s *Storage) LIndex(key []byte, index int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	le := s.lists[s.key(key)]
	if le == nil {
		return nil, nil
	}
	v := le.listAt(index)
	if v == nil {
		return nil, nil
	}
	return v, nil
}

// LSet sets element at index
func (s *Storage) LSet(key []byte, index int, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	le := s.lists[s.key(key)]
	if le == nil {
		return errors.New("no such key")
	}
	n := le.listLen()
	if index < 0 {
		index = n + index
	}
	if index < 0 || index >= n {
		return errors.New("index out of range")
	}
	hlen := len(le.head)
	if index < hlen {
		le.head[hlen-1-index] = value
	} else {
		le.tail[index-hlen] = value
	}
	return nil
}

// listItems returns all items in logical order (for LRem, LTrim, dump)
func (le *listEntry) listItems() [][]byte {
	n := le.listLen()
	if n == 0 {
		return nil
	}
	out := make([][]byte, 0, n)
	for i := len(le.head) - 1; i >= 0; i-- {
		out = append(out, le.head[i])
	}
	out = append(out, le.tail...)
	return out
}

// setFromItems replaces head/tail with logical items (items[0]=first)
func (le *listEntry) setFromItems(items [][]byte) {
	le.head = nil
	le.tail = nil
	for i := len(items) - 1; i >= 0; i-- {
		le.head = append(le.head, items[i])
	}
}

// LRem removes occurrences of value. Redis semantics:
// count>0: remove first count from head; count<0: remove last (-count) from tail; count=0: remove all.
func (s *Storage) LRem(key []byte, count int, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	k := s.key(key)
	le := s.lists[k]
	if le == nil {
		return 0, nil
	}
	items := le.listItems()
	valStr := string(value)
	removed := 0
	var newItems [][]byte
	if count == 0 {
		for _, it := range items {
			if string(it) != valStr {
				newItems = append(newItems, it)
			} else {
				removed++
			}
		}
	} else if count > 0 {
		for _, it := range items {
			if string(it) == valStr && removed < count {
				removed++
			} else {
				newItems = append(newItems, it)
			}
		}
	} else {
		// count < 0: iterate backward, remove last (-count) occurrences in place
		for i := len(items) - 1; i >= 0 && removed < -count; i-- {
			if string(items[i]) == valStr {
				removed++
				items = append(items[:i], items[i+1:]...)
			}
		}
		newItems = items
	}
	le.setFromItems(newItems)
	if le.listLen() == 0 {
		delete(s.lists, k)
	}
	return removed, nil
}

// LTrim keeps only [start:stop]
func (s *Storage) LTrim(key []byte, start, stop int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireList(key)
	k := s.key(key)
	le := s.lists[k]
	if le == nil {
		return nil
	}
	n := le.listLen()
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		le.head = nil
		le.tail = nil
		delete(s.lists, k)
		return nil
	}
	items := le.listItems()
	le.setFromItems(items[start : stop+1])
	return nil
}

func (s *Storage) maybeExpireList(key []byte) {
	k := s.key(key)
	le := s.lists[k]
	if le == nil || le.expire == nil {
		return
	}
	if time.Now().After(*le.expire) {
		delete(s.lists, k)
	}
}

// Type returns "string", "list", "set", "zset", "hash", or "none"
func (s *Storage) Type(key []byte) string {
	s.mu.Lock()
	s.maybeExpire(key)
	s.maybeExpireList(key)
	s.maybeExpireSet(key)
	s.maybeExpireHash(key)
	s.maybeExpireZSet(key)
	k := s.key(key)
	if _, ok := s.tombs[k]; ok {
		s.mu.Unlock()
		return "none"
	}
	if _, ok := s.data[k]; ok {
		s.mu.Unlock()
		return "string"
	}
	if _, ok := s.lists[k]; ok {
		s.mu.Unlock()
		return "list"
	}
	if _, ok := s.sets[k]; ok {
		s.mu.Unlock()
		return "set"
	}
	if _, ok := s.hashes[k]; ok {
		s.mu.Unlock()
		return "hash"
	}
	if _, ok := s.zsets[k]; ok {
		s.mu.Unlock()
		return "zset"
	}
	s.mu.Unlock()
	return "none"
}

// DBSize returns total key count (excludes tombstoned keys)
func (s *Storage) DBSize() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	n := 0
	for k, e := range s.data {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if e.expire != nil && now.After(*e.expire) {
			delete(s.data, k)
			continue
		}
		n++
	}
	for k, le := range s.lists {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if le.expire != nil && now.After(*le.expire) {
			delete(s.lists, k)
			continue
		}
		n++
	}
	for k, se := range s.sets {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if se.expire != nil && now.After(*se.expire) {
			delete(s.sets, k)
			continue
		}
		n++
	}
	for k, he := range s.hashes {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if he.expire != nil && now.After(*he.expire) {
			delete(s.hashes, k)
			continue
		}
		n++
	}
	for k, ze := range s.zsets {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if ze.expire != nil && now.After(*ze.expire) {
			delete(s.zsets, k)
			continue
		}
		n++
	}
	return n, nil
}

// FlushDB removes all keys
func (s *Storage) FlushDB() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]entry)
	s.lists = make(map[string]*listEntry)
	s.sets = make(map[string]*setEntry)
	s.hashes = make(map[string]*hashEntry)
	s.zsets = make(map[string]*zsetEntry)
	s.tombs = make(map[string]time.Time)
}

// RandomKey returns a random key or nil (excludes tombstoned keys)
func (s *Storage) RandomKey() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var keys []string
	for k, e := range s.data {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if e.expire != nil && now.After(*e.expire) {
			delete(s.data, k)
			continue
		}
		keys = append(keys, k)
	}
	for k, le := range s.lists {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if le.expire != nil && now.After(*le.expire) {
			delete(s.lists, k)
			continue
		}
		keys = append(keys, k)
	}
	for k, se := range s.sets {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if se.expire != nil && now.After(*se.expire) {
			delete(s.sets, k)
			continue
		}
		keys = append(keys, k)
	}
	for k, he := range s.hashes {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if he.expire != nil && now.After(*he.expire) {
			delete(s.hashes, k)
			continue
		}
		keys = append(keys, k)
	}
	for k, ze := range s.zsets {
		if _, ok := s.tombs[k]; ok {
			continue
		}
		if ze.expire != nil && now.After(*ze.expire) {
			delete(s.zsets, k)
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return []byte(keys[rand.Intn(len(keys))]), nil
}
