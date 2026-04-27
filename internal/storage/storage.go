package storage

import (
	"errors"
	"hash/maphash"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Storage is in-memory key-value store with TTL.
//
// Internally sharded: keys hash into one of `numShards` shards, each with its
// own RWMutex. Replaces the old single global RWMutex which collapsed under
// many-core contention (atomic ping-pong on the reader counter). Single-key
// ops touch exactly one shard; scans (Keys, DBSize, etc.) and sweepers iterate
// all shards in turn — those accept eventual consistency, same as before.
//
// Shard count = 128. At this size parallel write throughput at 14 cores
// matches the asymptote (going to 256 costs ~20% on KEYS scans for no
// measurable parallel-throughput gain), and 64 starts to lose on the most
// contention-sensitive ops (Incr) when the keyspace is small. Must be a power
// of 2 — shardFor uses `& (numShards-1)`.
//
// Deletions use tombstone marks: DEL writes a tombstone so it replicates
// consistently across nodes. Tombstones are purged after TombstoneGracePeriod.
const numShards = 128

// idemEntry caches result for idempotent retries (Netflix-style safe retry).
type idemEntry struct {
	result    int
	expiresAt time.Time
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

// shard owns all five type maps for the keys whose hash routes here, plus
// tombstones and the idempotency cache for those keys. The 56-byte tail pad
// keeps adjacent shards off the same 64B cache line so writes on one shard
// don't invalidate the next shard's lock cache line.
type shard struct {
	mu          sync.RWMutex
	data        map[string]entry
	lists       map[string]*listEntry
	sets        map[string]*setEntry
	hashes      map[string]*hashEntry
	zsets       map[string]*zsetEntry
	tombs       map[string]time.Time // key -> when tombstoned (for GC)
	idempotency map[string]idemEntry // "(key,token)" -> result (Netflix: safe retry/hedge)
	_           [56]byte
}

func newShard() *shard {
	return &shard{
		data:        make(map[string]entry),
		lists:       make(map[string]*listEntry),
		sets:        make(map[string]*setEntry),
		hashes:      make(map[string]*hashEntry),
		zsets:       make(map[string]*zsetEntry),
		tombs:       make(map[string]time.Time),
		idempotency: make(map[string]idemEntry),
	}
}

type Storage struct {
	shards               [numShards]*shard
	seed                 maphash.Seed
	TombstoneGracePeriod time.Duration // tombstones older than this are purged
	IdempotencyTTL       time.Duration // how long to cache idempotency results (default 5m)
}

// New creates a storage engine
func New() *Storage {
	s := &Storage{
		seed:                 maphash.MakeSeed(),
		TombstoneGracePeriod: 60 * time.Second, // default grace period
		IdempotencyTTL:       5 * time.Minute,  // Netflix: idempotency for safe retry/hedge
	}
	for i := range s.shards {
		s.shards[i] = newShard()
	}
	return s
}

func (s *Storage) shardFor(key []byte) *shard {
	h := maphash.Bytes(s.seed, key)
	return s.shards[h&(numShards-1)]
}

// expired reports whether the timestamp pointer indicates a past expiry.
// Used by read paths so they don't have to delete (and therefore don't have
// to take the write lock); the background sweeper handles eviction.
func expired(t *time.Time) bool {
	return t != nil && time.Now().After(*t)
}

// Get returns value or nil (tombstoned keys return nil). Read-only path:
// does not evict expired entries — that's the sweeper's job (see
// RunExpiredKeysGC), so concurrent reads can run in parallel under RLock.
func (s *Storage) Get(key []byte) ([]byte, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if _, ok := sh.tombs[string(key)]; ok {
		return nil, nil
	}
	e, ok := sh.data[string(key)]
	if !ok || expired(e.expire) {
		return nil, nil
	}
	return e.value, nil
}

// Set stores key-value with optional TTL (clears tombstone if key was deleted)
func (s *Storage) Set(key, value []byte, ttlSeconds *int) error {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	delete(sh.tombs, k)
	e := entry{value: value}
	if ttlSeconds != nil && *ttlSeconds > 0 {
		t := time.Now().Add(time.Duration(*ttlSeconds) * time.Second)
		e.expire = &t
	}
	sh.data[k] = e
	return nil
}

// SetTombstone marks a key as deleted (tombstone). Replicate to all replicas for consistent deletion.
// Returns true if the key existed (any type). Tombstoned keys are treated as non-existent by Get/Exists/etc.
func (s *Storage) SetTombstone(key []byte) (bool, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	had := false
	if _, ok := sh.data[k]; ok {
		delete(sh.data, k)
		had = true
	}
	if _, ok := sh.lists[k]; ok {
		delete(sh.lists, k)
		had = true
	}
	if _, ok := sh.sets[k]; ok {
		delete(sh.sets, k)
		had = true
	}
	if _, ok := sh.hashes[k]; ok {
		delete(sh.hashes, k)
		had = true
	}
	if _, ok := sh.zsets[k]; ok {
		delete(sh.zsets, k)
		had = true
	}
	sh.tombs[k] = time.Now()
	return had, nil
}

// RunTombstoneGC removes tombstones older than TombstoneGracePeriod across
// all shards. Call periodically.
func (s *Storage) RunTombstoneGC() int {
	if s.TombstoneGracePeriod <= 0 {
		return 0
	}
	cutoff := time.Now().Add(-s.TombstoneGracePeriod)
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		for k, ts := range sh.tombs {
			if ts.Before(cutoff) {
				delete(sh.tombs, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

// RunExpiredKeysGC sweeps all expired entries across every type and shard.
// Read paths no longer evict expired entries (so they can run under RLock);
// this background sweep takes their place. Returns the number of entries
// removed.
func (s *Storage) RunExpiredKeysGC() int {
	now := time.Now()
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		for k, e := range sh.data {
			if e.expire != nil && now.After(*e.expire) {
				delete(sh.data, k)
				n++
			}
		}
		for k, le := range sh.lists {
			if le.expire != nil && now.After(*le.expire) {
				delete(sh.lists, k)
				n++
			}
		}
		for k, se := range sh.sets {
			if se.expire != nil && now.After(*se.expire) {
				delete(sh.sets, k)
				n++
			}
		}
		for k, he := range sh.hashes {
			if he.expire != nil && now.After(*he.expire) {
				delete(sh.hashes, k)
				n++
			}
		}
		for k, ze := range sh.zsets {
			if ze.expire != nil && now.After(*ze.expire) {
				delete(sh.zsets, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

// RunIdempotencyGC purges expired idempotency cache entries across all shards.
// Without this the map grows for every (key, token) pair never read again —
// Get clears expired entries lazily, but tokens used only once would leak.
func (s *Storage) RunIdempotencyGC() int {
	now := time.Now()
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		for k, e := range sh.idempotency {
			if now.After(e.expiresAt) {
				delete(sh.idempotency, k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

// Delete removes key immediately (no tombstone). Use for internal operations (e.g. rebalance).
// For user DEL, use SetTombstone and replicate to all replicas.
func (s *Storage) Delete(key []byte) (bool, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	delete(sh.tombs, k)
	had := false
	if _, ok := sh.data[k]; ok {
		delete(sh.data, k)
		had = true
	}
	if _, ok := sh.lists[k]; ok {
		delete(sh.lists, k)
		had = true
	}
	if _, ok := sh.sets[k]; ok {
		delete(sh.sets, k)
		had = true
	}
	if _, ok := sh.hashes[k]; ok {
		delete(sh.hashes, k)
		had = true
	}
	if _, ok := sh.zsets[k]; ok {
		delete(sh.zsets, k)
		had = true
	}
	return had, nil
}

// Exists returns 1 if key exists, 0 otherwise (tombstoned and expired keys return 0).
func (s *Storage) Exists(key []byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	k := string(key)
	if _, ok := sh.tombs[k]; ok {
		return 0, nil
	}
	if e, ok := sh.data[k]; ok && !expired(e.expire) {
		return 1, nil
	}
	if le, ok := sh.lists[k]; ok && !expired(le.expire) {
		return 1, nil
	}
	if se, ok := sh.sets[k]; ok && !expired(se.expire) {
		return 1, nil
	}
	if he, ok := sh.hashes[k]; ok && !expired(he.expire) {
		return 1, nil
	}
	if ze, ok := sh.zsets[k]; ok && !expired(ze.expire) {
		return 1, nil
	}
	return 0, nil
}

func (sh *shard) maybeExpire(k string) {
	e, ok := sh.data[k]
	if !ok || e.expire == nil {
		return
	}
	if time.Now().After(*e.expire) {
		delete(sh.data, k)
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

// keyMatcher returns a per-call matcher specialized for the pattern shape. The
// hot paths "*" and "literal-prefix*" avoid filepath.Match entirely (which
// re-parses the pattern on every call).
func keyMatcher(pat string) func(string) bool {
	if pat == "*" {
		return func(string) bool { return true }
	}
	if n := len(pat); n > 0 && pat[n-1] == '*' {
		body := pat[:n-1]
		if !strings.ContainsAny(body, "*?[\\") {
			return func(k string) bool { return strings.HasPrefix(k, body) }
		}
	}
	return func(k string) bool { return keysMatch(pat, k) }
}

// Keys returns keys matching pattern (* = all, key:* = prefix, etc). Excludes
// tombstoned and expired keys. Read-only — does not evict expired entries.
//
// Two-pass: pass 1 collects matches and total byte size; pass 2 builds single
// pre-sized buffer so all returned []byte slices point into one allocation.
func (s *Storage) Keys(pattern []byte) ([][]byte, error) {
	now := time.Now()
	isExpired := func(t *time.Time) bool { return t != nil && now.After(*t) }
	match := keyMatcher(string(pattern))

	var keys []string
	var totalLen int
	for _, sh := range s.shards {
		sh.mu.RLock()
		// Whole-shard fast path: if no entries at all, skip the inner ranges
		// (each `range` over a make()'d empty map still pays mapiterinit).
		if len(sh.data)+len(sh.lists)+len(sh.sets)+len(sh.hashes)+len(sh.zsets) == 0 {
			sh.mu.RUnlock()
			continue
		}
		hasTombs := len(sh.tombs) > 0
		tombed := func(k string) bool {
			if !hasTombs {
				return false
			}
			_, ok := sh.tombs[k]
			return ok
		}
		if len(sh.data) > 0 {
			for k, e := range sh.data {
				if tombed(k) || isExpired(e.expire) {
					continue
				}
				if match(k) {
					keys = append(keys, k)
					totalLen += len(k)
				}
			}
		}
		if len(sh.lists) > 0 {
			for k, le := range sh.lists {
				if tombed(k) || isExpired(le.expire) {
					continue
				}
				if match(k) {
					keys = append(keys, k)
					totalLen += len(k)
				}
			}
		}
		if len(sh.sets) > 0 {
			for k, se := range sh.sets {
				if tombed(k) || isExpired(se.expire) {
					continue
				}
				if match(k) {
					keys = append(keys, k)
					totalLen += len(k)
				}
			}
		}
		if len(sh.hashes) > 0 {
			for k, he := range sh.hashes {
				if tombed(k) || isExpired(he.expire) {
					continue
				}
				if match(k) {
					keys = append(keys, k)
					totalLen += len(k)
				}
			}
		}
		if len(sh.zsets) > 0 {
			for k, ze := range sh.zsets {
				if tombed(k) || isExpired(ze.expire) {
					continue
				}
				if match(k) {
					keys = append(keys, k)
					totalLen += len(k)
				}
			}
		}
		sh.mu.RUnlock()
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
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpire(k)
	delete(sh.tombs, k)
	e, ok := sh.data[k]
	val := 0
	if ok {
		var err error
		val, err = strconv.Atoi(string(e.value))
		if err != nil {
			return 0, errors.New("value is not an integer or out of range")
		}
	}
	val += delta
	sh.data[k] = entry{value: []byte(strconv.Itoa(val)), expire: e.expire}
	return val, nil
}

// TTL returns -2 if not exists, -1 if no expire, else seconds (tombstoned = -2)
func (s *Storage) TTL(key []byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	k := string(key)
	if _, ok := sh.tombs[k]; ok {
		return -2, nil
	}
	var exp *time.Time
	if e, ok := sh.data[k]; ok {
		exp = e.expire
	} else if le, ok := sh.lists[k]; ok {
		exp = le.expire
	} else if se, ok := sh.sets[k]; ok {
		exp = se.expire
	} else if he, ok := sh.hashes[k]; ok {
		exp = he.expire
	} else if ze, ok := sh.zsets[k]; ok {
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
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpire(k)
	sh.maybeExpireList(k)
	sh.maybeExpireSet(k)
	sh.maybeExpireHash(k)
	sh.maybeExpireZSet(k)
	t := time.Now().Add(time.Duration(seconds) * time.Second)
	if e, ok := sh.data[k]; ok {
		sh.data[k] = entry{value: e.value, expire: &t}
		return true, nil
	}
	if le, ok := sh.lists[k]; ok {
		le.expire = &t
		return true, nil
	}
	if se, ok := sh.sets[k]; ok {
		se.expire = &t
		return true, nil
	}
	if he, ok := sh.hashes[k]; ok {
		he.expire = &t
		return true, nil
	}
	if ze, ok := sh.zsets[k]; ok {
		ze.expire = &t
		return true, nil
	}
	return false, nil
}

// Strlen returns string length (read-only).
func (s *Storage) Strlen(key []byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := sh.data[string(key)]
	if !ok || expired(e.expire) {
		return 0, nil
	}
	return len(e.value), nil
}

// Append appends to string, returns new length
func (s *Storage) Append(key, value []byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpire(k)
	delete(sh.tombs, k)
	e, ok := sh.data[k]
	if !ok {
		sh.data[k] = entry{value: append([]byte(nil), value...), expire: nil}
		return len(value), nil
	}
	e.value = append(e.value, value...)
	sh.data[k] = e
	return len(e.value), nil
}

// GetRange returns substring [start:end] (inclusive). Read-only path.
func (s *Storage) GetRange(key []byte, start, end int) ([]byte, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := sh.data[string(key)]
	if !ok || expired(e.expire) {
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
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpire(k)
	delete(sh.tombs, k)
	e, ok := sh.data[k]
	if !ok {
		e = entry{value: make([]byte, offset+len(value)), expire: nil}
		copy(e.value[offset:], value)
		sh.data[k] = e
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
	sh.data[k] = e
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
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	ck := idemCacheKey(key, token)
	e, ok := sh.idempotency[ck]
	if !ok || time.Now().After(e.expiresAt) {
		if ok {
			delete(sh.idempotency, ck)
		}
		return 0, false
	}
	return e.result, true
}

// IdempotencyPut stores (key, token) -> result for idempotent retries.
func (s *Storage) IdempotencyPut(key, token []byte, result int) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	ttl := s.IdempotencyTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	sh.idempotency[idemCacheKey(key, token)] = idemEntry{result: result, expiresAt: time.Now().Add(ttl)}
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
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	if _, ok := sh.data[k]; ok {
		return 0, nil
	}
	delete(sh.tombs, k)
	sh.data[k] = entry{value: append([]byte(nil), value...), expire: nil}
	return 1, nil
}

// GetSet sets and returns old value (clears tombstone)
func (s *Storage) GetSet(key, value []byte) ([]byte, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpire(k)
	delete(sh.tombs, k)
	old, _ := sh.data[k]
	sh.data[k] = entry{value: append([]byte(nil), value...), expire: old.expire}
	return old.value, nil
}

// Persist removes TTL for any key type
func (s *Storage) Persist(key []byte) (bool, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpire(k)
	sh.maybeExpireList(k)
	sh.maybeExpireSet(k)
	sh.maybeExpireHash(k)
	sh.maybeExpireZSet(k)
	if e, ok := sh.data[k]; ok {
		sh.data[k] = entry{value: e.value, expire: nil}
		return true, nil
	}
	if le, ok := sh.lists[k]; ok {
		le.expire = nil
		return true, nil
	}
	if se, ok := sh.sets[k]; ok {
		se.expire = nil
		return true, nil
	}
	if he, ok := sh.hashes[k]; ok {
		he.expire = nil
		return true, nil
	}
	if ze, ok := sh.zsets[k]; ok {
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

// LPUSH inserts values at head of list, returns new length (clears tombstone) — O(1) per value.
// Redis semantics: each value pushed to the head in order, so LPUSH a b c yields [c,b,a].
// Internally head is stored reversed (head[len-1] is logical index 0), so each value is just
// appended in the order it arrives.
func (s *Storage) LPush(key []byte, values ...[]byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireList(k)
	delete(sh.tombs, k)
	le := sh.lists[k]
	if le == nil {
		le = &listEntry{}
		sh.lists[k] = le
	}
	for _, v := range values {
		le.head = append(le.head, v)
	}
	return le.listLen(), nil
}

// RPUSH appends values to list, returns new length (clears tombstone) — O(1) per value
func (s *Storage) RPush(key []byte, values ...[]byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireList(k)
	delete(sh.tombs, k)
	le := sh.lists[k]
	if le == nil {
		le = &listEntry{}
		sh.lists[k] = le
	}
	le.tail = append(le.tail, values...)
	return le.listLen(), nil
}

// LLEN returns list length (read-only).
func (s *Storage) LLen(key []byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	le := sh.lists[string(key)]
	if le == nil || expired(le.expire) {
		return 0, nil
	}
	return le.listLen(), nil
}

// LRANGE returns elements from start to stop (inclusive, Redis semantics). Read-only.
func (s *Storage) LRange(key []byte, start, stop int) ([][]byte, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	le := sh.lists[string(key)]
	if le == nil || expired(le.expire) {
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
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireList(k)
	le := sh.lists[k]
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
		delete(sh.lists, k)
	}
	return val, nil
}

// RPOP removes and returns last element
func (s *Storage) RPop(key []byte) ([]byte, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireList(k)
	le := sh.lists[k]
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
		delete(sh.lists, k)
	}
	return val, nil
}

// LIndex returns element at index (read-only).
func (s *Storage) LIndex(key []byte, index int) ([]byte, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	le := sh.lists[string(key)]
	if le == nil || expired(le.expire) {
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
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireList(k)
	le := sh.lists[k]
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
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireList(k)
	le := sh.lists[k]
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
		delete(sh.lists, k)
	}
	return removed, nil
}

// LTrim keeps only [start:stop]
func (s *Storage) LTrim(key []byte, start, stop int) error {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireList(k)
	le := sh.lists[k]
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
		delete(sh.lists, k)
		return nil
	}
	items := le.listItems()
	le.setFromItems(items[start : stop+1])
	return nil
}

func (sh *shard) maybeExpireList(k string) {
	le := sh.lists[k]
	if le == nil || le.expire == nil {
		return
	}
	if time.Now().After(*le.expire) {
		delete(sh.lists, k)
	}
}

// Type returns "string", "list", "set", "zset", "hash", or "none". Read-only.
func (s *Storage) Type(key []byte) string {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	k := string(key)
	if _, ok := sh.tombs[k]; ok {
		return "none"
	}
	if e, ok := sh.data[k]; ok && !expired(e.expire) {
		return "string"
	}
	if le, ok := sh.lists[k]; ok && !expired(le.expire) {
		return "list"
	}
	if se, ok := sh.sets[k]; ok && !expired(se.expire) {
		return "set"
	}
	if he, ok := sh.hashes[k]; ok && !expired(he.expire) {
		return "hash"
	}
	if ze, ok := sh.zsets[k]; ok && !expired(ze.expire) {
		return "zset"
	}
	return "none"
}

// DBSize returns total key count (excludes tombstoned and expired keys). Read-only.
func (s *Storage) DBSize() (int, error) {
	now := time.Now()
	isExpired := func(t *time.Time) bool { return t != nil && now.After(*t) }
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		if len(sh.data)+len(sh.lists)+len(sh.sets)+len(sh.hashes)+len(sh.zsets) == 0 {
			sh.mu.RUnlock()
			continue
		}
		hasTombs := len(sh.tombs) > 0
		tombed := func(k string) bool {
			if !hasTombs {
				return false
			}
			_, ok := sh.tombs[k]
			return ok
		}
		if len(sh.data) > 0 {
			for k, e := range sh.data {
				if !tombed(k) && !isExpired(e.expire) {
					n++
				}
			}
		}
		if len(sh.lists) > 0 {
			for k, le := range sh.lists {
				if !tombed(k) && !isExpired(le.expire) {
					n++
				}
			}
		}
		if len(sh.sets) > 0 {
			for k, se := range sh.sets {
				if !tombed(k) && !isExpired(se.expire) {
					n++
				}
			}
		}
		if len(sh.hashes) > 0 {
			for k, he := range sh.hashes {
				if !tombed(k) && !isExpired(he.expire) {
					n++
				}
			}
		}
		if len(sh.zsets) > 0 {
			for k, ze := range sh.zsets {
				if !tombed(k) && !isExpired(ze.expire) {
					n++
				}
			}
		}
		sh.mu.RUnlock()
	}
	return n, nil
}

// FlushDB removes all keys
func (s *Storage) FlushDB() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.data = make(map[string]entry)
		sh.lists = make(map[string]*listEntry)
		sh.sets = make(map[string]*setEntry)
		sh.hashes = make(map[string]*hashEntry)
		sh.zsets = make(map[string]*zsetEntry)
		sh.tombs = make(map[string]time.Time)
		sh.mu.Unlock()
	}
}

// RandomKey returns a random key or nil (excludes tombstoned and expired keys). Read-only.
func (s *Storage) RandomKey() ([]byte, error) {
	now := time.Now()
	isExpired := func(t *time.Time) bool { return t != nil && now.After(*t) }
	var keys []string
	for _, sh := range s.shards {
		sh.mu.RLock()
		if len(sh.data)+len(sh.lists)+len(sh.sets)+len(sh.hashes)+len(sh.zsets) == 0 {
			sh.mu.RUnlock()
			continue
		}
		hasTombs := len(sh.tombs) > 0
		tombed := func(k string) bool {
			if !hasTombs {
				return false
			}
			_, ok := sh.tombs[k]
			return ok
		}
		if len(sh.data) > 0 {
			for k, e := range sh.data {
				if !tombed(k) && !isExpired(e.expire) {
					keys = append(keys, k)
				}
			}
		}
		if len(sh.lists) > 0 {
			for k, le := range sh.lists {
				if !tombed(k) && !isExpired(le.expire) {
					keys = append(keys, k)
				}
			}
		}
		if len(sh.sets) > 0 {
			for k, se := range sh.sets {
				if !tombed(k) && !isExpired(se.expire) {
					keys = append(keys, k)
				}
			}
		}
		if len(sh.hashes) > 0 {
			for k, he := range sh.hashes {
				if !tombed(k) && !isExpired(he.expire) {
					keys = append(keys, k)
				}
			}
		}
		if len(sh.zsets) > 0 {
			for k, ze := range sh.zsets {
				if !tombed(k) && !isExpired(ze.expire) {
					keys = append(keys, k)
				}
			}
		}
		sh.mu.RUnlock()
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return []byte(keys[rand.Intn(len(keys))]), nil
}
