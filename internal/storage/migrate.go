package storage

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"time"
)

// Key type bytes for migration serialization
const (
	migrateTypeString byte = 1
	migrateTypeList   byte = 2
	migrateTypeSet    byte = 3
	migrateTypeHash   byte = 4
	migrateTypeZSet   byte = 5
)

// DumpKey serializes a key for migration. Returns nil if key does not exist.
// Binary format: [1 byte type][4 byte ttl sec, -1=no TTL][type-specific payload]
// Types: string=raw bytes; list/set=[4 byte count][4 byte len, item bytes]*; hash=[4 byte len, field, 4 byte len, value]*; zset=[8 byte score, 4 byte len, member]*.
func (s *Storage) DumpKey(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(key)

	// Expire any stale keys first
	s.maybeExpire(key)
	s.maybeExpireList(key)
	s.maybeExpireSet(key)
	s.maybeExpireHash(key)
	s.maybeExpireZSet(key)

	var ttlSec int32 = -1
	if e, ok := s.data[k]; ok {
		if e.expire != nil {
			ttlSec = int32(time.Until(*e.expire).Seconds())
			if ttlSec < 0 {
				ttlSec = 0
			}
		}
		buf := make([]byte, 1+4+len(e.value))
		buf[0] = migrateTypeString
		binary.BigEndian.PutUint32(buf[1:5], uint32(ttlSec))
		copy(buf[5:], e.value)
		return buf, nil
	}
	if le, ok := s.lists[k]; ok {
		if le.expire != nil {
			ttlSec = int32(time.Until(*le.expire).Seconds())
			if ttlSec < 0 {
				ttlSec = 0
			}
		}
		return s.dumpList(le, ttlSec)
	}
	if se, ok := s.sets[k]; ok {
		if se.expire != nil {
			ttlSec = int32(time.Until(*se.expire).Seconds())
			if ttlSec < 0 {
				ttlSec = 0
			}
		}
		return s.dumpSet(se, ttlSec)
	}
	if he, ok := s.hashes[k]; ok {
		if he.expire != nil {
			ttlSec = int32(time.Until(*he.expire).Seconds())
			if ttlSec < 0 {
				ttlSec = 0
			}
		}
		return s.dumpHash(he, ttlSec)
	}
	if ze, ok := s.zsets[k]; ok {
		if ze.expire != nil {
			ttlSec = int32(time.Until(*ze.expire).Seconds())
			if ttlSec < 0 {
				ttlSec = 0
			}
		}
		return s.dumpZSet(ze, ttlSec)
	}
	return nil, nil
}

func (s *Storage) dumpList(le *listEntry, ttl int32) ([]byte, error) {
	items := le.listItems()
	size := 1 + 4 + 4
	for _, it := range items {
		size += 4 + len(it)
	}
	buf := make([]byte, size)
	buf[0] = migrateTypeList
	binary.BigEndian.PutUint32(buf[1:5], uint32(ttl))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(items)))
	off := 9
	for _, it := range items {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(it)))
		off += 4
		copy(buf[off:], it)
		off += len(it)
	}
	return buf, nil
}

func (s *Storage) dumpSet(se *setEntry, ttl int32) ([]byte, error) {
	members := make([][]byte, 0, len(se.members))
	for m := range se.members {
		members = append(members, []byte(m))
	}
	size := 1 + 4 + 4
	for _, m := range members {
		size += 4 + len(m)
	}
	buf := make([]byte, size)
	buf[0] = migrateTypeSet
	binary.BigEndian.PutUint32(buf[1:5], uint32(ttl))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(members)))
	off := 9
	for _, m := range members {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(m)))
		off += 4
		copy(buf[off:], m)
		off += len(m)
	}
	return buf, nil
}

func (s *Storage) dumpHash(he *hashEntry, ttl int32) ([]byte, error) {
	size := 1 + 4 + 4
	for f, v := range he.fields {
		size += 4 + len(f) + 4 + len(v)
	}
	buf := make([]byte, size)
	buf[0] = migrateTypeHash
	binary.BigEndian.PutUint32(buf[1:5], uint32(ttl))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(he.fields)))
	off := 9
	for f, v := range he.fields {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(f)))
		off += 4
		copy(buf[off:], f)
		off += len(f)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(v)))
		off += 4
		copy(buf[off:], v)
		off += len(v)
	}
	return buf, nil
}

func (s *Storage) dumpZSet(ze *zsetEntry, ttl int32) ([]byte, error) {
	ze.ensureOrder()
	size := 1 + 4 + 4
	for _, m := range ze.order {
		size += 8 + 4 + len(m)
	}
	buf := make([]byte, size)
	buf[0] = migrateTypeZSet
	binary.BigEndian.PutUint32(buf[1:5], uint32(ttl))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(ze.order)))
	off := 9
	for _, m := range ze.order {
		score := ze.scores[m]
		binary.BigEndian.PutUint64(buf[off:], math.Float64bits(score))
		off += 8
		binary.BigEndian.PutUint32(buf[off:], uint32(len(m)))
		off += 4
		copy(buf[off:], m)
		off += len(m)
	}
	return buf, nil
}

// RestoreKey deserializes and stores a key from migration payload.
func (s *Storage) RestoreKey(key, data []byte) error {
	if len(data) < 5 {
		return errors.New("truncated dump data")
	}
	typ := data[0]
	ttl := int32(binary.BigEndian.Uint32(data[1:5]))
	var ttlPtr *int
	if ttl >= 0 {
		t := int(ttl)
		ttlPtr = &t
	}
	payload := data[5:]

	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(key)

	switch typ {
	case migrateTypeString:
		s.data[k] = entry{value: append([]byte(nil), payload...), expire: s.ttlFromSec(ttlPtr)}
		return nil
	case migrateTypeList:
		return s.restoreList(k, payload, ttlPtr)
	case migrateTypeSet:
		return s.restoreSet(k, payload, ttlPtr)
	case migrateTypeHash:
		return s.restoreHash(k, payload, ttlPtr)
	case migrateTypeZSet:
		return s.restoreZSet(k, payload, ttlPtr)
	default:
		return errors.New("unknown key type in dump")
	}
}

func (s *Storage) ttlFromSec(sec *int) *time.Time {
	if sec == nil || *sec < 0 {
		return nil
	}
	t := time.Now().Add(time.Duration(*sec) * time.Second)
	return &t
}

func (s *Storage) restoreList(k string, payload []byte, ttl *int) error {
	if len(payload) < 4 {
		return errors.New("truncated list payload")
	}
	n := int(binary.BigEndian.Uint32(payload[0:4]))
	off := 4
	items := make([][]byte, 0, n)
	for i := 0; i < n && off+4 <= len(payload); i++ {
		alen := binary.BigEndian.Uint32(payload[off:])
		off += 4
		if off+int(alen) > len(payload) {
			break
		}
		items = append(items, append([]byte(nil), payload[off:off+int(alen)]...))
		off += int(alen)
	}
	le := &listEntry{expire: s.ttlFromSec(ttl)}
	le.setFromItems(items)
	s.lists[k] = le
	return nil
}

func (s *Storage) restoreSet(k string, payload []byte, ttl *int) error {
	if len(payload) < 4 {
		return errors.New("truncated set payload")
	}
	n := int(binary.BigEndian.Uint32(payload[0:4]))
	off := 4
	se := &setEntry{members: make(map[string]bool), expire: s.ttlFromSec(ttl)}
	for i := 0; i < n && off+4 <= len(payload); i++ {
		alen := binary.BigEndian.Uint32(payload[off:])
		off += 4
		if off+int(alen) > len(payload) {
			break
		}
		se.members[string(payload[off:off+int(alen)])] = true
		off += int(alen)
	}
	s.sets[k] = se
	return nil
}

func (s *Storage) restoreHash(k string, payload []byte, ttl *int) error {
	if len(payload) < 4 {
		return errors.New("truncated hash payload")
	}
	n := int(binary.BigEndian.Uint32(payload[0:4]))
	off := 4
	he := &hashEntry{fields: make(map[string][]byte), expire: s.ttlFromSec(ttl)}
	for i := 0; i < n && off+8 <= len(payload); i++ {
		flen := binary.BigEndian.Uint32(payload[off:])
		off += 4
		if off+int(flen)+4 > len(payload) {
			break
		}
		field := string(payload[off : off+int(flen)])
		off += int(flen)
		vlen := binary.BigEndian.Uint32(payload[off:])
		off += 4
		if off+int(vlen) > len(payload) {
			break
		}
		he.fields[field] = append([]byte(nil), payload[off:off+int(vlen)]...)
		off += int(vlen)
	}
	s.hashes[k] = he
	return nil
}

func (s *Storage) restoreZSet(k string, payload []byte, ttl *int) error {
	if len(payload) < 4 {
		return errors.New("truncated zset payload")
	}
	n := int(binary.BigEndian.Uint32(payload[0:4]))
	off := 4
	ze := &zsetEntry{scores: make(map[string]float64), expire: s.ttlFromSec(ttl)}
	for i := 0; i < n && off+12 <= len(payload); i++ {
		score := math.Float64frombits(binary.BigEndian.Uint64(payload[off:]))
		off += 8
		mlen := binary.BigEndian.Uint32(payload[off:])
		off += 4
		if off+int(mlen) > len(payload) {
			break
		}
		member := string(payload[off : off+int(mlen)])
		off += int(mlen)
		ze.scores[member] = score
		ze.order = append(ze.order, member)
	}
	// Sort order by score
	sort.Slice(ze.order, func(i, j int) bool {
		si, sj := ze.scores[ze.order[i]], ze.scores[ze.order[j]]
		if si != sj {
			return si < sj
		}
		return ze.order[i] < ze.order[j]
	})
	s.zsets[k] = ze
	return nil
}
