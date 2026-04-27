package storage

import (
	"math/rand"
	"time"
)

// Set storage: key -> set of string members
type setEntry struct {
	members map[string]bool
	expire  *time.Time
}

// SAdd adds members to set, returns count of new members (clears tombstone)
func (s *Storage) SAdd(key []byte, members ...[]byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireSet(k)
	delete(sh.tombs, k)
	se := sh.sets[k]
	if se == nil {
		se = &setEntry{members: make(map[string]bool)}
		sh.sets[k] = se
	}
	added := 0
	for _, m := range members {
		ms := string(m)
		if !se.members[ms] {
			se.members[ms] = true
			added++
		}
	}
	return added, nil
}

// SRem removes members, returns count removed
func (s *Storage) SRem(key []byte, members ...[]byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireSet(k)
	se := sh.sets[k]
	if se == nil {
		return 0, nil
	}
	removed := 0
	for _, m := range members {
		ms := string(m)
		if se.members[ms] {
			delete(se.members, ms)
			removed++
		}
	}
	if len(se.members) == 0 {
		delete(sh.sets, k)
	}
	return removed, nil
}

// SIsMember returns 1 if member in set, 0 otherwise (read-only).
func (s *Storage) SIsMember(key, member []byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	se := sh.sets[string(key)]
	if se == nil || expired(se.expire) {
		return 0, nil
	}
	if se.members[string(member)] {
		return 1, nil
	}
	return 0, nil
}

// SMembers returns all members (read-only).
func (s *Storage) SMembers(key []byte) ([][]byte, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	se := sh.sets[string(key)]
	if se == nil || expired(se.expire) {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, len(se.members))
	for m := range se.members {
		out = append(out, []byte(m))
	}
	return out, nil
}

// SCard returns set size (read-only).
func (s *Storage) SCard(key []byte) (int, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	se := sh.sets[string(key)]
	if se == nil || expired(se.expire) {
		return 0, nil
	}
	return len(se.members), nil
}

// SPop removes and returns random member
func (s *Storage) SPop(key []byte) ([]byte, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	sh.maybeExpireSet(k)
	se := sh.sets[k]
	if se == nil || len(se.members) == 0 {
		return nil, nil
	}
	idx := rand.Intn(len(se.members))
	for m := range se.members {
		if idx == 0 {
			delete(se.members, m)
			if len(se.members) == 0 {
				delete(sh.sets, k)
			}
			return []byte(m), nil
		}
		idx--
	}
	return nil, nil
}

func (sh *shard) maybeExpireSet(k string) {
	se := sh.sets[k]
	if se == nil || se.expire == nil {
		return
	}
	if time.Now().After(*se.expire) {
		delete(sh.sets, k)
	}
}
