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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireSet(key)
	k := s.key(key)
	delete(s.tombs, k)
	se := s.sets[k]
	if se == nil {
		se = &setEntry{members: make(map[string]bool)}
		s.sets[k] = se
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireSet(key)
	k := s.key(key)
	se := s.sets[k]
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
		delete(s.sets, k)
	}
	return removed, nil
}

// SIsMember returns 1 if member in set, 0 otherwise
func (s *Storage) SIsMember(key, member []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireSet(key)
	se := s.sets[s.key(key)]
	if se == nil {
		return 0, nil
	}
	if se.members[string(member)] {
		return 1, nil
	}
	return 0, nil
}

// SMembers returns all members
func (s *Storage) SMembers(key []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireSet(key)
	se := s.sets[s.key(key)]
	if se == nil {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, len(se.members))
	for m := range se.members {
		out = append(out, []byte(m))
	}
	return out, nil
}

// SCard returns set size
func (s *Storage) SCard(key []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireSet(key)
	se := s.sets[s.key(key)]
	if se == nil {
		return 0, nil
	}
	return len(se.members), nil
}

// SPop removes and returns random member
func (s *Storage) SPop(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireSet(key)
	k := s.key(key)
	se := s.sets[k]
	if se == nil || len(se.members) == 0 {
		return nil, nil
	}
	idx := rand.Intn(len(se.members))
	for m := range se.members {
		if idx == 0 {
			delete(se.members, m)
			if len(se.members) == 0 {
				delete(s.sets, k)
			}
			return []byte(m), nil
		}
		idx--
	}
	return nil, nil
}

func (s *Storage) maybeExpireSet(key []byte) {
	k := s.key(key)
	se := s.sets[k]
	if se == nil || se.expire == nil {
		return
	}
	if time.Now().After(*se.expire) {
		delete(s.sets, k)
	}
}
