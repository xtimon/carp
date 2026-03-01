package storage

import (
	"sort"
	"strconv"
	"time"
)

// zsetEntry stores a sorted set. scores is source of truth; order is lazily rebuilt.
// order nil means dirty (ZAdd/ZRem just ran); ensureOrder rebuilds before ZRange/ZRank.
type zsetEntry struct {
	scores map[string]float64
	order  []string // members sorted by score for range (nil = dirty, needs rebuild)
	expire *time.Time
}

func (z *zsetEntry) rebuildOrder() {
	z.order = make([]string, 0, len(z.scores))
	for m := range z.scores {
		z.order = append(z.order, m)
	}
	sort.Slice(z.order, func(i, j int) bool {
		si, sj := z.scores[z.order[i]], z.scores[z.order[j]]
		if si != sj {
			return si < sj
		}
		return z.order[i] < z.order[j]
	})
}

// ensureOrder rebuilds order slice if dirty (nil)
func (z *zsetEntry) ensureOrder() {
	if z.order == nil {
		z.rebuildOrder()
	}
}

// ZAdd adds/updates members. Args: [score1, member1, score2, member2, ...]
// Returns count of new members added (clears tombstone).
// Uses lazy order rebuild: marks dirty on write, rebuilds on first read.
func (s *Storage) ZAdd(key []byte, args [][]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireZSet(key)
	k := s.key(key)
	delete(s.tombs, k)
	ze := s.zsets[k]
	if ze == nil {
		ze = &zsetEntry{scores: make(map[string]float64)}
		s.zsets[k] = ze
	}
	added := 0
	for i := 0; i+1 < len(args); i += 2 {
		score, _ := strconv.ParseFloat(string(args[i]), 64)
		member := string(args[i+1])
		if _, existed := ze.scores[member]; !existed {
			added++
		}
		ze.scores[member] = score
	}
	ze.order = nil // mark dirty; rebuild on ZRange/ZRank/etc
	return added, nil
}

// ZRem removes members, returns count removed
func (s *Storage) ZRem(key []byte, members ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireZSet(key)
	k := s.key(key)
	ze := s.zsets[k]
	if ze == nil {
		return 0, nil
	}
	removed := 0
	for _, m := range members {
		ms := string(m)
		if _, ok := ze.scores[ms]; ok {
			delete(ze.scores, ms)
			removed++
		}
	}
	if len(ze.scores) == 0 {
		delete(s.zsets, k)
	} else {
		ze.order = nil
	}
	return removed, nil
}

// ZScore returns member's score
func (s *Storage) ZScore(key, member []byte) (float64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireZSet(key)
	ze := s.zsets[s.key(key)]
	if ze == nil {
		return 0, false, nil
	}
	score, ok := ze.scores[string(member)]
	return score, ok, nil
}

// ZCard returns sorted set size
func (s *Storage) ZCard(key []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireZSet(key)
	ze := s.zsets[s.key(key)]
	if ze == nil {
		return 0, nil
	}
	return len(ze.scores), nil
}

// ZRank returns 0-based rank (lowest score = 0)
func (s *Storage) ZRank(key, member []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireZSet(key)
	ze := s.zsets[s.key(key)]
	if ze == nil {
		return -1, nil
	}
	ze.ensureOrder()
	ms := string(member)
	if _, ok := ze.scores[ms]; !ok {
		return -1, nil
	}
	for i, m := range ze.order {
		if m == ms {
			return i, nil
		}
	}
	return -1, nil
}

// ZRevRank returns rank from high to low
func (s *Storage) ZRevRank(key, member []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireZSet(key)
	ze := s.zsets[s.key(key)]
	if ze == nil {
		return -1, nil
	}
	ze.ensureOrder()
	ms := string(member)
	if _, ok := ze.scores[ms]; !ok {
		return -1, nil
	}
	n := len(ze.order)
	for i, m := range ze.order {
		if m == ms {
			return n - 1 - i, nil
		}
	}
	return -1, nil
}

// ZRange returns members from start to stop (inclusive)
func (s *Storage) ZRange(key []byte, start, stop int, withScores bool) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireZSet(key)
	ze := s.zsets[s.key(key)]
	if ze == nil {
		return [][]byte{}, nil
	}
	ze.ensureOrder()
	n := len(ze.order)
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
	var out [][]byte
	for i := start; i <= stop; i++ {
		m := ze.order[i]
		out = append(out, []byte(m))
		if withScores {
			out = append(out, []byte(strconv.FormatFloat(ze.scores[m], 'f', -1, 64)))
		}
	}
	return out, nil
}

func (s *Storage) maybeExpireZSet(key []byte) {
	k := s.key(key)
	ze := s.zsets[k]
	if ze == nil || ze.expire == nil {
		return
	}
	if time.Now().After(*ze.expire) {
		delete(s.zsets, k)
	}
}
