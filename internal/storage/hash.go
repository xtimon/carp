package storage

import "time"

// Hash storage: key -> field -> value
type hashEntry struct {
	fields map[string][]byte
	expire *time.Time
}

// HSet sets field=value, returns 1 if new field else 0 (clears tombstone)
func (s *Storage) HSet(key, field, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireHash(key)
	k := s.key(key)
	delete(s.tombs, k)
	he := s.hashes[k]
	if he == nil {
		he = &hashEntry{fields: make(map[string][]byte)}
		s.hashes[k] = he
	}
	fs := string(field)
	_, existed := he.fields[fs]
	he.fields[fs] = value
	if existed {
		return 0, nil
	}
	return 1, nil
}

// HGet returns field value or nil
func (s *Storage) HGet(key, field []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireHash(key)
	he := s.hashes[s.key(key)]
	if he == nil {
		return nil, nil
	}
	return he.fields[string(field)], nil
}

// HDel removes fields, returns count removed
func (s *Storage) HDel(key []byte, fields ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireHash(key)
	k := s.key(key)
	he := s.hashes[k]
	if he == nil {
		return 0, nil
	}
	removed := 0
	for _, f := range fields {
		fs := string(f)
		if _, ok := he.fields[fs]; ok {
			delete(he.fields, fs)
			removed++
		}
	}
	if len(he.fields) == 0 {
		delete(s.hashes, k)
	}
	return removed, nil
}

// HExists returns 1 if field exists, 0 otherwise
func (s *Storage) HExists(key, field []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireHash(key)
	he := s.hashes[s.key(key)]
	if he == nil {
		return 0, nil
	}
	if _, ok := he.fields[string(field)]; ok {
		return 1, nil
	}
	return 0, nil
}

// HLen returns number of fields
func (s *Storage) HLen(key []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireHash(key)
	he := s.hashes[s.key(key)]
	if he == nil {
		return 0, nil
	}
	return len(he.fields), nil
}

// HGetAll returns all field-value pairs [f1,v1,f2,v2,...]
func (s *Storage) HGetAll(key []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireHash(key)
	he := s.hashes[s.key(key)]
	if he == nil {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, len(he.fields)*2)
	for f, v := range he.fields {
		out = append(out, []byte(f), v)
	}
	return out, nil
}

// HKeys returns all field names
func (s *Storage) HKeys(key []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireHash(key)
	he := s.hashes[s.key(key)]
	if he == nil {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, len(he.fields))
	for f := range he.fields {
		out = append(out, []byte(f))
	}
	return out, nil
}

// HVals returns all values
func (s *Storage) HVals(key []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeExpireHash(key)
	he := s.hashes[s.key(key)]
	if he == nil {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, len(he.fields))
	for _, v := range he.fields {
		out = append(out, v)
	}
	return out, nil
}

func (s *Storage) maybeExpireHash(key []byte) {
	k := s.key(key)
	he := s.hashes[k]
	if he == nil || he.expire == nil {
		return
	}
	if time.Now().After(*he.expire) {
		delete(s.hashes, k)
	}
}
