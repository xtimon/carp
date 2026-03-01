package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// RDB magic and version (Redis-compatible style)
const (
	rdbMagic = "CARP"
	rdbVers  = 1
)

// SaveToFile writes a point-in-time snapshot to path (RDB-style).
// Format: [4 byte magic][4 byte version][foreach key: 4 keylen, key, 4 dumplen, dump].
// Uses .tmp + rename for atomic write.
func (s *Storage) SaveToFile(path string) error {
	keys, err := s.Keys([]byte("*"))
	if err != nil {
		return err
	}
	if keys == nil {
		keys = [][]byte{}
	}

	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer f.Close()

	// Header
	if _, err := f.Write([]byte(rdbMagic)); err != nil {
		os.Remove(tmp)
		return err
	}
	var vers [4]byte
	binary.BigEndian.PutUint32(vers[:], uint32(rdbVers))
	if _, err := f.Write(vers[:]); err != nil {
		os.Remove(tmp)
		return err
	}

	for _, key := range keys {
		dump, err := s.DumpKey(key)
		if err != nil || dump == nil {
			continue
		}
		// key_len (4) + key + dump_len (4) + dump
		var buf [8]byte
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(key)))
		binary.BigEndian.PutUint32(buf[4:8], uint32(len(dump)))
		if _, err := f.Write(buf[:]); err != nil {
			os.Remove(tmp)
			return err
		}
		if _, err := f.Write(key); err != nil {
			os.Remove(tmp)
			return err
		}
		if _, err := f.Write(dump); err != nil {
			os.Remove(tmp)
			return err
		}
	}

	if err := f.Sync(); err != nil {
		os.Remove(tmp)
		return err
	}
	f.Close()
	return os.Rename(tmp, path)
}

// LoadFromFile loads a snapshot from path. Merges with existing data (does not FlushDB first);
// keys in file overwrite existing keys. If path does not exist, returns nil (no error).
func (s *Storage) LoadFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	// Header
	magic := make([]byte, 4)
	if _, err := f.Read(magic); err != nil {
		return err
	}
	if string(magic) != rdbMagic {
		return errors.New("invalid RDB file: bad magic")
	}
	vers := make([]byte, 4)
	if _, err := f.Read(vers); err != nil {
		return err
	}
	// Ignore version for now
	_ = binary.BigEndian.Uint32(vers)

	for {
		// key_len
		kl := make([]byte, 4)
		if _, err := io.ReadFull(f, kl); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		keyLen := binary.BigEndian.Uint32(kl)
		if keyLen == 0 {
			break
		}
		// dump_len
		dl := make([]byte, 4)
		if _, err := io.ReadFull(f, dl); err != nil {
			return err
		}
		dumpLen := binary.BigEndian.Uint32(dl)

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(f, key); err != nil {
			return err
		}
		dump := make([]byte, dumpLen)
		if _, err := io.ReadFull(f, dump); err != nil {
			return err
		}
		if err := s.RestoreKey(key, dump); err != nil {
			return err
		}
	}
	return nil
}
