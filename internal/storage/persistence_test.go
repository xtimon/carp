package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.rdb")

	s := New()
	s.Set([]byte("k1"), []byte("v1"), nil)
	s.Set([]byte("k2"), []byte("v2"), nil)
	s.LPush([]byte("list1"), []byte("a"), []byte("b"))

	if err := s.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("dump file not created: %v", err)
	}

	s2 := New()
	if err := s2.LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	v1, _ := s2.Get([]byte("k1"))
	if string(v1) != "v1" {
		t.Errorf("k1 = %q, want v1", v1)
	}
	v2, _ := s2.Get([]byte("k2"))
	if string(v2) != "v2" {
		t.Errorf("k2 = %q, want v2", v2)
	}
	items, _ := s2.LRange([]byte("list1"), 0, -1)
	if len(items) != 2 || string(items[0]) != "a" || string(items[1]) != "b" {
		t.Errorf("list1 = %v, want [a b]", items)
	}
}

func TestLoadFromFile_NotExist(t *testing.T) {
	s := New()
	if err := s.LoadFromFile("/nonexistent/dump.rdb"); err != nil {
		t.Errorf("LoadFromFile on missing file should return nil: %v", err)
	}
}
