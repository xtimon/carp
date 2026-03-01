package storage

import (
	"reflect"
	"testing"
	"time"
)

func TestStorage_BasicGetSet(t *testing.T) {
	s := New()
	key := []byte("foo")
	val := []byte("bar")
	if got, _ := s.Get(key); got != nil {
		t.Errorf("Get missing key should return nil, got %v", got)
	}
	s.Set(key, val, nil)
	got, _ := s.Get(key)
	if string(got) != string(val) {
		t.Errorf("Get = %q, want %q", got, val)
	}
}

func TestStorage_Delete(t *testing.T) {
	s := New()
	key := []byte("k")
	s.Set(key, []byte("v"), nil)
	ok, _ := s.Delete(key)
	if !ok {
		t.Error("Delete existing key should return true")
	}
	got, _ := s.Get(key)
	if got != nil {
		t.Errorf("Get after Delete should return nil, got %v", got)
	}
	ok2, _ := s.Delete(key)
	if ok2 {
		t.Error("Delete non-existent key should return false")
	}
}

func TestStorage_SetTombstone(t *testing.T) {
	s := New()
	key := []byte("k")
	s.Set(key, []byte("v"), nil)
	ok, _ := s.SetTombstone(key)
	if !ok {
		t.Error("SetTombstone existing key should return true")
	}
	got, _ := s.Get(key)
	if got != nil {
		t.Errorf("Get after SetTombstone should return nil, got %v", got)
	}
	if n, _ := s.Exists(key); n != 0 {
		t.Errorf("Exists after SetTombstone = %d, want 0", n)
	}
	// Overwriting clears tombstone
	s.Set(key, []byte("v2"), nil)
	if got, _ := s.Get(key); string(got) != "v2" {
		t.Errorf("Get after overwrite = %q, want v2", got)
	}
}

func TestStorage_RunTombstoneGC(t *testing.T) {
	s := New()
	s.TombstoneGracePeriod = 50 * time.Millisecond
	key := []byte("k")
	s.Set(key, []byte("v"), nil)
	s.SetTombstone(key)
	if n := s.RunTombstoneGC(); n != 0 {
		t.Errorf("GC immediately should purge 0 (tombstone too new), got %d", n)
	}
	time.Sleep(60 * time.Millisecond)
	if n := s.RunTombstoneGC(); n != 1 {
		t.Errorf("GC after grace should purge 1, got %d", n)
	}
	// Key no longer in tombs - Get returns nil (key doesn't exist)
	got, _ := s.Get(key)
	if got != nil {
		t.Errorf("Get after GC purge = %v, want nil", got)
	}
}

func TestStorage_Exists(t *testing.T) {
	s := New()
	if n, _ := s.Exists([]byte("x")); n != 0 {
		t.Errorf("Exists missing = %d, want 0", n)
	}
	s.Set([]byte("x"), []byte("1"), nil)
	if n, _ := s.Exists([]byte("x")); n != 1 {
		t.Errorf("Exists present = %d, want 1", n)
	}
}

func TestStorage_Incr(t *testing.T) {
	s := New()
	key := []byte("n")
	v, err := s.Incr(key, 1)
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Errorf("Incr from 0 = %d, want 1", v)
	}
	v, _ = s.Incr(key, 5)
	if v != 6 {
		t.Errorf("Incr 1+5 = %d, want 6", v)
	}
}

func TestStorage_IncrByWithIdempotency(t *testing.T) {
	s := New()
	key := []byte("cnt")
	token := []byte("req-123")
	v, err := s.IncrByWithIdempotency(key, token, 5)
	if err != nil {
		t.Fatal(err)
	}
	if v != 5 {
		t.Errorf("first incr = %d, want 5", v)
	}
	// Retry with same token: should return cached, no over-count
	v2, err := s.IncrByWithIdempotency(key, token, 5)
	if err != nil {
		t.Fatal(err)
	}
	if v2 != 5 {
		t.Errorf("retry should return cached 5, got %d", v2)
	}
	got, _ := s.Get(key)
	if string(got) != "5" {
		t.Errorf("value should still be 5 after retry, got %s", got)
	}
	// Different token: apply normally
	v3, _ := s.IncrByWithIdempotency(key, []byte("req-456"), 3)
	if v3 != 8 {
		t.Errorf("new token incr 3 = %d, want 8", v3)
	}
}

func TestStorage_Keys(t *testing.T) {
	s := New()
	s.Set([]byte("a"), []byte("1"), nil)
	s.Set([]byte("b"), []byte("2"), nil)
	s.Set([]byte("key:1"), []byte("v1"), nil)
	s.Set([]byte("key:2"), []byte("v2"), nil)
	keys, _ := s.Keys([]byte("*"))
	if len(keys) != 4 {
		t.Errorf("Keys(*) = %d, want 4", len(keys))
	}
	keysA, _ := s.Keys([]byte("a"))
	if len(keysA) != 1 || string(keysA[0]) != "a" {
		t.Errorf("Keys(a) = %v", keysA)
	}
	keysPrefix, _ := s.Keys([]byte("key:*"))
	if len(keysPrefix) != 2 {
		t.Errorf("Keys(key:*) = %d, want 2", len(keysPrefix))
	}
}

func TestStorage_SetNX(t *testing.T) {
	s := New()
	key := []byte("nx")
	n, _ := s.SetNX(key, []byte("v1"))
	if n != 1 {
		t.Errorf("SetNX first = %d, want 1", n)
	}
	n, _ = s.SetNX(key, []byte("v2"))
	if n != 0 {
		t.Errorf("SetNX second = %d, want 0", n)
	}
	got, _ := s.Get(key)
	if string(got) != "v1" {
		t.Errorf("value should remain v1, got %q", got)
	}
}

func TestStorage_GetSetCmd(t *testing.T) {
	s := New()
	key := []byte("gs")
	old, _ := s.GetSet(key, []byte("new"))
	if old != nil {
		t.Errorf("GetSet on new key old = %v, want nil", old)
	}
	old, _ = s.GetSet(key, []byte("newer"))
	if string(old) != "new" {
		t.Errorf("GetSet old = %q, want new", old)
	}
	got, _ := s.Get(key)
	if string(got) != "newer" {
		t.Errorf("Get = %q, want newer", got)
	}
}

func TestStorage_LPush_LPop(t *testing.T) {
	s := New()
	key := []byte("list")
	n, _ := s.LPush(key, []byte("a"), []byte("b"))
	if n != 2 {
		t.Errorf("LPush len = %d, want 2", n)
	}
	got, _ := s.LPop(key)
	if string(got) != "a" {
		t.Errorf("LPop first = %q, want a (first pushed at head)", got)
	}
	got, _ = s.LPop(key)
	if string(got) != "b" {
		t.Errorf("LPop second = %q, want b", got)
	}
	got, _ = s.LPop(key)
	if got != nil {
		t.Errorf("LPop empty = %v, want nil", got)
	}
}

func TestStorage_LRange(t *testing.T) {
	s := New()
	key := []byte("list")
	s.RPush(key, []byte("a"), []byte("b"), []byte("c"))
	items, _ := s.LRange(key, 0, -1)
	if len(items) != 3 {
		t.Fatalf("LRange 0 -1 = %d items", len(items))
	}
	if string(items[0]) != "a" || string(items[1]) != "b" || string(items[2]) != "c" {
		t.Errorf("LRange = %v", items)
	}
}

func TestStorage_SAdd_SMembers(t *testing.T) {
	s := New()
	key := []byte("set")
	s.SAdd(key, []byte("a"), []byte("b"), []byte("a"))
	n, _ := s.SCard(key)
	if n != 2 {
		t.Errorf("SCard = %d, want 2 (dup ignored)", n)
	}
	members, _ := s.SMembers(key)
	if len(members) != 2 {
		t.Fatalf("SMembers = %d", len(members))
	}
}

func TestStorage_HSet_HGet(t *testing.T) {
	s := New()
	key := []byte("hash")
	s.HSet(key, []byte("f1"), []byte("v1"))
	s.HSet(key, []byte("f2"), []byte("v2"))
	got, _ := s.HGet(key, []byte("f1"))
	if string(got) != "v1" {
		t.Errorf("HGet = %q, want v1", got)
	}
	n, _ := s.HLen(key)
	if n != 2 {
		t.Errorf("HLen = %d, want 2", n)
	}
}

func TestStorage_ZAdd_ZRange(t *testing.T) {
	s := New()
	key := []byte("zset")
	s.ZAdd(key, [][]byte{[]byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("0.5"), []byte("c")})
	items, _ := s.ZRange(key, 0, -1, false)
	if len(items) != 3 {
		t.Fatalf("ZRange = %d items", len(items))
	}
	if string(items[0]) != "c" || string(items[1]) != "a" || string(items[2]) != "b" {
		t.Errorf("ZRange by score order = %v", items)
	}
}

func TestStorage_Type(t *testing.T) {
	s := New()
	if s.Type([]byte("x")) != "none" {
		t.Errorf("Type missing = %q", s.Type([]byte("x")))
	}
	s.Set([]byte("x"), []byte("v"), nil)
	if s.Type([]byte("x")) != "string" {
		t.Errorf("Type string = %q", s.Type([]byte("x")))
	}
}

func TestStorage_FlushDB(t *testing.T) {
	s := New()
	s.Set([]byte("a"), []byte("1"), nil)
	s.FlushDB()
	keys, _ := s.Keys([]byte("*"))
	if len(keys) != 0 {
		t.Errorf("FlushDB: keys remain %d", len(keys))
	}
}

func TestStorage_DumpKey_RestoreKey(t *testing.T) {
	s := New()
	key := []byte("foo")
	s.Set(key, []byte("bar"), nil)
	dump, err := s.DumpKey(key)
	if err != nil || dump == nil {
		t.Fatalf("DumpKey failed: %v", err)
	}
	s.Delete(key)
	if got, _ := s.Get(key); got != nil {
		t.Fatal("key should be deleted")
	}
	err = s.RestoreKey(key, dump)
	if err != nil {
		t.Fatalf("RestoreKey failed: %v", err)
	}
	got, _ := s.Get(key)
	if string(got) != "bar" {
		t.Errorf("RestoreKey: Get = %q, want bar", got)
	}
}

func TestStorage_DumpKey_RestoreKey_List(t *testing.T) {
	s := New()
	key := []byte("listkey")
	s.RPush(key, []byte("a"), []byte("b"))
	dump, err := s.DumpKey(key)
	if err != nil || dump == nil {
		t.Fatalf("DumpKey list failed: %v", err)
	}
	s.Delete(key)
	s.RestoreKey(key, dump)
	items, _ := s.LRange(key, 0, -1)
	if !reflect.DeepEqual([][]byte{[]byte("a"), []byte("b")}, items) {
		t.Errorf("RestoreKey list = %v", items)
	}
}
