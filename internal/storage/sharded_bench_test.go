package storage

// Prototype: minimal sharded KV to validate that the parallel-throughput
// regression on the global RWMutex (see BenchmarkStorage_*_Parallel) is
// caused by lock contention rather than any other property of the store.
//
// Compares apples-to-apples against the real Storage on Set/Get/Incr with
// pre-built keys so fmt.Sprintf overhead does not pollute the results.

import (
	"fmt"
	"hash/maphash"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

const benchShardCount = 256

type shardedKV struct {
	shards [benchShardCount]*kvShard
	seed   maphash.Seed
}

type kvShard struct {
	mu   sync.Mutex
	data map[string][]byte
	_    [40]byte // pad to keep adjacent shards off the same 64B cache line
}

func newShardedKV() *shardedKV {
	s := &shardedKV{seed: maphash.MakeSeed()}
	for i := range s.shards {
		s.shards[i] = &kvShard{data: make(map[string][]byte)}
	}
	return s
}

func (s *shardedKV) shardFor(key []byte) *kvShard {
	h := maphash.Bytes(s.seed, key)
	return s.shards[h&(benchShardCount-1)]
}

func (s *shardedKV) Set(key, val []byte) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.data[string(key)] = val
	sh.mu.Unlock()
}

func (s *shardedKV) Get(key []byte) []byte {
	sh := s.shardFor(key)
	sh.mu.Lock()
	v := sh.data[string(key)]
	sh.mu.Unlock()
	return v
}

func (s *shardedKV) Incr(key []byte, delta int) int {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	cur := 0
	if v, ok := sh.data[string(key)]; ok {
		cur, _ = strconv.Atoi(string(v))
	}
	cur += delta
	sh.data[string(key)] = []byte(strconv.Itoa(cur))
	return cur
}

func makeKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("bench:key:%d", i))
	}
	return keys
}

// ---- existing Storage, with pre-built keys (no fmt.Sprintf in hot path) ----

func BenchmarkStorage_Set_ParallelClean(b *testing.B) {
	s := New()
	val := []byte("value")
	keys := makeKeys(1024)
	mask := uint64(len(keys) - 1)
	var ctr uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&ctr, 1)
			s.Set(keys[i&mask], val, nil)
		}
	})
}

func BenchmarkStorage_Get_ParallelClean(b *testing.B) {
	s := New()
	keys := makeKeys(1024)
	val := []byte("value")
	for _, k := range keys {
		s.Set(k, val, nil)
	}
	mask := uint64(len(keys) - 1)
	var ctr uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&ctr, 1)
			s.Get(keys[i&mask])
		}
	})
}

func BenchmarkStorage_Incr_ParallelClean(b *testing.B) {
	s := New()
	keys := makeKeys(128)
	for _, k := range keys {
		s.Set(k, []byte("0"), nil)
	}
	mask := uint64(len(keys) - 1)
	var ctr uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&ctr, 1)
			s.Incr(keys[i&mask], 1)
		}
	})
}

// ---- sharded prototype, same workload ----

func BenchmarkSharded_Set_Parallel(b *testing.B) {
	s := newShardedKV()
	val := []byte("value")
	keys := makeKeys(1024)
	mask := uint64(len(keys) - 1)
	var ctr uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&ctr, 1)
			s.Set(keys[i&mask], val)
		}
	})
}

func BenchmarkSharded_Get_Parallel(b *testing.B) {
	s := newShardedKV()
	keys := makeKeys(1024)
	val := []byte("value")
	for _, k := range keys {
		s.Set(k, val)
	}
	mask := uint64(len(keys) - 1)
	var ctr uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&ctr, 1)
			s.Get(keys[i&mask])
		}
	})
}

func BenchmarkSharded_Incr_Parallel(b *testing.B) {
	s := newShardedKV()
	keys := makeKeys(128)
	for _, k := range keys {
		s.Set(k, []byte("0"))
	}
	mask := uint64(len(keys) - 1)
	var ctr uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&ctr, 1)
			s.Incr(keys[i&mask], 1)
		}
	})
}
