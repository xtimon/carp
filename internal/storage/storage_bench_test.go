package storage

import (
	"fmt"
	"testing"
)

func BenchmarkStorage_Set(b *testing.B) {
	s := New()
	key := []byte("bench:key")
	val := []byte("value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(key, val, nil)
	}
}

func BenchmarkStorage_Set_Parallel(b *testing.B) {
	s := New()
	val := []byte("value")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("bench:key:%d", i))
			s.Set(key, val, nil)
			i++
		}
	})
}

func BenchmarkStorage_Get(b *testing.B) {
	s := New()
	key := []byte("bench:key")
	s.Set(key, []byte("value"), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(key)
	}
}

func BenchmarkStorage_Get_Parallel(b *testing.B) {
	s := New()
	for i := 0; i < 1000; i++ {
		s.Set([]byte(fmt.Sprintf("bench:key:%d", i)), []byte("value"), nil)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("bench:key:%d", i%1000))
			s.Get(key)
			i++
		}
	})
}

func BenchmarkStorage_Incr(b *testing.B) {
	s := New()
	key := []byte("bench:incr")
	s.Set(key, []byte("0"), nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Incr(key, 1)
	}
}

func BenchmarkStorage_Incr_Parallel(b *testing.B) {
	s := New()
	for i := 0; i < 100; i++ {
		s.Set([]byte(fmt.Sprintf("bench:incr:%d", i)), []byte("0"), nil)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("bench:incr:%d", i%100))
			s.Incr(key, 1)
			i++
		}
	})
}

func BenchmarkStorage_IncrByWithIdempotency(b *testing.B) {
	s := New()
	key := []byte("bench:incr")
	token := []byte("req-token")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.IncrByWithIdempotency(key, token, 1)
	}
}

func BenchmarkStorage_SetGet(b *testing.B) {
	s := New()
	key := []byte("bench:key")
	val := []byte("value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(key, val, nil)
		s.Get(key)
	}
}

func BenchmarkStorage_Keys_Star(b *testing.B) {
	s := New()
	for i := 0; i < 1000; i++ {
		s.Set([]byte(fmt.Sprintf("bench:key:%d", i)), []byte("v"), nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Keys([]byte("*"))
	}
}

func BenchmarkStorage_Keys_Prefix(b *testing.B) {
	s := New()
	for i := 0; i < 1000; i++ {
		s.Set([]byte(fmt.Sprintf("bench:key:%d", i)), []byte("v"), nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Keys([]byte("bench:key:*"))
	}
}

func BenchmarkStorage_LPush(b *testing.B) {
	s := New()
	val := []byte("item")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("bench:list:%d", i%100))
		s.LPush(key, val)
	}
}

func BenchmarkStorage_LPop(b *testing.B) {
	s := New()
	key := []byte("bench:list")
	prefill := 1000
	for i := 0; i < prefill; i++ {
		s.LPush(key, []byte("item"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.LPop(key)
		if i > 0 && (i+1)%prefill == 0 {
			for j := 0; j < prefill; j++ {
				s.LPush(key, []byte("item"))
			}
		}
	}
}

func BenchmarkStorage_HSet(b *testing.B) {
	s := New()
	key := []byte("bench:hash")
	field := []byte("f")
	val := []byte("value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.HSet(key, []byte(fmt.Sprintf("%s%d", field, i)), val)
	}
}

func BenchmarkStorage_HGet(b *testing.B) {
	s := New()
	key := []byte("bench:hash")
	s.HSet(key, []byte("field"), []byte("value"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.HGet(key, []byte("field"))
	}
}

func BenchmarkStorage_ZAdd(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("bench:zset:%d", i%1000))
		score := []byte(fmt.Sprintf("%d", i))
		member := []byte(fmt.Sprintf("m%d", i))
		s.ZAdd(key, [][]byte{score, member})
	}
}

func BenchmarkStorage_SAdd(b *testing.B) {
	s := New()
	key := []byte("bench:set")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SAdd(key, []byte(fmt.Sprintf("member%d", i)))
	}
}

func BenchmarkStorage_Delete(b *testing.B) {
	s := New()
	key := []byte("bench:key")
	for i := 0; i < b.N; i++ {
		s.Set(key, []byte("v"), nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Delete(key)
		s.Set(key, []byte("v"), nil)
	}
}
