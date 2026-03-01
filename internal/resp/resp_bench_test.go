package resp

import (
	"testing"
)

func BenchmarkEncodeCommand_PING(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeCommand("PING")
	}
}

func BenchmarkEncodeCommand_GET(b *testing.B) {
	key := []byte("bench:key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeCommand("GET", key)
	}
}

func BenchmarkEncodeCommand_SET(b *testing.B) {
	key := []byte("bench:key")
	val := []byte("value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeCommand("SET", key, val)
	}
}

func BenchmarkEncodeCommand_SET_Large(b *testing.B) {
	key := []byte("bench:key")
	val := make([]byte, 1024)
	for i := range val {
		val[i] = 'x'
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeCommand("SET", key, val)
	}
}

func BenchmarkEncodeCommand_INCR(b *testing.B) {
	key := []byte("bench:incr")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeCommand("INCR", key)
	}
}

func BenchmarkEncodeArray(b *testing.B) {
	items := [][]byte{
		[]byte("SET"),
		[]byte("key"),
		[]byte("value"),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeArray(items)
	}
}

func BenchmarkEncodeInteger(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeInteger(42)
	}
}

func BenchmarkEncodeBulkString(b *testing.B) {
	val := []byte("hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeBulkString(val)
	}
}
