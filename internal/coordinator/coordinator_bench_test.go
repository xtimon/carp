package coordinator

import (
	"testing"

	"github.com/carp/internal/cluster"
	"github.com/carp/internal/partitioner"
	"github.com/carp/internal/storage"
)

func setupCoordinator() *Coordinator {
	gossip := cluster.NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	part := partitioner.NewPartitioner(1)
	part.SetNodes([]string{"node1"}, map[string]string{"node1": "rack1"})
	store := storage.New()
	return New("node1", gossip, part, store, 1, "test")
}

func BenchmarkCoordinator_Execute_PING(b *testing.B) {
	c := setupCoordinator()
	cmd := []byte("PING")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Execute(cmd, nil, nil)
	}
}

func BenchmarkCoordinator_Execute_GET(b *testing.B) {
	c := setupCoordinator()
	key := []byte("bench:key")
	c.Storage.Set(key, []byte("value"), nil)
	cmd := []byte("GET")
	args := [][]byte{key}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Execute(cmd, args, nil)
	}
}

func BenchmarkCoordinator_Execute_SET(b *testing.B) {
	c := setupCoordinator()
	key := []byte("bench:key")
	val := []byte("value")
	cmd := []byte("SET")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := [][]byte{key, val}
		c.Execute(cmd, args, nil)
	}
}

func BenchmarkCoordinator_Execute_INCR(b *testing.B) {
	c := setupCoordinator()
	key := []byte("bench:incr")
	c.Storage.Set(key, []byte("0"), nil)
	cmd := []byte("INCR")
	args := [][]byte{key}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Execute(cmd, args, nil)
	}
}

func BenchmarkCoordinator_Execute_INCRBY(b *testing.B) {
	c := setupCoordinator()
	key := []byte("bench:incr")
	c.Storage.Set(key, []byte("0"), nil)
	cmd := []byte("INCRBY")
	delta := []byte("1")
	args := [][]byte{key, delta}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Execute(cmd, args, nil)
	}
}

func BenchmarkCoordinator_Execute_ECHO(b *testing.B) {
	c := setupCoordinator()
	cmd := []byte("ECHO")
	args := [][]byte{[]byte("hello")}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Execute(cmd, args, nil)
	}
}

func BenchmarkCoordinator_Execute_DBSIZE(b *testing.B) {
	c := setupCoordinator()
	for i := 0; i < 100; i++ {
		c.Storage.Set([]byte{byte('a' + i%26), byte('0' + i%10)}, []byte("v"), nil)
	}
	cmd := []byte("DBSIZE")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Execute(cmd, nil, nil)
	}
}

func BenchmarkCoordinator_Execute_KEYS(b *testing.B) {
	c := setupCoordinator()
	for i := 0; i < 100; i++ {
		c.Storage.Set([]byte{byte('a' + i%26), byte('0' + i%10)}, []byte("v"), nil)
	}
	cmd := []byte("KEYS")
	args := [][]byte{[]byte("*")}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Execute(cmd, args, nil)
	}
}
