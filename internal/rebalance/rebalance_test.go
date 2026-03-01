package rebalance

import (
	"testing"
	"time"

	"github.com/carp/internal/cluster"
	"github.com/carp/internal/partitioner"
	"github.com/carp/internal/storage"
)

func TestNew(t *testing.T) {
	gossip := cluster.NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	part := partitioner.NewPartitioner(2)
	store := storage.New()
	rb := New("node1", gossip, part, store)
	if rb.NodeID != "node1" || rb.Gossip != gossip || rb.Partitioner != part || rb.Storage != store {
		t.Error("New: fields not set correctly")
	}
}

func TestSetInterval(t *testing.T) {
	gossip := cluster.NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	part := partitioner.NewPartitioner(2)
	store := storage.New()
	rb := New("node1", gossip, part, store)
	rb.SetInterval(100 * time.Millisecond)
	if rb.interval != 100*time.Millisecond {
		t.Errorf("SetInterval: got %v", rb.interval)
	}
}

func TestRunRepair_NoKeys(t *testing.T) {
	gossip := cluster.NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	part := partitioner.NewPartitioner(2)
	part.SetNodes([]string{"node1", "node2"}, nil)
	store := storage.New()

	rb := New("node1", gossip, part, store)
	n := rb.RunRepair()
	if n != 0 {
		t.Errorf("RunRepair with no keys = %d, want 0", n)
	}
}

func TestRunRepair_EmptyRing(t *testing.T) {
	gossip := cluster.NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	part := partitioner.NewPartitioner(2)
	store := storage.New()
	store.Set([]byte("foo"), []byte("bar"), nil)

	rb := New("node1", gossip, part, store)
	n := rb.RunRepair()
	if n != 0 {
		t.Errorf("RunRepair with empty ring = %d, want 0", n)
	}
}

func TestRunRepair_SingleNode_NoOtherReplicas(t *testing.T) {
	gossip := cluster.NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	part := partitioner.NewPartitioner(1)
	part.SetNodes([]string{"node1"}, nil)
	store := storage.New()
	store.Set([]byte("foo"), []byte("bar"), nil)

	rb := New("node1", gossip, part, store)
	n := rb.RunRepair()
	// We're the only replica; runRepair skips self when sending. So 0 repaired.
	if n != 0 {
		t.Errorf("RunRepair when we're only replica = %d, want 0", n)
	}
}

func TestRunRepairSmooth(t *testing.T) {
	gossip := cluster.NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	part := partitioner.NewPartitioner(2)
	part.SetNodes([]string{"node1"}, nil)
	store := storage.New()

	rb := New("node1", gossip, part, store)
	n := rb.RunRepairSmooth(0)
	if n != 0 {
		t.Errorf("RunRepairSmooth = %d, want 0", n)
	}
}

func TestStartStop(t *testing.T) {
	gossip := cluster.NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	part := partitioner.NewPartitioner(2)
	store := storage.New()
	rb := New("node1", gossip, part, store)
	rb.SetInterval(1 * time.Hour) // avoid flapping during test

	rb.Start()
	rb.Start() // idempotent
	rb.Stop()
	rb.Stop() // idempotent
}
