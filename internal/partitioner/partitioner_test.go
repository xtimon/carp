package partitioner

import (
	"reflect"
	"testing"
)

func TestTokenForKey(t *testing.T) {
	// TokenForKey uses Murmur3; we verify determinism and that different keys yield different tokens
	gotFoo := TokenForKey([]byte("foo"))
	gotBar := TokenForKey([]byte("bar"))
	gotEmpty := TokenForKey([]byte(""))
	if TokenForKey([]byte("foo")) != gotFoo {
		t.Error("TokenForKey should be deterministic")
	}
	if gotFoo == gotBar && len([]byte("foo")) > 0 && len([]byte("bar")) > 0 {
		t.Error("different keys should typically yield different tokens")
	}
	_ = gotEmpty // empty key has some token
}

func TestTokenForKey_Deterministic(t *testing.T) {
	key := []byte("consistent")
	t1 := TokenForKey(key)
	t2 := TokenForKey(key)
	if t1 != t2 {
		t.Errorf("TokenForKey should be deterministic: got %d and %d", t1, t2)
	}
}

func TestPartitioner_GetReplicas_EmptyRing(t *testing.T) {
	p := NewPartitioner(3)
	replicas := p.GetReplicas([]byte("foo"))
	if replicas != nil {
		t.Errorf("GetReplicas with empty ring should return nil, got %v", replicas)
	}
}

func TestPartitioner_GetReplicas_SingleNode(t *testing.T) {
	p := NewPartitioner(3)
	p.SetNodes([]string{"node1"}, nil)
	replicas := p.GetReplicas([]byte("foo"))
	if len(replicas) != 1 {
		t.Errorf("expected 1 replica with 1 node, got %d", len(replicas))
	}
	if replicas[0] != "node1" {
		t.Errorf("expected node1, got %s", replicas[0])
	}
}

func TestPartitioner_GetReplicas_ThreeNodes(t *testing.T) {
	p := NewPartitioner(3)
	p.SetNodes([]string{"node1", "node2", "node3"}, nil)
	replicas := p.GetReplicas([]byte("foo"))
	if len(replicas) != 3 {
		t.Errorf("expected 3 replicas with RF=3 and 3 nodes, got %d", len(replicas))
	}
	seen := make(map[string]bool)
	for _, r := range replicas {
		if seen[r] {
			t.Errorf("duplicate replica: %s", r)
		}
		seen[r] = true
	}
}

func TestPartitioner_GetReplicas_RFLessThanNodes(t *testing.T) {
	p := NewPartitioner(2)
	p.SetNodes([]string{"node1", "node2", "node3"}, nil)
	replicas := p.GetReplicas([]byte("foo"))
	if len(replicas) != 2 {
		t.Errorf("expected 2 replicas with RF=2, got %d", len(replicas))
	}
}

func TestPartitioner_GetReplicas_SameKeySameReplicas(t *testing.T) {
	p := NewPartitioner(3)
	p.SetNodes([]string{"node1", "node2", "node3"}, nil)
	r1 := p.GetReplicas([]byte("foo"))
	r2 := p.GetReplicas([]byte("foo"))
	if !reflect.DeepEqual(r1, r2) {
		t.Errorf("same key should yield same replicas: %v vs %v", r1, r2)
	}
}

func TestPartitioner_GetRing(t *testing.T) {
	p := NewPartitioner(2)
	p.SetNumVnodes(4)
	p.SetNodes([]string{"node1", "node2"}, nil)
	ring := p.GetRing()
	// With 2 nodes and 4 vnodes each: 8 entries
	if len(ring) != 8 {
		t.Errorf("expected 8 ring entries (2 nodes * 4 vnodes), got %d", len(ring))
	}
	// Ring should be sorted by token
	for i := 1; i < len(ring); i++ {
		if ring[i].Token < ring[i-1].Token {
			t.Errorf("ring not sorted: %d < %d at index %d", ring[i].Token, ring[i-1].Token, i)
		}
	}
}

func TestPartitioner_SetNodes_Concurrent(t *testing.T) {
	p := NewPartitioner(2)
	p.SetNodes([]string{"node1"}, nil)
	replicas1 := p.GetReplicas([]byte("foo"))
	p.SetNodes([]string{"node1", "node2"}, nil)
	replicas2 := p.GetReplicas([]byte("foo"))
	if len(replicas1) != 1 {
		t.Errorf("single node: expected 1 replica, got %d", len(replicas1))
	}
	if len(replicas2) != 2 {
		t.Errorf("two nodes: expected 2 replicas, got %d", len(replicas2))
	}
}

func TestPartitioner_GetReplicas_RackAware(t *testing.T) {
	rackMap := map[string]string{"node1": "rack1", "node2": "rack2", "node3": "rack3"}
	p := NewPartitioner(3)
	p.SetNodes([]string{"node1", "node2", "node3"}, rackMap)
	replicas := p.GetReplicas([]byte("foo"))
	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}
	// All three racks should be represented
	racks := make(map[string]bool)
	for _, nid := range replicas {
		racks[rackMap[nid]] = true
	}
	if len(racks) != 3 {
		t.Errorf("expected replicas from 3 different racks, got %v (replicas=%v)", racks, replicas)
	}
}

func TestPartitioner_GetReplicas_RackAware_RF2(t *testing.T) {
	rackMap := map[string]string{"node1": "rack1", "node2": "rack2", "node3": "rack3"}
	p := NewPartitioner(2)
	p.SetNodes([]string{"node1", "node2", "node3"}, rackMap)
	replicas := p.GetReplicas([]byte("foo"))
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(replicas))
	}
	// Should prefer different racks
	racks := make(map[string]bool)
	for _, nid := range replicas {
		racks[rackMap[nid]] = true
	}
	if len(racks) < 2 {
		t.Errorf("expected replicas from at least 2 different racks, got %v (replicas=%v)", racks, replicas)
	}
}
