package cluster

import (
	"testing"
)

func TestMergeState_NewNode(t *testing.T) {
	g := NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	g.recordContact("node1")

	remote := []map[string]interface{}{
		{
			"node_id":     "node2",
			"host":        "127.0.0.1",
			"port":        float64(6380),
			"gossip_port": float64(7001),
			"state":       "UP",
			"generation":  float64(100),
			"heartbeat":   float64(5),
		},
	}
	g.MergeState(remote)

	nodes := g.GetAllNodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after merge, got %d", len(nodes))
	}
	found := false
	for _, n := range nodes {
		if n.NodeID == "node2" {
			found = true
			if n.Host != "127.0.0.1" || n.Port != 6380 {
				t.Errorf("node2 wrong host/port: %s:%d", n.Host, n.Port)
			}
			break
		}
	}
	if !found {
		t.Error("node2 not found after merge")
	}
}

func TestMergeState_NewerWins(t *testing.T) {
	g := NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")

	// Add node2 with gen=1, hb=1
	g.MergeState([]map[string]interface{}{
		{"node_id": "node2", "host": "127.0.0.1", "port": float64(6380), "gossip_port": float64(7001),
			"state": "UP", "generation": float64(1), "heartbeat": float64(1)},
	})
	n := g.nodes["node2"]
	if n == nil || n.Generation != 1 || n.Heartbeat != 1 {
		t.Fatalf("initial merge failed: %+v", n)
	}

	// Older (gen=0, hb=0) should not overwrite
	g.MergeState([]map[string]interface{}{
		{"node_id": "node2", "host": "old", "port": float64(9999), "gossip_port": float64(7001),
			"state": "UP", "generation": float64(0), "heartbeat": float64(0)},
	})
	n = g.nodes["node2"]
	if n.Host == "old" || n.Port == 9999 {
		t.Error("older state should not overwrite")
	}

	// Newer (gen=2) should overwrite
	g.MergeState([]map[string]interface{}{
		{"node_id": "node2", "host": "new", "port": float64(6380), "gossip_port": float64(7001),
			"state": "UP", "generation": float64(2), "heartbeat": float64(1)},
	})
	n = g.nodes["node2"]
	if n.Host != "new" || n.Generation != 2 {
		t.Errorf("newer state should overwrite: %+v", n)
	}
}

func TestMergeState_LeavingRemoves(t *testing.T) {
	g := NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	g.MergeState([]map[string]interface{}{
		{"node_id": "node2", "host": "127.0.0.1", "port": float64(6380), "gossip_port": float64(7001),
			"state": "UP", "generation": float64(1), "heartbeat": float64(1)},
	})
	if len(g.nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(g.nodes))
	}

	g.MergeState([]map[string]interface{}{
		{"node_id": "node2", "state": "LEAVING"},
	})
	if _, ok := g.nodes["node2"]; ok {
		t.Error("LEAVING should remove node from cluster")
	}
}

func TestSerializeState(t *testing.T) {
	g := NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	g.SetNumVnodes(64)
	state := g.SerializeState()
	if len(state) != 1 {
		t.Fatalf("expected 1 node in state, got %d", len(state))
	}
	if state[0]["node_id"] != "node1" {
		t.Errorf("node_id = %v", state[0]["node_id"])
	}
	if state[0]["num_vnodes"].(int) != 64 {
		t.Errorf("num_vnodes = %v", state[0]["num_vnodes"])
	}
}

func TestGetRingNodes_ExcludesUnreachable(t *testing.T) {
	g := NewGossip("node1", "127.0.0.1", 6379, 7000, "test", "")
	g.recordContact("node1")
	g.MergeState([]map[string]interface{}{
		{"node_id": "node2", "host": "127.0.0.1", "port": float64(6380), "gossip_port": float64(7001),
			"state": "UP", "generation": float64(1), "heartbeat": float64(1)},
	})
	// node2 not contacted - lastContact doesn't have it (or has zero value)
	// isUp checks lastContact and downThreshold. We never called recordContact("node2")
	// so lastContact["node2"] doesn't exist - isUp returns false
	ring := g.GetRingNodes()
	if len(ring) != 1 {
		t.Errorf("unreachable node2 should be excluded: got %v", ring)
	}
	g.recordContact("node2")
	ring = g.GetRingNodes()
	if len(ring) != 2 {
		t.Errorf("after recordContact node2 should be in ring: got %v", ring)
	}
}

func TestClusterNameMatches(t *testing.T) {
	if !clusterNameMatches("carp", "carp") {
		t.Error("same name should match")
	}
	if !clusterNameMatches("", "carp") {
		t.Error("empty remote should default to carp and match")
	}
	if clusterNameMatches("carp", "other") {
		t.Error("different names should not match")
	}
}
