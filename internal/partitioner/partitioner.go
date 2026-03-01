package partitioner

import (
	"fmt"
	"sort"
	"sync"
)

// DefaultNumVnodes is the number of virtual nodes per physical node for better distribution
const DefaultNumVnodes = 256

// murmur3_32 implements MurmurHash3 32-bit finalizer (Cassandra-style partitioning).
// See: https://en.wikipedia.org/wiki/MurmurHash
func murmur3_32(data []byte, seed uint32) uint32 {
	length := len(data)
	nblocks := length / 4
	h1 := seed
	c1 := uint32(0xCC9E2D51)
	c2 := uint32(0x1B873593)

	for i := 0; i < nblocks; i++ {
		k1 := uint32(data[i*4+3])<<24 | uint32(data[i*4+2])<<16 |
			uint32(data[i*4+1])<<8 | uint32(data[i*4])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19)
		h1 = h1*5 + 0xE6546B64
	}

	tail := data[nblocks*4:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(length)
	h1 ^= h1 >> 16
	h1 *= 0x85EBCA6B
	h1 ^= h1 >> 13
	h1 *= 0xC2B2AE35
	h1 ^= h1 >> 16
	return h1
}

// TokenForKey returns partition token for a key
func TokenForKey(key []byte) int {
	return int(murmur3_32(key, 0))
}

// RingEntry is (token, nodeID)
type RingEntry struct {
	Token  int
	NodeID string
}

// Partitioner manages consistent-hash ring with virtual nodes
type Partitioner struct {
	mu                sync.RWMutex
	ReplicationFactor int
	NumVnodes         int
	ring              []RingEntry
	rackMap           map[string]string // nodeID -> rack for replica diversity
}

// NewPartitioner creates a partitioner
func NewPartitioner(rf int) *Partitioner {
	return &Partitioner{ReplicationFactor: rf, NumVnodes: DefaultNumVnodes}
}

// SetNumVnodes sets the number of virtual nodes per physical node
func (p *Partitioner) SetNumVnodes(n int) {
	if n < 1 {
		n = 1
	}
	p.NumVnodes = n
}

// SetNodes updates the ring with node IDs using vnodes for better distribution.
// rackMap maps nodeID -> rack; if non-nil, GetReplicas prefers spreading replicas across racks.
func (p *Partitioner) SetNodes(nodeIDs []string, rackMap map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ring = nil
	p.rackMap = rackMap
	vnodes := p.NumVnodes
	if vnodes < 1 {
		vnodes = 1
	}
	for _, nid := range nodeIDs {
		for v := 0; v < vnodes; v++ {
			vnodeKey := []byte(fmt.Sprintf("%s:%d", nid, v))
			token := int(murmur3_32(vnodeKey, 0))
			p.ring = append(p.ring, RingEntry{Token: token, NodeID: nid})
		}
	}
	sort.Slice(p.ring, func(i, j int) bool { return p.ring[i].Token < p.ring[j].Token })
}

// GetReplicas returns replica nodes for a key (clockwise from token on the ring).
// When rackMap is set, uses three-step rack-aware selection:
// 1) primary = first node; 2) add nodes from racks not yet used; 3) fill remaining with any nodes.
func (p *Partitioner) GetReplicas(key []byte) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.ring) == 0 {
		return nil
	}
	token := TokenForKey(key)
	startIdx := 0
	for i, e := range p.ring {
		if e.Token >= token {
			startIdx = i
			break
		}
	}

	// Collect unique node IDs in clockwise ring order
	var candidates []string
	seen := make(map[string]bool)
	for i := 0; i < len(p.ring); i++ {
		idx := (startIdx + i) % len(p.ring)
		nid := p.ring[idx].NodeID
		if !seen[nid] {
			seen[nid] = true
			candidates = append(candidates, nid)
		}
	}

	if p.rackMap == nil || len(p.rackMap) == 0 {
		// No rack awareness: take first RF unique nodes in ring order
		rf := p.ReplicationFactor
		if rf > len(candidates) {
			rf = len(candidates)
		}
		return append([]string{}, candidates[:rf]...)
	}

	// Rack-aware selection: spread replicas across racks for fault tolerance
	replicas := make([]string, 0, p.ReplicationFactor)
	racksUsed := make(map[string]bool)
	// Step 1: add primary (first node in ring order)
	replicas = append(replicas, candidates[0])
	racksUsed[p.rackMap[candidates[0]]] = true

	// Step 2: add nodes from racks we don't have yet (maximize rack diversity)
	for _, nid := range candidates[1:] {
		if len(replicas) >= p.ReplicationFactor {
			break
		}
		rack := p.rackMap[nid]
		if !racksUsed[rack] {
			racksUsed[rack] = true
			replicas = append(replicas, nid)
		}
	}
	// Step 3: fill remaining slots with any nodes (same rack ok when RF > num racks)
	for _, nid := range candidates[1:] {
		if len(replicas) >= p.ReplicationFactor {
			break
		}
		have := false
		for _, r := range replicas {
			if r == nid {
				have = true
				break
			}
		}
		if !have {
			replicas = append(replicas, nid)
		}
	}
	return replicas
}

// GetRing returns ring topology
func (p *Partitioner) GetRing() []RingEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]RingEntry{}, p.ring...)
}

