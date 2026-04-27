package partitioner

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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

// ringSnapshot is the immutable, lookup-optimized form of the ring.
// SetNodes publishes a new snapshot atomically; GetReplicas reads it lock-free.
// The replicas field caches the rack-aware replica list for each ring entry
// (replicas[i] is the result for any token that lands on entry i), so the hot
// path is a binary search + slice return — no allocations, no map ops.
//
// Slices in `replicas` are shared with all callers; treat them as immutable.
type ringSnapshot struct {
	ring     []RingEntry
	replicas [][]string
}

// Partitioner manages consistent-hash ring with virtual nodes.
//
// Writes (SetNodes) take `mu` to serialize snapshot construction.
// Reads (GetReplicas) use the atomic snap pointer — no lock, no allocation.
type Partitioner struct {
	mu                sync.Mutex
	ReplicationFactor int
	NumVnodes         int
	rackMap           map[string]string // nodeID -> rack for replica diversity (under mu)
	snap              atomic.Pointer[ringSnapshot]
}

// NewPartitioner creates a partitioner
func NewPartitioner(rf int) *Partitioner {
	p := &Partitioner{ReplicationFactor: rf, NumVnodes: DefaultNumVnodes}
	p.snap.Store(&ringSnapshot{})
	return p
}

// SetNumVnodes sets the number of virtual nodes per physical node
func (p *Partitioner) SetNumVnodes(n int) {
	if n < 1 {
		n = 1
	}
	p.NumVnodes = n
}

// SetNodes updates the ring with node IDs using vnodes for better distribution.
// rackMap maps nodeID -> rack; if non-nil, replica selection prefers spreading
// replicas across racks. Builds a fresh immutable snapshot — concurrent
// GetReplicas readers see the old snapshot until the atomic store completes.
func (p *Partitioner) SetNodes(nodeIDs []string, rackMap map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rackMap = rackMap
	vnodes := p.NumVnodes
	if vnodes < 1 {
		vnodes = 1
	}
	ring := make([]RingEntry, 0, len(nodeIDs)*vnodes)
	for _, nid := range nodeIDs {
		for v := 0; v < vnodes; v++ {
			vnodeKey := []byte(fmt.Sprintf("%s:%d", nid, v))
			token := int(murmur3_32(vnodeKey, 0))
			ring = append(ring, RingEntry{Token: token, NodeID: nid})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].Token < ring[j].Token })

	replicas := make([][]string, len(ring))
	for i := range ring {
		replicas[i] = computeReplicas(ring, i, p.ReplicationFactor, rackMap)
	}
	p.snap.Store(&ringSnapshot{ring: ring, replicas: replicas})
}

// computeReplicas runs the rack-aware selection for a startIdx into the ring.
// Three-step: 1) primary = ring[startIdx]; 2) add nodes from racks not yet
// used (rack diversity); 3) fill remaining slots with any unused nodes.
// When rackMap is empty, just returns the first RF unique nodes in ring order.
func computeReplicas(ring []RingEntry, startIdx int, rf int, rackMap map[string]string) []string {
	if len(ring) == 0 || rf <= 0 {
		return nil
	}
	// Collect unique node IDs clockwise from startIdx
	candidates := make([]string, 0, rf*2)
	seen := make(map[string]struct{}, rf*2)
	for i := 0; i < len(ring); i++ {
		nid := ring[(startIdx+i)%len(ring)].NodeID
		if _, ok := seen[nid]; !ok {
			seen[nid] = struct{}{}
			candidates = append(candidates, nid)
		}
	}

	if len(rackMap) == 0 {
		if rf > len(candidates) {
			rf = len(candidates)
		}
		out := make([]string, rf)
		copy(out, candidates[:rf])
		return out
	}

	replicas := make([]string, 0, rf)
	racksUsed := make(map[string]struct{}, rf)
	replicas = append(replicas, candidates[0])
	racksUsed[rackMap[candidates[0]]] = struct{}{}

	for _, nid := range candidates[1:] {
		if len(replicas) >= rf {
			break
		}
		rack := rackMap[nid]
		if _, used := racksUsed[rack]; !used {
			racksUsed[rack] = struct{}{}
			replicas = append(replicas, nid)
		}
	}
	for _, nid := range candidates[1:] {
		if len(replicas) >= rf {
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

// GetReplicas returns replica nodes for a key, clockwise from the key's token
// on the ring with rack-aware selection. The returned slice is shared with
// other callers and the cached snapshot — callers MUST NOT mutate it.
func (p *Partitioner) GetReplicas(key []byte) []string {
	s := p.snap.Load()
	if s == nil || len(s.ring) == 0 {
		return nil
	}
	token := TokenForKey(key)
	idx := sort.Search(len(s.ring), func(i int) bool { return s.ring[i].Token >= token })
	if idx == len(s.ring) {
		idx = 0 // wrap clockwise past the highest token
	}
	return s.replicas[idx]
}

// GetRing returns a copy of the ring topology (safe to inspect/mutate).
func (p *Partitioner) GetRing() []RingEntry {
	s := p.snap.Load()
	if s == nil {
		return nil
	}
	return append([]RingEntry{}, s.ring...)
}

