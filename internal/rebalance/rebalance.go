package rebalance

import (
	"bytes"
	"log"
	"sync"
	"time"

	"github.com/carp/internal/cluster"
	"github.com/carp/internal/partitioner"
	"github.com/carp/internal/rpc"
	"github.com/carp/internal/storage"
)

// Rebalancer migrates orphaned keys to their correct owners when topology changes
type Rebalancer struct {
	NodeID      string
	Gossip      *cluster.Gossip
	Partitioner *partitioner.Partitioner
	Storage     *storage.Storage

	interval     time.Duration
	mu           sync.Mutex
	running      bool
	stopCh       chan struct{}
	prevOrphaned map[string]bool // keys orphaned in previous round; migrate only after 2-round stability
}

// New creates a rebalancer
func New(nodeID string, gossip *cluster.Gossip, part *partitioner.Partitioner, store *storage.Storage) *Rebalancer {
	return &Rebalancer{
		NodeID:       nodeID,
		Gossip:       gossip,
		Partitioner:  part,
		Storage:      store,
		interval:     2 * time.Second,
		stopCh:       make(chan struct{}),
		prevOrphaned: make(map[string]bool),
	}
}

// SetInterval sets how often to run rebalancing
func (r *Rebalancer) SetInterval(d time.Duration) {
	r.interval = d
}

// Start begins the rebalance loop
func (r *Rebalancer) Start() {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.mu.Unlock()
	log.Printf("[rebalance] Started for node %s", r.NodeID)
	go r.loop()
}

// Stop stops the rebalance loop
func (r *Rebalancer) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
}

func (r *Rebalancer) loop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.runRound()
		}
	}
}

// runRound detects orphaned keys (we hold them but we're not a replica) and migrates.
// Two-round stabilization: a key is only migrated if it was orphaned in the previous round too,
// avoiding flapping when the ring is still converging.
func (r *Rebalancer) runRound() {
	keys, err := r.Storage.Keys([]byte("*"))
	if err != nil || len(keys) == 0 {
		r.prevOrphaned = make(map[string]bool)
		return
	}

	orphanedThisRound := make(map[string]bool)
	for _, key := range keys {
		replicas := r.Partitioner.GetReplicas(key)
		if len(replicas) == 0 {
			continue
		}

		// Check if we should still hold this key
		weAreReplica := false
		for _, nid := range replicas {
			if nid == r.NodeID {
				weAreReplica = true
				break
			}
		}
		if weAreReplica {
			continue // We're correct, keep the key
		}
		orphanedThisRound[string(key)] = true
	}

	for _, key := range keys {
		keyStr := string(key)
		if !orphanedThisRound[keyStr] {
			continue
		}
		// Stabilization: only migrate if orphaned for two consecutive rounds
		if !r.prevOrphaned[keyStr] {
			continue
		}

		replicas := r.Partitioner.GetReplicas(key)
		if len(replicas) == 0 {
			continue
		}

		// We have orphaned data - migrate to new owners
		dump, err := r.Storage.DumpKey(key)
		if err != nil || dump == nil {
			continue
		}

		// Send to all replicas (they need the data)
		acks := 0
		for _, nid := range replicas {
			host, port, ok := r.Gossip.GetNodeByID(nid)
			if !ok {
				continue
			}
			if r.sendRestore(host, port, key, dump) {
				acks++
			}
		}

		// We need at least one replica to have received it before deleting
		if acks > 0 {
			r.Storage.Delete(key)
			log.Printf("[rebalance] Migrated key %q from %s to new owners", string(key), r.NodeID)
		}
	}

	r.prevOrphaned = orphanedThisRound
}

func (r *Rebalancer) sendRestore(host string, respPort int, key, dump []byte) bool {
	res, err := rpc.SendCommand(host, respPort, rpc.CmdRestoreKey, [][]byte{key, dump})
	if err != nil {
		return false
	}
	return bytes.Equal(res, []byte("OK"))
}

// RunRepair pushes every key we have to all replicas that should hold it.
// Use after CLUSTER LEAVE FORCE <node> to restore replication factor (fix under-replicated keys).
func (r *Rebalancer) RunRepair() int {
	return r.runRepair(0)
}

// RunRepairSmooth does the same as RunRepair but processes keys vnode by vnode with a delay
// between each vnode to avoid overwhelming the cluster.
func (r *Rebalancer) RunRepairSmooth(delayBetweenVnodes time.Duration) int {
	return r.runRepair(delayBetweenVnodes)
}

// runRepair pushes every local key to all replicas that should hold it (fixes under-replication).
// Groups keys by vnode so smooth mode can process vnode-by-vnode with delays to avoid overload.
func (r *Rebalancer) runRepair(delayBetweenVnodes time.Duration) int {
	keys, err := r.Storage.Keys([]byte("*"))
	if err != nil || len(keys) == 0 {
		return 0
	}
	ring := r.Partitioner.GetRing()
	if len(ring) == 0 {
		return 0
	}
	// Group keys by primary vnode (first ring entry with token >= key's token)
	keysByVnode := make(map[int][][]byte)
	for _, key := range keys {
		tok := partitioner.TokenForKey(key)
		idx := 0
		for i, e := range ring {
			if e.Token >= tok {
				idx = i
				break
			}
		}
		keysByVnode[idx] = append(keysByVnode[idx], key)
	}
	repaired := 0
	for i := 0; i < len(ring); i++ {
		keysInVnode := keysByVnode[i]
		if len(keysInVnode) == 0 {
			continue
		}
		for _, key := range keysInVnode {
			replicas := r.Partitioner.GetReplicas(key)
			if len(replicas) == 0 {
				continue
			}
			dump, err := r.Storage.DumpKey(key)
			if err != nil || dump == nil {
				continue
			}
			for _, nid := range replicas {
				if nid == r.NodeID {
					continue
				}
				host, port, ok := r.Gossip.GetNodeByID(nid)
				if !ok {
					continue
				}
				if r.sendRestore(host, port, key, dump) {
					repaired++
				}
			}
		}
		if delayBetweenVnodes > 0 {
			time.Sleep(delayBetweenVnodes)
		}
	}
	return repaired
}
