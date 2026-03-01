package cluster

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

// NodeState is the EndpointState for a node (Cassandra-inspired).
// HeartBeatState: Generation (startup timestamp) + Version (monotonic).
// Application state: host, port, gossip_port, state, num_vnodes, rack.
type NodeState struct {
	NodeID     string `json:"node_id"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	GossipPort int    `json:"gossip_port"`
	State      string `json:"state"`
	Generation int64  `json:"generation"` // startup timestamp; distinguishes restarts
	Heartbeat  int64  `json:"heartbeat"`  // monotonic version; higher = newer
	NumVnodes  int    `json:"num_vnodes"`
	Rack       string `json:"rack"` // logical grouping for fault tolerance (replicas spread across racks)
}

// Gossip implements Cassandra-style membership protocol.
//
// Design (from Apache Cassandra):
// - EndpointState per node: HeartBeatState (generation, version) + application fields
// - Generation: startup timestamp; version: monotonic. Merge: (gen, ver) newer wins.
// - Failure detection: last contact time; node DOWN if no successful gossip > threshold
// - Gossip targets per round: random live, random unreachable (prob), random seed (prob)
// - Nodes never deleted (except LEAVING/LEFT); DOWN nodes excluded from ring
// - Partitioner updated only when ring stable for 2 rounds
type Gossip struct {
	NodeID      string
	Host        string
	Port        int
	GossipPort  int
	ClusterName string
	NumVnodes   int

	mu    sync.RWMutex
	nodes map[string]*NodeState
	seeds []string

	// Failure detection: last time we successfully exchanged with this node (inbound or outbound)
	lastContact   map[string]time.Time
	downThreshold time.Duration // no contact for this long = DOWN (default 15s)

	// Ring stability: only notify when unchanged for 2 rounds
	prevRingNodes map[string]bool
	stableRounds  int
	onRingChange  func(nodeIDs []string)

	version int64 // monotonic; bumped each round for self
	running bool
	stopCh  chan struct{}
}

// NewGossip creates a Gossip instance. Generation = startup timestamp.
// rack is a logical grouping of nodes for fault tolerance (e.g., datacenter rack, availability zone).
func NewGossip(nodeID, host string, port, gossipPort int, clusterName string, rack string) *Gossip {
	if clusterName == "" {
		clusterName = "carp"
	}
	gen := time.Now().Unix()
	g := &Gossip{
		NodeID:        nodeID,
		Host:          host,
		Port:          port,
		GossipPort:    gossipPort,
		ClusterName:   clusterName,
		nodes:         make(map[string]*NodeState),
		seeds:         nil,
		lastContact:   make(map[string]time.Time),
		downThreshold: 15 * time.Second,
		prevRingNodes: make(map[string]bool),
		stopCh:        make(chan struct{}),
		version:       1,
	}
	g.nodes[nodeID] = &NodeState{
		NodeID: nodeID, Host: host, Port: port, GossipPort: gossipPort,
		State: "UP", Generation: gen, Heartbeat: 1, NumVnodes: 0, Rack: rack,
	}
	g.lastContact[nodeID] = time.Now()
	return g
}

// SetNumVnodes sets vnodes per node (gossiped for ring consistency)
func (g *Gossip) SetNumVnodes(n int) {
	g.mu.Lock()
	g.NumVnodes = n
	if g.nodes[g.NodeID] != nil {
		g.nodes[g.NodeID].NumVnodes = n
	}
	g.mu.Unlock()
}

// SetOnRingChange sets callback when ring nodes stabilize
func (g *Gossip) SetOnRingChange(fn func(nodeIDs []string)) {
	g.mu.Lock()
	g.onRingChange = fn
	g.mu.Unlock()
}

// AddSeed adds a seed endpoint
func (g *Gossip) AddSeed(host string, gossipPort int) {
	g.mu.Lock()
	g.seeds = append(g.seeds, net.JoinHostPort(host, strconv.Itoa(gossipPort)))
	g.mu.Unlock()
}

// GetAllNodes returns all known nodes (including DOWN; for discovery)
func (g *Gossip) GetAllNodes() []*NodeState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*NodeState, 0, len(g.nodes))
	for _, n := range g.nodes {
		out = append(out, n)
	}
	return out
}

// isUp returns true if node was contacted within downThreshold
func (g *Gossip) isUp(nid string) bool {
	if nid == g.NodeID {
		return true
	}
	t, ok := g.lastContact[nid]
	if !ok {
		return false
	}
	return time.Since(t) < g.downThreshold
}

// GetRingNodeStates returns NodeStates for UP nodes
func (g *Gossip) GetRingNodeStates() []*NodeState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var out []*NodeState
	for nid, n := range g.nodes {
		if n.State == "LEAVING" || n.State == "LEFT" {
			continue
		}
		if !g.isUp(nid) {
			continue
		}
		out = append(out, n)
	}
	return out
}

// GetRingNodes returns node IDs for the ring (UP nodes)
func (g *Gossip) GetRingNodes() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var out []string
	for nid := range g.nodes {
		if n := g.nodes[nid]; n.State == "LEAVING" || n.State == "LEFT" {
			continue
		}
		if !g.isUp(nid) {
			continue
		}
		out = append(out, nid)
	}
	sort.Strings(out)
	return out
}

// GetNodeByID returns (host, port) for UP nodes only
func (g *Gossip) GetNodeByID(nodeID string) (string, int, bool) {
	g.mu.RLock()
	n := g.nodes[nodeID]
	if n == nil || n.State == "LEAVING" || n.State == "LEFT" || !g.isUp(nodeID) {
		g.mu.RUnlock()
		return "", 0, false
	}
	host, port := n.Host, n.Port
	g.mu.RUnlock()
	return host, port, true
}

// MergeState merges remote endpoint states. (generation, heartbeat) newer wins.
// joiningBlocked: no new nodes adopted if any node is LEAVING (only one leave at a time).
// adoptedOne: at most one new node adopted per call to avoid thundering herd during scale-up.
func (g *Gossip) MergeState(remote []map[string]interface{}) {
	g.mu.Lock()
	before := make(map[string]bool)
	for k := range g.nodes {
		before[k] = true
	}
	joiningBlocked := g.hasLeavingNodeLocked()
	adoptedOne := false
	for _, rn := range remote {
		nid, _ := rn["node_id"].(string)
		if nid == "" || nid == g.NodeID {
			continue
		}
		state, _ := rn["state"].(string)
		if state == "LEAVING" || state == "LEFT" {
			if _, had := g.nodes[nid]; had {
				delete(g.nodes, nid)
				delete(g.lastContact, nid)
				log.Printf("[cluster] Node %s left the cluster", nid)
			}
			continue
		}
		host, _ := rn["host"].(string)
		port := 0
		if p, ok := rn["port"].(float64); ok {
			port = int(p)
		}
		if state == "" {
			state = "UP"
		}
		gen := int64(0)
		if ge, ok := rn["generation"].(float64); ok {
			gen = int64(ge)
		}
		hb := int64(0)
		if h, ok := rn["heartbeat"].(float64); ok {
			hb = int64(h)
		}
		gossipPort := 0
		if gp, ok := rn["gossip_port"].(float64); ok {
			gossipPort = int(gp)
		}
		numVnodes := g.NumVnodes
		if nv, ok := rn["num_vnodes"].(float64); ok && int(nv) > 0 {
			numVnodes = int(nv)
		}
		rack, _ := rn["rack"].(string)
		existing := g.nodes[nid]
		adopt := false
		if existing == nil {
			// New node: only adopt if no leave in progress and we haven't adopted one this round
			if joiningBlocked || adoptedOne {
				continue
			}
			adopt = true
			adoptedOne = true
		} else if gen > existing.Generation {
			adopt = true
		} else if gen == existing.Generation && hb >= existing.Heartbeat {
			adopt = true
		}
		if adopt {
			g.nodes[nid] = &NodeState{
				NodeID: nid, Host: host, Port: port, GossipPort: gossipPort,
				State: state, Generation: gen, Heartbeat: hb, NumVnodes: numVnodes, Rack: rack,
			}
		}
	}
	discovered := []string{}
	for k := range g.nodes {
		if !before[k] {
			discovered = append(discovered, k)
		}
	}
	if len(discovered) > 0 {
		sort.Strings(discovered)
		after := make([]string, 0, len(g.nodes))
		for k := range g.nodes {
			after = append(after, k)
		}
		sort.Strings(after)
		log.Printf("[cluster] Discovered %v → cluster: %v", discovered, after)
	}
	g.mu.Unlock()
}

// SerializeState returns endpoint states for gossip exchange
func (g *Gossip) SerializeState() []map[string]interface{} {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]map[string]interface{}, 0, len(g.nodes))
	for _, n := range g.nodes {
		nv := n.NumVnodes
		if nv <= 0 {
			nv = g.NumVnodes
		}
		out = append(out, map[string]interface{}{
			"node_id":     n.NodeID,
			"host":        n.Host,
			"port":        n.Port,
			"gossip_port": n.GossipPort,
			"state":       n.State,
			"generation":  n.Generation,
			"heartbeat":   n.Heartbeat,
			"num_vnodes":  nv,
			"rack":        n.Rack,
		})
	}
	return out
}

// Start begins the gossip loop (runs every second, Cassandra default)
func (g *Gossip) Start() {
	g.mu.Lock()
	if g.running {
		g.mu.Unlock()
		return
	}
	g.running = true
	g.mu.Unlock()
	log.Printf("[cluster] Gossip started for node %s (generation=%d, vnodes=%d)", g.NodeID, g.nodes[g.NodeID].Generation, g.NumVnodes)
	go g.gossipLoop()
}

// HasLeavingNode returns true if any node (including self) has State LEAVING
func (g *Gossip) HasLeavingNode() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, n := range g.nodes {
		if n != nil && n.State == "LEAVING" {
			return true
		}
	}
	return false
}

// CanLeave returns true if no leave is in progress (only one node at a time)
func (g *Gossip) CanLeave() bool {
	return !g.HasLeavingNode()
}

// SetLeaving marks this node as leaving. Returns false if another node is already leaving.
func (g *Gossip) SetLeaving() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, n := range g.nodes {
		if n != nil && n.State == "LEAVING" {
			return false // another node already leaving
		}
	}
	if n := g.nodes[g.NodeID]; n != nil {
		n.State = "LEAVING"
		n.Heartbeat = g.version
		g.version++
		return true
	}
	return false
}

// BroadcastLeave runs gossip rounds to propagate LEAVING
func (g *Gossip) BroadcastLeave(rounds int) {
	for i := 0; i < rounds; i++ {
		g.gossipRound()
		if i < rounds-1 {
			time.Sleep(300 * time.Millisecond)
		}
	}
}

// RemoveDeadNode broadcasts LEFT and removes from all peers. Returns error if another node is already leaving.
func (g *Gossip) RemoveDeadNode(nodeID string) error {
	if nodeID == "" || nodeID == g.NodeID {
		return nil
	}
	g.mu.Lock()
	if g.hasLeavingNodeLocked() {
		g.mu.Unlock()
		return fmt.Errorf("another node is already leaving the ring; wait for it to complete")
	}
	g.nodes[nodeID] = &NodeState{NodeID: nodeID, State: "LEFT"}
	g.mu.Unlock()
	g.BroadcastLeave(3)
	g.mu.Lock()
	delete(g.nodes, nodeID)
	delete(g.lastContact, nodeID)
	g.mu.Unlock()
	log.Printf("[cluster] Removed dead node %s from cluster", nodeID)
	return nil
}

func (g *Gossip) hasLeavingNodeLocked() bool {
	for _, n := range g.nodes {
		if n != nil && n.State == "LEAVING" {
			return true
		}
	}
	return false
}

// Stop stops gossip
func (g *Gossip) Stop() {
	g.mu.Lock()
	if !g.running {
		g.mu.Unlock()
		return
	}
	g.running = false
	g.mu.Unlock()
	select {
	case <-g.stopCh:
	default:
		close(g.stopCh)
	}
}

func (g *Gossip) gossipLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.bumpHeartbeat()
			g.gossipRound()
		}
	}
}

func (g *Gossip) bumpHeartbeat() {
	g.mu.Lock()
	g.version++
	if n := g.nodes[g.NodeID]; n != nil {
		n.Heartbeat = g.version
	}
	g.mu.Unlock()
}

// selectGossipTargets returns 1–3 targets per round (Cassandra-style):
// 1) one random live peer (always if any)
// 2) one random unreachable peer with 0.1 probability (to re-establish)
// 3) one random seed with 0.05 probability, or always if few live (bootstrap/partition healing)
func (g *Gossip) selectGossipTargets() []gossipTarget {
	g.mu.RLock()
	myAddr := net.JoinHostPort(g.Host, strconv.Itoa(g.GossipPort))
	seeds := append([]string{}, g.seeds...)
	liveAddrs := []string{}
	unreachAddrs := []string{}
	addrToNodeID := make(map[string]string)
	for nid, n := range g.nodes {
		if nid == g.NodeID || n.State == "LEAVING" || n.State == "LEFT" {
			continue
		}
		if n.GossipPort <= 0 {
			continue
		}
		addr := net.JoinHostPort(n.Host, strconv.Itoa(n.GossipPort))
		addrToNodeID[addr] = nid
		if g.isUp(nid) {
			liveAddrs = append(liveAddrs, addr)
		} else {
			unreachAddrs = append(unreachAddrs, addr)
		}
	}
	g.mu.RUnlock()

	type target struct {
		addr   string
		nodeID string
	}
	var targets []target
	seen := make(map[string]bool)

	// Filter seeds (exclude self)
	seedAddrs := []string{}
	for _, s := range seeds {
		if s != myAddr && !seen[s] {
			seedAddrs = append(seedAddrs, s)
			seen[s] = true
		}
	}

	// 1. Random live peer (if any)
	if len(liveAddrs) > 0 {
		a := liveAddrs[rand.Intn(len(liveAddrs))]
		targets = append(targets, target{a, addrToNodeID[a]})
	}
	// 2. Random unreachable with probability (to re-establish)
	if len(unreachAddrs) > 0 && rand.Float64() < 0.1 {
		a := unreachAddrs[rand.Intn(len(unreachAddrs))]
		targets = append(targets, target{a, addrToNodeID[a]})
	}
	// 3. Random seed with probability (bootstrap / partition healing)
	if len(seedAddrs) > 0 && (len(liveAddrs) < len(seedAddrs) || rand.Float64() < 0.05) {
		a := seedAddrs[rand.Intn(len(seedAddrs))]
		targets = append(targets, target{a, addrToNodeID[a]})
	}

	out := make([]gossipTarget, 0, len(targets))
	seenAddr := make(map[string]bool)
	for _, t := range targets {
		if seenAddr[t.addr] {
			continue
		}
		seenAddr[t.addr] = true
		out = append(out, gossipTarget{addr: t.addr, nodeID: t.nodeID})
	}
	return out
}

type gossipTarget struct {
	addr   string
	nodeID string
}

func (g *Gossip) gossipRound() {
	targets := g.selectGossipTargets()
	if len(targets) == 0 {
		g.mu.RLock()
		hasSeeds := len(g.seeds) > 0
		g.mu.RUnlock()
		if hasSeeds {
			log.Printf("[cluster] No gossip targets - %s may be partitioned", g.NodeID)
		}
		return
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"from":         g.NodeID,
		"cluster_name": g.ClusterName,
		"nodes":        g.SerializeState(),
	})
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	for _, t := range targets {
		conn, err := net.DialTimeout("tcp", t.addr, 2*time.Second)
		if err != nil {
			log.Printf("[cluster] Gossip to %s failed: %v", t.addr, err)
			continue
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		if _, err := conn.Write(lenBuf); err != nil {
			conn.Close()
			continue
		}
		if _, err := conn.Write(payload); err != nil {
			conn.Close()
			continue
		}
		if _, err := conn.Read(lenBuf); err != nil {
			conn.Close()
			continue
		}
		respLen := binary.BigEndian.Uint32(lenBuf)
		respBuf := make([]byte, respLen)
		if _, err := conn.Read(respBuf); err != nil {
			conn.Close()
			continue
		}
		conn.Close()

		var data struct {
			From        string                   `json:"from"`
			ClusterName string                   `json:"cluster_name"`
			Nodes       []map[string]interface{} `json:"nodes"`
		}
		if json.Unmarshal(respBuf, &data) == nil && clusterNameMatches(data.ClusterName, g.ClusterName) {
			g.recordContact(data.From) // server's NodeID (we know who we exchanged with)
			g.MergeState(data.Nodes)
		}
	}

	g.checkRingStability()
}

func (g *Gossip) recordContact(nodeID string) {
	if nodeID == "" {
		return
	}
	g.mu.Lock()
	g.lastContact[nodeID] = time.Now()
	g.mu.Unlock()
}

// RecordContact marks nodeID as recently contacted (for tests and simulations)
func (g *Gossip) RecordContact(nodeID string) {
	g.recordContact(nodeID)
}

// checkRingStability runs after each gossip round. Only invokes onRingChange when the ring
// has been unchanged for 2 consecutive rounds, to avoid partitioner flapping during joins/leaves.
func (g *Gossip) checkRingStability() {
	ringNodes := g.GetRingNodes()
	currSet := make(map[string]bool)
	for _, nid := range ringNodes {
		currSet[nid] = true
	}
	g.mu.Lock()
	changed := len(currSet) != len(g.prevRingNodes)
	if !changed {
		for nid := range currSet {
			if !g.prevRingNodes[nid] {
				changed = true
				break
			}
		}
	}
	if changed {
		g.prevRingNodes = currSet
		g.stableRounds = 0
	} else {
		g.stableRounds++
		if g.stableRounds >= 2 && g.onRingChange != nil {
			nodeIDs := make([]string, 0, len(ringNodes))
			for _, nid := range ringNodes {
				nodeIDs = append(nodeIDs, nid)
			}
			sort.Strings(nodeIDs)
			fn := g.onRingChange
			g.mu.Unlock()
			fn(nodeIDs)
			g.mu.Lock()
		}
	}
	g.mu.Unlock()
}

func clusterNameMatches(remote, local string) bool {
	if remote == "" {
		remote = "carp"
	}
	if local == "" {
		local = "carp"
	}
	return remote == local
}

// GossipServer accepts incoming gossip connections
type GossipServer struct {
	gossip *Gossip
	ln     net.Listener
}

// NewGossipServer creates a gossip server
func NewGossipServer(g *Gossip) *GossipServer {
	return &GossipServer{gossip: g}
}

// Start listens for gossip
func (s *GossipServer) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.gossip.Host, s.gossip.GossipPort))
	if err != nil {
		return err
	}
	s.ln = ln
	log.Printf("[cluster] Gossip server on %s:%d", s.gossip.Host, s.gossip.GossipPort)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handleConn(conn)
		}
	}()
	return nil
}

func (s *GossipServer) handleConn(conn net.Conn) {
	defer conn.Close()
	lenBuf := make([]byte, 4)
	if _, err := conn.Read(lenBuf); err != nil {
		return
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)
	msg := make([]byte, msgLen)
	if _, err := conn.Read(msg); err != nil {
		return
	}
	var data struct {
		From        string                   `json:"from"`
		ClusterName string                   `json:"cluster_name"`
		Nodes       []map[string]interface{} `json:"nodes"`
	}
	if json.Unmarshal(msg, &data) != nil {
		return
	}
	// Record that "from" node contacted us (they're reachable)
	if data.From != "" {
		s.gossip.recordContact(data.From)
	}
	if clusterNameMatches(data.ClusterName, s.gossip.ClusterName) {
		s.gossip.MergeState(data.Nodes)
	}
	reply, _ := json.Marshal(map[string]interface{}{
		"from":         s.gossip.NodeID,
		"cluster_name": s.gossip.ClusterName,
		"nodes":        s.gossip.SerializeState(),
	})
	binary.BigEndian.PutUint32(lenBuf, uint32(len(reply)))
	conn.Write(lenBuf)
	conn.Write(reply)
}

// Close stops the server
func (s *GossipServer) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}
