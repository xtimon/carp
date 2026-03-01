package client

import (
	"bytes"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/carp/internal/partitioner"
	"github.com/carp/internal/resp"
)

// ConsistencyLevel for read/write (Cassandra-style).
type ConsistencyLevel int

const (
	ConsistencyOne         ConsistencyLevel = 1
	ConsistencyQuorum      ConsistencyLevel = 2
	ConsistencyAll         ConsistencyLevel = 3
	ConsistencyLevelDefault ConsistencyLevel = 0 // uses server default QUORUM
)

// RingEntry is a node on the consistent-hash ring.
type RingEntry struct {
	Token  int
	NodeID string
	Addr   string // "host:port" (RESP port)
}

// Client is a ring-aware Redis client that routes requests directly to the node
// that owns the key (Cassandra-style), with reconnection on node failure.
type Client struct {
	mu sync.RWMutex

	seeds     []string   // seed addresses "host:port"
	ring      []RingEntry
	nodeAddrs map[string]string // nodeID -> "host:port"
	rf        int               // replication factor for replica fallback

	consistency ConsistencyLevel // session default; 0 = server default (QUORUM)

	connectTimeout time.Duration
	retryBackoff   time.Duration
	maxRetries     int
}

// New creates a ring-aware client. Seeds are Redis (RESP) addresses, e.g. "127.0.0.1:6379".
func New(seeds []string) *Client {
	if len(seeds) == 0 {
		seeds = []string{"127.0.0.1:6379"}
	}
	c := &Client{
		seeds:          seeds,
		ring:           nil,
		nodeAddrs:      make(map[string]string),
		rf:             3,
		connectTimeout: 3 * time.Second,
		retryBackoff:   100 * time.Millisecond,
		maxRetries:     3,
	}
	_ = c.refreshRing()
	return c
}

// SetReplicationFactor sets the replication factor for replica fallback when primary is down.
func (c *Client) SetReplicationFactor(rf int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rf = rf
}

// SetConsistencyLevel sets the session consistency level for subsequent commands.
// Use ConsistencyOne, ConsistencyQuorum, ConsistencyAll, or ConsistencyLevelDefault.
func (c *Client) SetConsistencyLevel(level ConsistencyLevel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consistency = level
}

// consistencyLevelString returns the server string for the level.
func consistencyLevelString(level ConsistencyLevel) string {
	switch level {
	case ConsistencyOne:
		return "ONE"
	case ConsistencyQuorum:
		return "QUORUM"
	case ConsistencyAll:
		return "ALL"
	}
	return ""
}

// refreshRing fetches CLUSTER RING from any available node and updates the local ring.
// Tries seeds first, then known node addresses from a previous ring fetch (for failover when a node dies).
func (c *Client) refreshRing() error {
	// Try seeds
	for _, seed := range c.seeds {
		ring, addrs, err := c.fetchRingFrom(seed)
		if err != nil {
			continue
		}
		if len(ring) > 0 {
			c.mu.Lock()
			c.ring = ring
			c.nodeAddrs = addrs
			c.mu.Unlock()
			return nil
		}
	}
	// Seeds failed - try known node addresses from last ring (failover when seed is dead)
	c.mu.RLock()
	nodeAddrs := make([]string, 0, len(c.nodeAddrs))
	for _, a := range c.nodeAddrs {
		nodeAddrs = append(nodeAddrs, a)
	}
	c.mu.RUnlock()
	for _, addr := range nodeAddrs {
		ring, addrs, err := c.fetchRingFrom(addr)
		if err != nil {
			continue
		}
		if len(ring) > 0 {
			c.mu.Lock()
			c.ring = ring
			c.nodeAddrs = addrs
			c.mu.Unlock()
			return nil
		}
	}
	return fmt.Errorf("could not fetch ring from any node")
}

func (c *Client) fetchRingFrom(addr string) ([]RingEntry, map[string]string, error) {
	conn, err := net.DialTimeout("tcp", addr, c.connectTimeout)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(c.connectTimeout))

	cmd := resp.EncodeCommand("CLUSTER", []byte("RING"))
	if _, err := conn.Write(cmd); err != nil {
		return nil, nil, err
	}

	rr := resp.NewResponseReader(conn)
	raw, err := rr.ReadResponse()
	if err != nil {
		return nil, nil, err
	}

	// Parse bulk string: $len\r\n...content...\r\n
	if len(raw) < 5 || raw[0] != '$' {
		return nil, nil, fmt.Errorf("invalid CLUSTER RING response")
	}
	idx := bytes.IndexByte(raw, '\r')
	if idx < 0 {
		return nil, nil, fmt.Errorf("invalid response")
	}
	length, _ := strconv.Atoi(string(raw[1:idx]))
	if length <= 0 {
		return []RingEntry{}, make(map[string]string), nil
	}
	content := raw[idx+2 : idx+2+length]

	var ring []RingEntry
	addrs := make(map[string]string)
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) < 3 {
			continue
		}
		// Format: nodeID|addr|token or nodeID|addr|token|rack
		nodeID, addr, tokenStr := parts[0], parts[1], parts[2]
		token, err := strconv.Atoi(tokenStr)
		if err != nil {
			continue
		}
		ring = append(ring, RingEntry{Token: token, NodeID: nodeID, Addr: addr})
		addrs[nodeID] = addr
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].Token < ring[j].Token })
	return ring, addrs, nil
}

// getReplicas returns replica addresses for a key (primary first, then others).
// Traverses ring clockwise from key's token; startIdx = first ring entry with token >= keyToken.
// Stops after rf unique nodes to limit replica count.
func (c *Client) getReplicas(key []byte) []string {
	c.mu.RLock()
	ring := c.ring
	nodeAddrs := make(map[string]string)
	for k, v := range c.nodeAddrs {
		nodeAddrs[k] = v
	}
	rf := c.rf
	c.mu.RUnlock()
	if len(ring) == 0 {
		return nil
	}
	token := partitioner.TokenForKey(key)
	seen := make(map[string]bool)
	var addrs []string
	startIdx := 0
	for i, e := range ring {
		if e.Token >= token {
			startIdx = i
			break
		}
	}
	for i := 0; i < len(ring); i++ {
		idx := (startIdx + i) % len(ring)
		nodeID := ring[idx].NodeID
		if !seen[nodeID] {
			seen[nodeID] = true
			addr, ok := nodeAddrs[nodeID]
			if ok && addr != "" {
				addrs = append(addrs, addr)
			}
			if len(addrs) >= rf {
				break
			}
		}
	}
	return addrs
}

// getAnyAddr returns any known node address for key-less commands.
func (c *Client) getAnyAddr() string {
	addrs := c.getAnyAddrs()
	if len(addrs) > 0 {
		return addrs[0]
	}
	return ""
}

// getAnyAddrs returns all known node addresses for failover.
// Uses seeds first (user-specified order) so we try 6380, 6381 before retrying a dead 6379.
func (c *Client) getAnyAddrs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Prefer seeds order - user typically lists multiple for failover
	seedSet := make(map[string]bool)
	for _, s := range c.seeds {
		seedSet[s] = true
	}
	addrs := append([]string{}, c.seeds...)
	if len(c.ring) > 0 {
		for _, e := range c.ring {
			if a := c.nodeAddrs[e.NodeID]; a != "" && !seedSet[a] {
				seedSet[a] = true
				addrs = append(addrs, a)
			}
		}
	}
	return addrs
}

// commandsWithoutKey are sent to any node.
var commandsWithoutKey = map[string]bool{
	"PING": true, "HELLO": true, "QUIT": true, "ECHO": true, "TIME": true,
	"DBSIZE": true, "FLUSHDB": true, "RANDOMKEY": true, "INFO": true,
	"CONFIG": true,
}

// isClusterMetadata returns true if the command is a CLUSTER subcommand that any node can serve.
func isClusterMetadata(cmd string, args [][]byte) bool {
	if cmd != "CLUSTER" || len(args) < 1 {
		return false
	}
	sub := strings.ToUpper(string(args[0]))
	// LEAVE with node arg is sent to any node; LEAVE without arg must target the leaving node
	return sub == "INFO" || sub == "NODES" || sub == "RING" || sub == "KEYSLOT" || sub == "TOKEN" || sub == "KEYNODE" ||
		(sub == "LEAVE" && len(args) >= 2) || sub == "REPAIR" || sub == "TOMBSTONE"
}

// firstKey returns the first key argument for key-based commands.
func firstKey(cmd string, args [][]byte) []byte {
	if len(args) < 1 {
		return nil
	}
	// Commands where first arg is the key
	keyedCmds := map[string]bool{
		"GET": true, "SET": true, "SETEX": true, "SETNX": true, "GETSET": true,
		"DEL": true, "EXISTS": true, "EXPIRE": true, "TTL": true, "PERSIST": true,
		"TYPE": true, "STRLEN": true, "APPEND": true, "GETRANGE": true, "SETRANGE": true,
		"INCR": true, "INCRBY": true, "DECR": true, "DECRBY": true,
		"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true, "LLEN": true,
		"LRANGE": true, "LINDEX": true, "LSET": true, "LREM": true, "LTRIM": true,
		"SADD": true, "SREM": true, "SISMEMBER": true, "SMEMBERS": true, "SCARD": true, "SPOP": true,
		"HSET": true, "HGET": true, "HMSET": true, "HMGET": true, "HGETALL": true,
		"HDEL": true, "HEXISTS": true, "HLEN": true, "HKEYS": true, "HVALS": true,
		"ZADD": true, "ZREM": true, "ZRANGE": true, "ZRANK": true, "ZREVRANK": true,
		"ZSCORE": true, "ZCARD": true,
	}
	if keyedCmds[cmd] {
		return args[0]
	}
	return nil
}

// Do executes a Redis command. For key-based commands, routes directly to the primary node.
// Retries with replica fallback and ring refresh on connection failure.
func (c *Client) Do(cmd string, args ...[]byte) ([]byte, error) {
	cmdUpper := strings.ToUpper(cmd)

	var addrs []string
	if commandsWithoutKey[cmdUpper] || isClusterMetadata(cmdUpper, args) {
		addrs = c.getAnyAddrs()
		if len(addrs) == 0 {
			if err := c.refreshRing(); err != nil {
				return nil, err
			}
			addrs = c.getAnyAddrs()
		}
	} else if key := firstKey(cmdUpper, args); key != nil {
		addrs = c.getReplicas(key)
		if len(addrs) == 0 {
			if err := c.refreshRing(); err != nil {
				return nil, err
			}
			addrs = c.getReplicas(key)
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("no nodes available for key")
		}
	} else {
		// MGET, MSET, KEYS, etc. - use any node (server will coordinate)
		addrs = c.getAnyAddrs()
		if len(addrs) == 0 {
			if err := c.refreshRing(); err != nil {
				return nil, err
			}
			addrs = c.getAnyAddrs()
		}
	}

	// Build request; prepend CONSISTENCY when session level is set
	req, numResponses := c.buildRequest(cmd, args)
	var lastErr error
	// Failover order: try each addr → refresh ring on all fail → retry with exponential backoff
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		for _, addr := range addrs {
			raw, err := c.sendToNode(addr, req, numResponses)
			if err == nil {
				return raw, nil
			}
			lastErr = err
		}
		// All addrs failed - refresh ring and retry with updated node list
		if err := c.refreshRing(); err == nil {
			addrs = c.getAnyAddrs()
		}
		if attempt < c.maxRetries-1 {
			time.Sleep(c.retryBackoff * time.Duration(1<<uint(attempt)))
		}
	}
	// Ensure we never return (nil, nil) - use CLUSTERDOWN when no nodes reachable
	if lastErr != nil {
		return nil, fmt.Errorf("CLUSTERDOWN The cluster is down: %w", lastErr)
	}
	return nil, fmt.Errorf("CLUSTERDOWN The cluster is down")
}

// buildRequest returns encoded command(s) and how many responses to expect.
// If consistency is set, pipelines CONSISTENCY + command (2 responses).
func (c *Client) buildRequest(cmd string, args [][]byte) ([]byte, int) {
	c.mu.RLock()
	cl := c.consistency
	c.mu.RUnlock()
	if cl == ConsistencyLevelDefault || consistencyLevelString(cl) == "" {
		return resp.EncodeCommand(cmd, args...), 1
	}
	consistencyCmd := resp.EncodeCommand("CONSISTENCY", []byte(consistencyLevelString(cl)))
	mainCmd := resp.EncodeCommand(cmd, args...)
	return append(consistencyCmd, mainCmd...), 2
}

func (c *Client) sendToNode(addr string, req []byte, numResponses int) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, c.connectTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := conn.Write(req); err != nil {
		return nil, err
	}

	rr := resp.NewResponseReader(conn)
	var last []byte
	for i := 0; i < numResponses; i++ {
		raw, err := rr.ReadResponse()
		if err != nil {
			return nil, err
		}
		last = raw
	}
	return last, nil
}

// RefreshRing forces a refresh of the ring topology from the cluster.
func (c *Client) RefreshRing() error {
	return c.refreshRing()
}
