package coordinator

import (
	"bytes"
	"encoding/binary"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/carp/internal/cluster"
	"github.com/carp/internal/partitioner"
	"github.com/carp/internal/resp"
	"github.com/carp/internal/rpc"
	"github.com/carp/internal/storage"
)

// ConsistencyLevel for read/write (Cassandra-style)
type ConsistencyLevel int

const (
	ConsistencyOne    ConsistencyLevel = 1
	ConsistencyQuorum ConsistencyLevel = 2 // default, quorum of replicas
	ConsistencyAll    ConsistencyLevel = 3
)

// DefaultConsistencyLevel is QUORUM when not specified
const DefaultConsistencyLevel = ConsistencyQuorum

// Coordinator routes Redis commands to replicas
type Coordinator struct {
	NodeID            string
	Gossip            *cluster.Gossip
	Partitioner       *partitioner.Partitioner
	Storage           *storage.Storage
	ReplicationFactor int
	ConsistencyLevel  ConsistencyLevel
	ClusterName       string
	ShutdownCh        chan<- struct{}         // when set, CLUSTER LEAVE signals here to trigger server shutdown
	RepairFunc        func() int              // run local repair; used by CLUSTER REPAIR
	RepairFuncSmooth  func(time.Duration) int // run smooth repair vnode by vnode
	SavePath          string                  // path for SAVE/BGSAVE (empty = disabled)
	bgsaveMu          sync.Mutex
	bgsaveInProgress  bool
}

// SetShutdownCh sets the channel to signal when CLUSTER LEAVE is executed
func (c *Coordinator) SetShutdownCh(ch chan<- struct{}) {
	c.ShutdownCh = ch
}

// SetRepairFunc sets the function to run local repair (CLUSTER REPAIR)
func (c *Coordinator) SetRepairFunc(fn func() int) {
	c.RepairFunc = fn
}

// SetRepairFuncSmooth sets the function to run smooth repair (CLUSTER REPAIR SMOOTH)
func (c *Coordinator) SetRepairFuncSmooth(fn func(time.Duration) int) {
	c.RepairFuncSmooth = fn
}

// SetSavePath sets the path for SAVE/BGSAVE (dump.rdb). Empty disables persistence.
func (c *Coordinator) SetSavePath(path string) {
	c.SavePath = path
}

// New creates a coordinator
func New(nodeID string, g *cluster.Gossip, p *partitioner.Partitioner, s *storage.Storage, rf int, clusterName string) *Coordinator {
	if clusterName == "" {
		clusterName = "carp"
	}
	return &Coordinator{
		NodeID:            nodeID,
		Gossip:            g,
		Partitioner:       p,
		Storage:           s,
		ReplicationFactor: rf,
		ConsistencyLevel:  DefaultConsistencyLevel,
		ClusterName:       clusterName,
	}
}

// requiredReplicasFor returns min number of acks for the given consistency level.
// If override is nil, uses the coordinator's default.
func (c *Coordinator) requiredReplicasFor(override *ConsistencyLevel) int {
	cl := c.ConsistencyLevel
	if override != nil {
		cl = *override
	}
	if cl == 0 {
		cl = DefaultConsistencyLevel
	}
	rf := c.ReplicationFactor
	if rf < 1 {
		rf = 1
	}
	switch cl {
	case ConsistencyOne:
		return 1
	case ConsistencyQuorum:
		return (rf / 2) + 1
	case ConsistencyAll:
		return rf
	default:
		return (rf / 2) + 1
	}
}

// ParseConsistencyLevel parses "ONE", "QUORUM", "ALL" from command args.
func ParseConsistencyLevel(arg []byte) (ConsistencyLevel, bool) {
	s := strings.ToUpper(string(arg))
	switch s {
	case "ONE":
		return ConsistencyOne, true
	case "QUORUM":
		return ConsistencyQuorum, true
	case "ALL":
		return ConsistencyAll, true
	}
	return 0, false
}

// replica maps a partitioner node ID to host/port for RPC; used to route commands to the correct replica.
type replica struct {
	nodeID string
	host   string
	port   int
}

func (c *Coordinator) getReplicas(key []byte) []replica {
	nids := c.Partitioner.GetReplicas(key)
	var out []replica
	for _, nid := range nids {
		host, port, ok := c.Gossip.GetNodeByID(nid)
		if ok {
			out = append(out, replica{nid, host, port})
		}
	}
	return out
}

// rpcWithRetry sends an RPC and retries once on failure (handles stale pooled conns, transient errors)
func (c *Coordinator) rpcWithRetry(r replica, cmd byte, args [][]byte) ([]byte, bool) {
	b, err := rpc.SendCommand(r.host, r.port, cmd, args)
	if err == nil {
		return b, true
	}
	b, err = rpc.SendCommand(r.host, r.port, cmd, args)
	return b, err == nil
}

// Execute handles a Redis command and returns RESP response.
//
// Flow: key-less commands (PING, etc) → single-key commands → extended (sets, hashes, zsets).
// Consistency levels (ONE, QUORUM, ALL) control how many replicas must acknowledge reads/writes.
// consistencyOverride is per-connection override (e.g. from CONSISTENCY command); nil uses default.
func (c *Coordinator) Execute(cmdName []byte, args [][]byte, consistencyOverride *ConsistencyLevel) []byte {
	cmd := strings.ToUpper(string(cmdName))

	// Key-less commands: no partition lookup
	switch cmd {
	case "PING":
		return resp.EncodeSimpleString("PONG")
	case "HELLO":
		return resp.EncodeSimpleString("OK")
	case "QUIT":
		return resp.EncodeSimpleString("OK")
	case "ECHO":
		if len(args) >= 1 {
			return resp.EncodeBulkString(args[0])
		}
		return resp.EncodeError("wrong number of arguments for 'echo' command")
	case "TIME":
		sec := time.Now().Unix()
		micro := time.Now().UnixNano() / 1000 % 1000000
		return resp.EncodeArray([][]byte{
			[]byte(strconv.FormatInt(sec, 10)),
			[]byte(strconv.FormatInt(micro, 10)),
		})
	case "DBSIZE":
		n, _ := c.Storage.DBSize()
		return resp.EncodeInteger(n)
	case "FLUSHDB":
		c.Storage.FlushDB()
		return resp.EncodeSimpleString("OK")
	case "SAVE":
		if c.SavePath == "" {
			return resp.EncodeError("ERR Background save disabled")
		}
		if err := c.Storage.SaveToFile(c.SavePath); err != nil {
			return resp.EncodeError("ERR " + err.Error())
		}
		return resp.EncodeSimpleString("OK")
	case "BGSAVE":
		if c.SavePath == "" {
			return resp.EncodeError("ERR Background save disabled")
		}
		c.bgsaveMu.Lock()
		if c.bgsaveInProgress {
			c.bgsaveMu.Unlock()
			return resp.EncodeError("ERR Background save already in progress")
		}
		c.bgsaveInProgress = true
		c.bgsaveMu.Unlock()
		go func() {
			defer func() {
				c.bgsaveMu.Lock()
				c.bgsaveInProgress = false
				c.bgsaveMu.Unlock()
			}()
			_ = c.Storage.SaveToFile(c.SavePath)
		}()
		return resp.EncodeSimpleString("Background saving started")
	case "RANDOMKEY":
		k, _ := c.Storage.RandomKey()
		return resp.EncodeBulkString(k)
	case "INFO":
		return resp.EncodeBulkString([]byte("# Server\nredis_mode:cluster\nos:go\n"))
	case "CONFIG":
		return c.handleConfig(args)
	case "CLUSTER":
		return c.handleCluster(args)
	}

	// Single-key commands
	if cmd == "GET" || cmd == "EXISTS" || cmd == "TTL" || cmd == "INCR" || cmd == "DECR" || cmd == "PERSIST" {
		if len(args) < 1 {
			return resp.EncodeError("wrong number of arguments for command")
		}
		key := args[0]
		replicas := c.getReplicas(key)
		if len(replicas) == 0 {
			return resp.EncodeError("CLUSTERDOWN The cluster is down")
		}
		switch cmd {
		case "GET":
			required := c.requiredReplicasFor(consistencyOverride)
			// Prefer primary (first replica) for consistency: primary is authoritative for recent writes,
			// so returning primaryVal avoids returning stale data when quorum is met from slower replicas.
			var primaryVal []byte
			responses := 0
			for _, r := range replicas {
				var val []byte
				var ok bool
				if r.nodeID == c.NodeID {
					val, _ = c.Storage.Get(key)
					ok = true
				} else {
					val, ok = c.rpcWithRetry(r, rpc.CmdGet, [][]byte{key})
				}
				if !ok {
					continue
				}
				responses++
				if primaryVal == nil {
					primaryVal = val
				}
				if responses >= required {
					break
				}
			}
			if responses >= required {
				return resp.EncodeBulkString(primaryVal)
			}
			return resp.EncodeError("CLUSTERDOWN Not enough replicas for QUORUM")
		case "EXISTS":
			required := c.requiredReplicasFor(consistencyOverride)
			responses, found := 0, false
			for _, r := range replicas {
				var n int
				var ok bool
				if r.nodeID == c.NodeID {
					n, _ = c.Storage.Exists(key)
					ok = true
				} else {
					var b []byte
					b, ok = c.rpcWithRetry(r, rpc.CmdExists, [][]byte{key})
					if ok && len(b) > 0 {
						n = int(b[0])
					}
				}
				if !ok {
					continue
				}
				responses++
				if n == 1 {
					found = true
				}
				if responses >= required {
					break
				}
			}
			if responses >= required {
				if found {
					return resp.EncodeInteger(1)
				}
				return resp.EncodeInteger(0)
			}
			return resp.EncodeError("CLUSTERDOWN Not enough replicas for QUORUM")
		case "TTL":
			required := c.requiredReplicasFor(consistencyOverride)
			var primaryTTL *int
			responses := 0
			for _, r := range replicas {
				var ok bool
				var t int
				if r.nodeID == c.NodeID {
					t, _ = c.Storage.TTL(key)
					ok = true
				} else {
					var b []byte
					b, ok = c.rpcWithRetry(r, rpc.CmdTTL, [][]byte{key})
					if ok && len(b) > 0 {
						t, _ = strconv.Atoi(string(b))
					}
				}
				if !ok {
					continue
				}
				responses++
				if primaryTTL == nil {
					tval := t
					primaryTTL = &tval
				}
				if responses >= required {
					return resp.EncodeInteger(*primaryTTL)
				}
			}
			return resp.EncodeError("CLUSTERDOWN Not enough replicas for QUORUM")
		case "INCR":
			// Only one replica performs the atomic INCR; others are replicated to.
			// Tradeoff: simpler than distributed consensus, but concurrent INCRs on different
			// replicas could race. We pick first successful replica, replicate result to others.
			for i, r := range replicas {
				var val int
				if r.nodeID == c.NodeID {
					v, err := c.Storage.Incr(key, 1)
					if err != nil {
						return resp.EncodeError(err.Error())
					}
					val = v
				} else {
					b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdIncr, [][]byte{key})
					if b != nil && !bytes.Equal(b, []byte("ERR")) {
						val, _ = strconv.Atoi(string(b))
					} else {
						continue
					}
				}
				// Replicate final value to other replicas
				for j, r2 := range replicas {
					if j != i {
						rpc.SendCommand(r2.host, r2.port, rpc.CmdSet, [][]byte{key, []byte(strconv.Itoa(val))})
					}
				}
				return resp.EncodeInteger(val)
			}
			return resp.EncodeError("replicas unavailable")
		case "DECR":
			// Same pattern as INCR: one replica does GET+SET locally; others get replicated.
			for _, r := range replicas {
				var val int
				if r.nodeID == c.NodeID {
					v, err := c.Storage.Incr(key, -1)
					if err != nil {
						return resp.EncodeError(err.Error())
					}
					val = v
				} else {
					b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdGet, [][]byte{key})
					cur := 0
					if len(b) > 0 {
						cur, _ = strconv.Atoi(string(b))
					}
					val = cur - 1
					rpc.SendCommand(r.host, r.port, rpc.CmdSet, [][]byte{key, []byte(strconv.Itoa(val))})
				}
				return resp.EncodeInteger(val)
			}
			return resp.EncodeError("replicas unavailable")
		case "PERSIST":
			required := c.requiredReplicasFor(consistencyOverride)
			acks := 0
			for _, r := range replicas {
				var ok bool
				if r.nodeID == c.NodeID {
					ok, _ = c.Storage.Persist(key)
				} else {
					b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdPersist, [][]byte{key})
					ok = bytes.Equal(b, []byte("1"))
				}
				if ok {
					acks++
				}
				if acks >= required {
					return resp.EncodeInteger(1)
				}
			}
			return resp.EncodeInteger(0)
		}
	}

	// SET
	if cmd == "SET" {
		if len(args) < 2 {
			return resp.EncodeError("wrong number of arguments for 'set' command")
		}
		key, value := args[0], args[1]
		var ttl *int
		if len(args) >= 4 && bytes.EqualFold(args[2], []byte("EX")) {
			secs, _ := strconv.Atoi(string(args[3]))
			ttl = &secs
		}
		replicas := c.getReplicas(key)
		if len(replicas) == 0 {
			return resp.EncodeError("CLUSTERDOWN The cluster is down")
		}
		acks := 0
		for _, r := range replicas {
			if r.nodeID == c.NodeID {
				c.Storage.Set(key, value, ttl)
				acks++
			} else {
				res, ok := c.rpcWithRetry(r, rpc.CmdSet, [][]byte{key, value})
				if ok && bytes.Equal(res, []byte("OK")) {
					acks++
				}
			}
		}
		required := c.requiredReplicasFor(consistencyOverride)
		if acks >= required {
			return resp.EncodeSimpleString("OK")
		}
		return resp.EncodeError("replication failed")
	}

	// DEL: replicate tombstone to all replicas for consistent deletion
	if cmd == "DEL" {
		if len(args) < 1 {
			return resp.EncodeError("wrong number of arguments for 'del' command")
		}
		required := c.requiredReplicasFor(consistencyOverride)
		total := 0
		for _, key := range args {
			replicas := c.getReplicas(key)
			if len(replicas) == 0 {
				continue
			}
			acks := 0
			had := false
			for _, r := range replicas {
				if r.nodeID == c.NodeID {
					hadKey, _ := c.Storage.SetTombstone(key)
					acks++
					had = had || hadKey
				} else {
					res, err := rpc.SendCommand(r.host, r.port, rpc.CmdSetTombstone, [][]byte{key})
					if err == nil && res != nil {
						acks++
						had = had || bytes.Equal(res, []byte("1"))
					}
				}
			}
			if acks >= required && had {
				total++
			}
		}
		return resp.EncodeInteger(total)
	}

	// EXPIRE
	if cmd == "EXPIRE" {
		if len(args) < 2 {
			return resp.EncodeError("wrong number of arguments for 'expire' command")
		}
		key := args[0]
		secs, _ := strconv.Atoi(string(args[1]))
		replicas := c.getReplicas(key)
		if len(replicas) == 0 {
			return resp.EncodeError("CLUSTERDOWN The cluster is down")
		}
		required := c.requiredReplicasFor(consistencyOverride)
		acks := 0
		keyExisted := false
		for _, r := range replicas {
			var ok bool
			if r.nodeID == c.NodeID {
				ok, _ = c.Storage.Expire(key, secs)
			} else {
				res, _ := rpc.SendCommand(r.host, r.port, rpc.CmdExpire, [][]byte{key, args[1]})
				ok = bytes.Equal(res, []byte("1"))
			}
			if ok {
				acks++
				keyExisted = true
			}
			if acks >= required {
				break
			}
		}
		if acks >= required && keyExisted {
			return resp.EncodeInteger(1)
		}
		return resp.EncodeInteger(0)
	}

	// KEYS
	if cmd == "KEYS" {
		pattern := []byte("*")
		if len(args) >= 1 {
			pattern = args[0]
		}
		seen := make(map[string]bool)
		for _, n := range c.Gossip.GetRingNodeStates() {
			var keys [][]byte
			if n.NodeID == c.NodeID {
				keys, _ = c.Storage.Keys(pattern)
			} else {
				r := replica{n.NodeID, n.Host, n.Port}
				b, ok := c.rpcWithRetry(r, rpc.CmdKeys, [][]byte{pattern})
				if ok && b != nil {
					for _, k := range bytes.Split(b, []byte("\n")) {
						if len(k) > 0 {
							keys = append(keys, k)
						}
					}
				}
			}
			for _, k := range keys {
				seen[string(k)] = true
			}
		}
		var items [][]byte
		for k := range seen {
			items = append(items, []byte(k))
		}
		return resp.EncodeArray(items)
	}

	// MSET
	if cmd == "MSET" {
		if len(args) < 2 || len(args)%2 != 0 {
			return resp.EncodeError("wrong number of arguments for 'mset' command")
		}
		required := c.requiredReplicasFor(consistencyOverride)
		for i := 0; i < len(args); i += 2 {
			key, value := args[i], args[i+1]
			replicas := c.getReplicas(key)
			acks := 0
			for _, r := range replicas {
				if r.nodeID == c.NodeID {
					c.Storage.Set(key, value, nil)
					acks++
				} else {
					res, _ := rpc.SendCommand(r.host, r.port, rpc.CmdSet, [][]byte{key, value})
					if bytes.Equal(res, []byte("OK")) {
						acks++
					}
				}
				if acks >= required {
					break
				}
			}
			if acks < required {
				return resp.EncodeError("replication failed")
			}
		}
		return resp.EncodeSimpleString("OK")
	}

	// MGET
	if cmd == "MGET" {
		var results [][]byte
		for _, key := range args {
			var val []byte
			for _, r := range c.getReplicas(key) {
				if r.nodeID == c.NodeID {
					val, _ = c.Storage.Get(key)
				} else {
					val, _ = rpc.SendCommand(r.host, r.port, rpc.CmdGet, [][]byte{key})
				}
				if val != nil {
					break
				}
			}
			results = append(results, val)
		}
		return resp.EncodeArray(results)
	}

	// List commands: LPUSH, RPUSH, LLEN, LRANGE, LPOP, RPOP, LINDEX, LSET, LREM, LTRIM
	if cmd == "LPUSH" || cmd == "RPUSH" || cmd == "LLEN" || cmd == "LRANGE" || cmd == "LPOP" ||
		cmd == "RPOP" || cmd == "LINDEX" || cmd == "LSET" || cmd == "LREM" || cmd == "LTRIM" {
		if len(args) < 1 {
			return resp.EncodeError("wrong number of arguments for command")
		}
		key := args[0]
		replicas := c.getReplicas(key)
		if len(replicas) == 0 {
			return resp.EncodeError("CLUSTERDOWN The cluster is down")
		}
		switch cmd {
		case "LPUSH":
			if len(args) < 2 {
				return resp.EncodeError("wrong number of arguments for 'lpush' command")
			}
			acks := 0
			var firstLen int
			for _, r := range replicas {
				if r.nodeID == c.NodeID {
					n, _ := c.Storage.LPush(key, args[1:]...)
					acks++
					firstLen = n
				} else {
					b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdLPush, args)
					if b != nil {
						acks++
						firstLen, _ = strconv.Atoi(string(b))
					}
				}
			}
			required := c.requiredReplicasFor(consistencyOverride)
			if acks >= required {
				return resp.EncodeInteger(firstLen)
			}
			return resp.EncodeError("replication failed")
		case "RPUSH":
			if len(args) < 2 {
				return resp.EncodeError("wrong number of arguments for 'rpush' command")
			}
			acks := 0
			var firstLen int
			for _, r := range replicas {
				if r.nodeID == c.NodeID {
					n, _ := c.Storage.RPush(key, args[1:]...)
					acks++
					firstLen = n
				} else {
					b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdRPush, args)
					if b != nil {
						acks++
						firstLen, _ = strconv.Atoi(string(b))
					}
				}
			}
			required := c.requiredReplicasFor(consistencyOverride)
			if acks >= required {
				return resp.EncodeInteger(firstLen)
			}
			return resp.EncodeError("replication failed")
		case "LLEN":
			required := c.requiredReplicasFor(consistencyOverride)
			var n int
			responses := 0
			for _, r := range replicas {
				var ok bool
				if r.nodeID == c.NodeID {
					n, _ = c.Storage.LLen(key)
					ok = true
				} else {
					var b []byte
					b, ok = c.rpcWithRetry(r, rpc.CmdLLen, [][]byte{key})
					if ok && len(b) > 0 {
						n, _ = strconv.Atoi(string(b))
					}
				}
				if !ok {
					continue
				}
				responses++
				if responses >= required {
					return resp.EncodeInteger(n)
				}
			}
			return resp.EncodeError("CLUSTERDOWN Not enough replicas for QUORUM")
		case "LRANGE":
			if len(args) < 3 {
				return resp.EncodeError("wrong number of arguments for 'lrange' command")
			}
			start, _ := strconv.Atoi(string(args[1]))
			stop, _ := strconv.Atoi(string(args[2]))
			for _, r := range replicas {
				var items [][]byte
				if r.nodeID == c.NodeID {
					items, _ = c.Storage.LRange(key, start, stop)
				} else {
					b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdLRange, [][]byte{key, args[1], args[2]})
					items = parseLRangeResponse(b)
				}
				if items != nil {
					return resp.EncodeArray(items)
				}
			}
			return resp.EncodeArray([][]byte{})
		case "LPOP":
			required := c.requiredReplicasFor(consistencyOverride)
			acks := 0
			var firstVal []byte
			for _, r := range replicas {
				var val []byte
				ok := false
				if r.nodeID == c.NodeID {
					val, _ = c.Storage.LPop(key)
					ok = true
				} else {
					val, ok = c.rpcWithRetry(r, rpc.CmdLPop, [][]byte{key})
				}
				if ok {
					acks++
					if firstVal == nil {
						firstVal = val
					}
					if acks >= required {
						return resp.EncodeBulkString(firstVal)
					}
				}
			}
			if acks > 0 {
				return resp.EncodeBulkString(firstVal)
			}
			return resp.EncodeError("replication failed")
		case "RPOP":
			required := c.requiredReplicasFor(consistencyOverride)
			acks := 0
			var firstVal []byte
			for _, r := range replicas {
				var val []byte
				ok := false
				if r.nodeID == c.NodeID {
					val, _ = c.Storage.RPop(key)
					ok = true
				} else {
					val, ok = c.rpcWithRetry(r, rpc.CmdRPop, [][]byte{key})
				}
				if ok {
					acks++
					if firstVal == nil {
						firstVal = val
					}
					if acks >= required {
						return resp.EncodeBulkString(firstVal)
					}
				}
			}
			if acks > 0 {
				return resp.EncodeBulkString(firstVal)
			}
			return resp.EncodeError("replication failed")
		case "LINDEX":
			if len(args) >= 2 {
				idx, _ := strconv.Atoi(string(args[1]))
				for _, r := range replicas {
					var val []byte
					if r.nodeID == c.NodeID {
						val, _ = c.Storage.LIndex(key, idx)
					} else {
						val, _ = rpc.SendCommand(r.host, r.port, rpc.CmdLIndex, [][]byte{key, args[1]})
					}
					return resp.EncodeBulkString(val)
				}
			}
		case "LSET":
			if len(args) >= 3 {
				idx, _ := strconv.Atoi(string(args[1]))
				acks := 0
				for _, r := range replicas {
					if r.nodeID == c.NodeID {
						err := c.Storage.LSet(key, idx, args[2])
						if err == nil {
							acks++
						}
					} else {
						b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdLSet, args)
						if bytes.Equal(b, []byte("OK")) {
							acks++
						}
					}
				}
				if acks >= c.requiredReplicasFor(consistencyOverride) {
					return resp.EncodeSimpleString("OK")
				}
			}
		case "LREM":
			if len(args) >= 3 {
				count, _ := strconv.Atoi(string(args[1]))
				required := c.requiredReplicasFor(consistencyOverride)
				acks := 0
				var firstN int
				for _, r := range replicas {
					var n int
					if r.nodeID == c.NodeID {
						n, _ = c.Storage.LRem(key, count, args[2])
						acks++
					} else {
						b, err := rpc.SendCommand(r.host, r.port, rpc.CmdLRem, args)
						if err == nil && len(b) > 0 {
							n, _ = strconv.Atoi(string(b))
							acks++
						}
					}
					if acks == 1 {
						firstN = n
					}
					if acks >= required {
						return resp.EncodeInteger(firstN)
					}
				}
				if acks > 0 {
					return resp.EncodeInteger(firstN)
				}
			}
		case "LTRIM":
			if len(args) >= 3 {
				start, _ := strconv.Atoi(string(args[1]))
				stop, _ := strconv.Atoi(string(args[2]))
				acks := 0
				for _, r := range replicas {
					if r.nodeID == c.NodeID {
						c.Storage.LTrim(key, start, stop)
						acks++
					} else {
						b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdLTrim, args)
						if bytes.Equal(b, []byte("OK")) {
							acks++
						}
					}
				}
				if acks >= c.requiredReplicasFor(consistencyOverride) {
					return resp.EncodeSimpleString("OK")
				}
			}
		}
	}

	// Extended: SETEX, TYPE, strings, sets, hashes, zsets - delegate to helper
	if out := c.handleExtended(cmd, args, consistencyOverride); out != nil {
		return out
	}

	return resp.EncodeError("unknown command '" + string(cmdName) + "'")
}

func (c *Coordinator) handleConfig(args [][]byte) []byte {
	if len(args) < 1 {
		return resp.EncodeError("wrong number of arguments for 'config' command")
	}
	sub := strings.ToUpper(string(args[0]))
	if sub == "GET" && len(args) >= 2 {
		// Return stub config to satisfy redis-benchmark (avoids "Could not fetch server CONFIG" warning)
		return resp.EncodeArray([][]byte{
			[]byte("maxmemory"), []byte("0"),
			[]byte("dir"), []byte("/tmp"),
		})
	}
	return resp.EncodeError("unsupported CONFIG subcommand")
}

// handleCluster implements CLUSTER subcommands.
// LEAVE: with arg = nodename → remove dead node; no args → this node gracefully leaves (signals shutdown).
// REPAIR: run repair locally and on peers; SMOOTH variant delays between vnodes to reduce load.
// TOMBSTONE GC: purge expired tombstone entries cluster-wide.
func (c *Coordinator) handleCluster(args [][]byte) []byte {
	sub := ""
	if len(args) >= 1 {
		sub = strings.ToUpper(string(args[0]))
	}
	ringNodes := c.Gossip.GetRingNodes()
	allNodes := c.Gossip.GetAllNodes()
	ring := c.Partitioner.GetRing()
	nodeMap := make(map[string]*cluster.NodeState)
	for _, n := range allNodes {
		nodeMap[n.NodeID] = n
	}

	switch sub {
	case "INFO":
		clusterName := c.ClusterName
		if clusterName == "" {
			clusterName = "carp"
		}
		return resp.EncodeBulkString([]byte("cluster_state:ok\ncluster_name:" + clusterName + "\ncluster_nodes:" + strconv.Itoa(len(ringNodes)) + "\ncluster_replication_factor:" + strconv.Itoa(c.ReplicationFactor) + "\n"))
	case "NODES":
		var lines []string
		for _, e := range ring {
			n := nodeMap[e.NodeID]
			if n == nil {
				continue
			}
			flags := "master"
			if e.NodeID == c.NodeID {
				flags = "myself,master"
			}
			rack := n.Rack
			if rack == "" {
				rack = "-"
			}
			lines = append(lines, e.NodeID+" "+n.Host+":"+strconv.Itoa(n.Port)+" "+strconv.Itoa(e.Token)+" "+rack+" "+flags)
		}
		return resp.EncodeBulkString([]byte(strings.Join(lines, "\n")))
	case "RING":
		var lines []string
		for _, e := range ring {
			n := nodeMap[e.NodeID]
			if n == nil {
				continue
			}
			rack := n.Rack
			if rack == "" {
				rack = "-"
			}
			lines = append(lines, e.NodeID+"|"+n.Host+":"+strconv.Itoa(n.Port)+"|"+strconv.Itoa(e.Token)+"|"+rack)
		}
		return resp.EncodeBulkString([]byte(strings.Join(lines, "\n")))
	case "KEYSLOT", "TOKEN":
		if len(args) < 2 {
			return resp.EncodeError("wrong number of arguments for CLUSTER KEYSLOT")
		}
		token := partitioner.TokenForKey(args[1])
		return resp.EncodeInteger(token)
	case "KEYNODE":
		if len(args) < 2 {
			return resp.EncodeError("wrong number of arguments for CLUSTER KEYNODE")
		}
		replicas := c.getReplicas(args[1])
		if len(replicas) == 0 {
			return resp.EncodeError("CLUSTERDOWN")
		}
		r := replicas[0]
		return resp.EncodeArray([][]byte{[]byte(r.nodeID), []byte(r.host), []byte(strconv.Itoa(r.port))})
	case "LEAVE":
		// CLUSTER LEAVE FORCE <nodename>: remove a dead node from the cluster (all peers).
		if len(args) >= 2 && strings.ToUpper(string(args[0])) == "FORCE" {
			nodeName := strings.TrimSpace(string(args[1]))
			if nodeName == "" {
				return resp.EncodeError("wrong number of arguments for 'cluster leave' command")
			}
			if nodeName == c.NodeID {
				return resp.EncodeError("ERR cannot force-remove self; use CLUSTER LEAVE " + nodeName + " for graceful leave")
			}
			if _, ok := nodeMap[nodeName]; !ok {
				return resp.EncodeError("ERR Unknown node " + nodeName)
			}
			if err := c.Gossip.RemoveDeadNode(nodeName); err != nil {
				return resp.EncodeError("ERR " + err.Error())
			}
			return resp.EncodeSimpleString("OK")
		}
		// CLUSTER LEAVE <nodename>: graceful leave (nodename must be this node)
		if len(args) < 1 {
			return resp.EncodeError("wrong number of arguments for 'cluster leave' command")
		}
		leaveNode := strings.TrimSpace(string(args[0]))
		if leaveNode != c.NodeID {
			return resp.EncodeError("ERR CLUSTER LEAVE <nodename> requires this node's name; use CLUSTER LEAVE FORCE " + leaveNode + " to remove a dead node")
		}
		if !c.Gossip.CanLeave() {
			return resp.EncodeError("ERR another node is already leaving the ring; wait for it to complete")
		}
		log.Printf("[cluster] Node %s initiating graceful leave", leaveNode)
		if !c.Gossip.SetLeaving() {
			return resp.EncodeError("ERR another node is already leaving the ring; wait for it to complete")
		}
		c.Gossip.BroadcastLeave(3)
		time.Sleep(500 * time.Millisecond)
		if c.ShutdownCh != nil {
			select {
			case c.ShutdownCh <- struct{}{}:
			default:
			}
		}
		return resp.EncodeSimpleString("OK")
	case "TOMBSTONE":
		if len(args) >= 2 && strings.ToUpper(string(args[1])) == "GC" {
			total := c.Storage.RunTombstoneGC()
			for _, n := range c.Gossip.GetRingNodeStates() {
				if n.NodeID == c.NodeID {
					continue
				}
				res, err := rpc.SendCommand(n.Host, n.Port, rpc.CmdRunTombstoneGC, nil)
				if err == nil && res != nil {
					if v, err := strconv.Atoi(string(res)); err == nil {
						total += v
					}
				}
			}
			log.Printf("[cluster] Tombstone GC purged %d entries cluster-wide", total)
			return resp.EncodeInteger(total)
		}
		return resp.EncodeError("usage: CLUSTER TOMBSTONE GC")
	case "REPAIR":
		smooth := len(args) >= 2 && strings.ToUpper(string(args[1])) == "SMOOTH"
		delayMs := 100
		if smooth && len(args) >= 3 {
			if d, err := strconv.Atoi(string(args[2])); err == nil && d > 0 {
				delayMs = d
			}
		}
		if smooth {
			log.Printf("[cluster] Running smooth repair on all nodes (delay=%dms between vnodes)", delayMs)
		} else {
			log.Printf("[cluster] Running repair on all nodes")
		}
		delayArg := []byte(strconv.Itoa(delayMs))
		var rpcCmd byte = rpc.CmdRunRepair
		if smooth {
			rpcCmd = rpc.CmdRunRepairSmooth
		}
		if smooth && c.RepairFuncSmooth != nil {
			c.RepairFuncSmooth(time.Duration(delayMs) * time.Millisecond)
		} else if !smooth && c.RepairFunc != nil {
			c.RepairFunc()
		}
		for _, n := range c.Gossip.GetRingNodeStates() {
			if n.NodeID == c.NodeID {
				continue
			}
			if smooth {
				rpc.SendCommand(n.Host, n.Port, rpcCmd, [][]byte{delayArg})
			} else {
				rpc.SendCommand(n.Host, n.Port, rpcCmd, nil)
			}
		}
		return resp.EncodeSimpleString("OK")
	}
	return resp.EncodeError("unknown CLUSTER subcommand '" + sub + "'")
}

// handleExtended handles SETEX, TYPE, string, set, hash, zset commands
func (c *Coordinator) handleExtended(cmd string, args [][]byte, consistencyOverride *ConsistencyLevel) []byte {
	if len(args) < 1 {
		return nil
	}
	key := args[0]
	replicas := c.getReplicas(key)
	if len(replicas) == 0 {
		return resp.EncodeError("CLUSTERDOWN The cluster is down")
	}
	required := c.requiredReplicasFor(consistencyOverride)

	switch cmd {
	case "SETEX":
		if len(args) < 3 {
			return resp.EncodeError("wrong number of arguments for 'setex' command")
		}
		// SETEX key seconds value (Redis standard)
		secs, _ := strconv.Atoi(string(args[1]))
		ttl := &secs
		acks := 0
		for _, r := range replicas {
			if r.nodeID == c.NodeID {
				c.Storage.Set(key, args[2], ttl)
				acks++
			} else {
				b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdSet, [][]byte{key, args[2]})
				if bytes.Equal(b, []byte("OK")) {
					acks++
				}
			}
		}
		if acks >= required {
			return resp.EncodeSimpleString("OK")
		}
		return resp.EncodeError("replication failed")
	case "TYPE":
		for _, r := range replicas {
			var t string
			if r.nodeID == c.NodeID {
				t = c.Storage.Type(key)
			} else {
				b, _ := rpc.SendCommand(r.host, r.port, rpc.CmdType, [][]byte{key})
				t = string(b)
			}
			return resp.EncodeBulkString([]byte(t))
		}
	case "STRLEN", "APPEND", "GETRANGE", "SETRANGE", "INCRBY", "DECRBY", "SETNX", "GETSET":
		return c.execStringCmd(cmd, key, replicas, args, required)
	case "SADD", "SREM", "SISMEMBER", "SMEMBERS", "SCARD", "SPOP":
		return c.execSetCmd(cmd, key, replicas, args, required)
	case "HSET", "HGET", "HMSET", "HMGET", "HGETALL", "HDEL", "HEXISTS", "HLEN", "HKEYS", "HVALS":
		return c.execHashCmd(cmd, key, replicas, args, required)
	case "ZADD", "ZREM", "ZRANGE", "ZRANK", "ZREVRANK", "ZSCORE", "ZCARD":
		return c.execZSetCmd(cmd, key, replicas, args, required)
	}
	return nil
}

func (c *Coordinator) execStringCmd(cmd string, key []byte, replicas []replica, args [][]byte, required int) []byte {
	// Netflix distributed counter: idempotency token for safe retry/hedge (optional 3rd arg)
	hasIdem := (cmd == "INCRBY" || cmd == "DECRBY") && len(args) >= 3
	var idemToken []byte
	if hasIdem {
		idemToken = args[2]
	}

	var rpcCmd byte
	switch cmd {
	case "STRLEN":
		rpcCmd = rpc.CmdStrlen
	case "APPEND":
		rpcCmd = rpc.CmdAppend
	case "GETRANGE":
		rpcCmd = rpc.CmdGetRange
	case "SETRANGE":
		rpcCmd = rpc.CmdSetRange
	case "INCRBY":
		if hasIdem {
			rpcCmd = rpc.CmdIncrByIdem
		} else {
			rpcCmd = rpc.CmdIncrBy
		}
	case "DECRBY":
		if len(args) >= 2 {
			delta, _ := strconv.Atoi(string(args[1]))
			newArgs := [][]byte{key, []byte(strconv.Itoa(-delta))}
			if hasIdem {
				newArgs = append(newArgs, idemToken)
				rpcCmd = rpc.CmdIncrByIdem
			} else {
				rpcCmd = rpc.CmdIncrBy
			}
			args = newArgs
		} else {
			return nil
		}
	case "SETNX":
		rpcCmd = rpc.CmdSetNX
	case "GETSET":
		rpcCmd = rpc.CmdGetSet
	default:
		return nil
	}
	writeCmds := map[string]bool{"GETSET": true, "SETNX": true, "APPEND": true, "SETRANGE": true, "INCRBY": true, "DECRBY": true}
	readCmds := map[string]bool{"GETRANGE": true, "STRLEN": true}

	// Don't require more acks than we have replicas (handles partial availability)
	if required > len(replicas) {
		required = len(replicas)
	}
	if required < 1 {
		required = 1
	}

	var firstResult []byte
	acks := 0

	// Netflix-style idempotency: primary applies INCRBY/DECRBY with token; then we replicate
	// (result, token) to others. Safe retry: duplicate requests with same token return same result.
	if hasIdem {
		for _, r := range replicas {
			var b []byte
			if firstResult == nil {
				if r.nodeID == c.NodeID {
					b = c.execStrLocalIncrByIdem(cmd, key, args)
				} else {
					b, _ = c.rpcWithRetry(r, rpcCmd, args)
				}
			} else {
				if r.nodeID == c.NodeID {
					resultVal, _ := strconv.Atoi(string(firstResult))
					c.Storage.Set(key, firstResult, nil)
					c.Storage.IdempotencyPut(key, idemToken, resultVal)
					b = firstResult
				} else {
					b, _ = c.rpcWithRetry(r, rpc.CmdIncrByRepl, [][]byte{key, firstResult, idemToken})
				}
			}
			if b != nil {
				acks++
				if firstResult == nil {
					firstResult = b
				}
				if acks >= required {
					n, _ := strconv.Atoi(string(firstResult))
					return resp.EncodeInteger(n)
				}
			}
		}
		if acks > 0 {
			return resp.EncodeError("replication failed")
		}
		return nil
	}

	for _, r := range replicas {
		var b []byte
		if r.nodeID == c.NodeID {
			b = c.execStrLocal(cmd, key, args)
		} else {
			b, _ = c.rpcWithRetry(r, rpcCmd, args)
		}
		if b != nil {
			acks++
			if firstResult == nil {
				firstResult = b
			}
			if readCmds[cmd] {
				switch cmd {
				case "GETRANGE":
					return resp.EncodeBulkString(b)
				case "STRLEN":
					n, _ := strconv.Atoi(string(b))
					return resp.EncodeInteger(n)
				}
				return nil
			}
			if acks >= required {
				switch cmd {
				case "GETSET":
					return resp.EncodeBulkString(firstResult)
				case "STRLEN", "APPEND", "SETRANGE", "INCRBY", "DECRBY", "SETNX":
					n, _ := strconv.Atoi(string(firstResult))
					return resp.EncodeInteger(n)
				}
			}
		}
	}
	if writeCmds[cmd] && acks > 0 {
		return resp.EncodeError("replication failed")
	}
	return nil
}

// execStrLocalIncrByIdem handles INCRBY/DECRBY with idempotency token (Netflix: safe retry).
func (c *Coordinator) execStrLocalIncrByIdem(cmd string, key []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return nil
	}
	delta, _ := strconv.Atoi(string(args[1]))
	token := []byte(nil)
	if len(args) >= 3 {
		token = args[2]
	}
	v, err := c.Storage.IncrByWithIdempotency(key, token, delta)
	if err != nil {
		return nil
	}
	return []byte(strconv.Itoa(v))
}

func (c *Coordinator) execStrLocal(cmd string, key []byte, args [][]byte) []byte {
	switch cmd {
	case "STRLEN":
		n, _ := c.Storage.Strlen(key)
		return []byte(strconv.Itoa(n))
	case "APPEND":
		n, _ := c.Storage.Append(key, args[1])
		return []byte(strconv.Itoa(n))
	case "GETRANGE":
		if len(args) >= 3 {
			start, _ := strconv.Atoi(string(args[1]))
			end, _ := strconv.Atoi(string(args[2]))
			v, _ := c.Storage.GetRange(key, start, end)
			return v
		}
	case "SETRANGE":
		if len(args) >= 3 {
			offset, _ := strconv.Atoi(string(args[1]))
			n, _ := c.Storage.SetRange(key, offset, args[2])
			return []byte(strconv.Itoa(n))
		}
	case "INCRBY", "DECRBY":
		if len(args) >= 2 {
			delta, _ := strconv.Atoi(string(args[1]))
			// For DECRBY, execStringCmd already mutates args to pass negative delta
			v, _ := c.Storage.IncrBy(key, delta)
			return []byte(strconv.Itoa(v))
		}
	case "SETNX":
		if len(args) >= 2 {
			n, _ := c.Storage.SetNX(key, args[1])
			return []byte(strconv.Itoa(n))
		}
	case "GETSET":
		if len(args) >= 2 {
			v, _ := c.Storage.GetSet(key, args[1])
			return v
		}
	}
	return nil
}

func (c *Coordinator) execSetCmd(cmd string, key []byte, replicas []replica, args [][]byte, required int) []byte {
	var rpcCmd byte
	switch cmd {
	case "SADD":
		rpcCmd = rpc.CmdSAdd
	case "SREM":
		rpcCmd = rpc.CmdSRem
	case "SISMEMBER":
		rpcCmd = rpc.CmdSIsMember
	case "SMEMBERS":
		rpcCmd = rpc.CmdSMembers
	case "SCARD":
		rpcCmd = rpc.CmdSCard
	case "SPOP":
		rpcCmd = rpc.CmdSPop
	default:
		return nil
	}
	acks := 0
	var firstResult []byte
	for _, r := range replicas {
		var b []byte
		if r.nodeID == c.NodeID {
			b = c.execSetLocal(cmd, key, args)
		} else {
			b, _ = rpc.SendCommand(r.host, r.port, rpcCmd, args)
		}
		if b != nil {
			acks++
			firstResult = b
		}
		if acks >= required {
			break
		}
	}
	if acks < required {
		return resp.EncodeError("replication failed")
	}
	switch cmd {
	case "SMEMBERS":
		items := bytes.Split(firstResult, []byte("\n"))
		var filtered [][]byte
		for _, it := range items {
			if len(it) > 0 {
				filtered = append(filtered, it)
			}
		}
		return resp.EncodeArray(filtered)
	case "SADD", "SREM", "SISMEMBER", "SCARD":
		n, _ := strconv.Atoi(string(firstResult))
		return resp.EncodeInteger(n)
	case "SPOP":
		return resp.EncodeBulkString(firstResult)
	}
	return nil
}

func (c *Coordinator) execSetLocal(cmd string, key []byte, args [][]byte) []byte {
	switch cmd {
	case "SADD":
		if len(args) >= 2 {
			n, _ := c.Storage.SAdd(key, args[1:]...)
			return []byte(strconv.Itoa(n))
		}
	case "SREM":
		if len(args) >= 2 {
			n, _ := c.Storage.SRem(key, args[1:]...)
			return []byte(strconv.Itoa(n))
		}
	case "SISMEMBER":
		if len(args) >= 2 {
			n, _ := c.Storage.SIsMember(key, args[1])
			return []byte(strconv.Itoa(n))
		}
	case "SMEMBERS":
		m, _ := c.Storage.SMembers(key)
		var out []byte
		for i, b := range m {
			if i > 0 {
				out = append(out, '\n')
			}
			out = append(out, b...)
		}
		return out
	case "SCARD":
		n, _ := c.Storage.SCard(key)
		return []byte(strconv.Itoa(n))
	case "SPOP":
		v, _ := c.Storage.SPop(key)
		return v
	}
	return nil
}

func (c *Coordinator) execHashCmd(cmd string, key []byte, replicas []replica, args [][]byte, required int) []byte {
	var rpcCmd byte
	switch cmd {
	case "HSET":
		rpcCmd = rpc.CmdHSet
	case "HGET":
		rpcCmd = rpc.CmdHGet
	case "HMSET":
		rpcCmd = rpc.CmdHMSet
	case "HMGET":
		rpcCmd = rpc.CmdHMGet
	case "HGETALL":
		rpcCmd = rpc.CmdHGetAll
	case "HDEL":
		rpcCmd = rpc.CmdHDel
	case "HEXISTS":
		rpcCmd = rpc.CmdHExists
	case "HLEN":
		rpcCmd = rpc.CmdHLen
	case "HKEYS":
		rpcCmd = rpc.CmdHKeys
	case "HVALS":
		rpcCmd = rpc.CmdHVals
	default:
		return nil
	}
	hashReadCmds := map[string]bool{"HGET": true, "HMGET": true, "HGETALL": true, "HEXISTS": true, "HLEN": true, "HKEYS": true, "HVALS": true}
	for _, r := range replicas {
		var b []byte
		if r.nodeID == c.NodeID {
			b = c.execHashLocal(cmd, key, args)
			// Hash read commands can return nil/empty; return local result immediately
			if hashReadCmds[cmd] {
				return c.encodeHashResponse(cmd, b)
			}
		} else {
			b, _ = rpc.SendCommand(r.host, r.port, rpcCmd, args)
		}
		if b != nil {
			return c.encodeHashResponse(cmd, b)
		}
	}
	return nil
}

func (c *Coordinator) encodeHashResponse(cmd string, b []byte) []byte {
	switch cmd {
	case "HGET":
		return resp.EncodeBulkString(b)
	case "HGETALL":
		return resp.EncodeArray(parseLRangeResponse(b))
	case "HMGET":
		return resp.EncodeArray(parseLRangeResponse(b))
	case "HKEYS", "HVALS":
		items := bytes.Split(b, []byte("\n"))
		var filtered [][]byte
		for _, it := range items {
			if len(it) > 0 {
				filtered = append(filtered, it)
			}
		}
		return resp.EncodeArray(filtered)
	case "HSET", "HDEL", "HEXISTS", "HLEN":
		n, _ := strconv.Atoi(string(b))
		return resp.EncodeInteger(n)
	case "HMSET":
		return resp.EncodeSimpleString("OK")
	default:
		return nil
	}
}

func (c *Coordinator) execHashLocal(cmd string, key []byte, args [][]byte) []byte {
	switch cmd {
	case "HSET":
		if len(args) >= 3 {
			n, _ := c.Storage.HSet(key, args[1], args[2])
			return []byte(strconv.Itoa(n))
		}
	case "HGET":
		if len(args) >= 2 {
			v, _ := c.Storage.HGet(key, args[1])
			return v
		}
	case "HMSET":
		if len(args) >= 3 {
			for i := 1; i+1 < len(args); i += 2 {
				c.Storage.HSet(key, args[i], args[i+1])
			}
			return []byte("OK")
		}
	case "HMGET":
		if len(args) >= 2 {
			size := 4
			for i := 1; i < len(args); i++ {
				v, _ := c.Storage.HGet(key, args[i])
				size += 4 + len(v)
			}
			buf := make([]byte, size)
			binary.BigEndian.PutUint32(buf[0:4], uint32(len(args)-1))
			off := 4
			for i := 1; i < len(args); i++ {
				v, _ := c.Storage.HGet(key, args[i])
				binary.BigEndian.PutUint32(buf[off:], uint32(len(v)))
				off += 4
				copy(buf[off:], v)
				off += len(v)
			}
			return buf[:off]
		}
	case "HGETALL":
		pairs, _ := c.Storage.HGetAll(key)
		size := 4
		for _, p := range pairs {
			size += 4 + len(p)
		}
		buf := make([]byte, size)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(pairs)))
		off := 4
		for _, p := range pairs {
			binary.BigEndian.PutUint32(buf[off:], uint32(len(p)))
			off += 4
			copy(buf[off:], p)
			off += len(p)
		}
		return buf
	case "HDEL":
		if len(args) >= 2 {
			n, _ := c.Storage.HDel(key, args[1:]...)
			return []byte(strconv.Itoa(n))
		}
	case "HEXISTS":
		if len(args) >= 2 {
			n, _ := c.Storage.HExists(key, args[1])
			return []byte(strconv.Itoa(n))
		}
	case "HLEN":
		n, _ := c.Storage.HLen(key)
		return []byte(strconv.Itoa(n))
	case "HKEYS":
		keys, _ := c.Storage.HKeys(key)
		var out []byte
		for i, k := range keys {
			if i > 0 {
				out = append(out, '\n')
			}
			out = append(out, k...)
		}
		return out
	case "HVALS":
		vals, _ := c.Storage.HVals(key)
		var out []byte
		for i, v := range vals {
			if i > 0 {
				out = append(out, '\n')
			}
			out = append(out, v...)
		}
		return out
	}
	return nil
}

func (c *Coordinator) execZSetCmd(cmd string, key []byte, replicas []replica, args [][]byte, required int) []byte {
	var rpcCmd byte
	switch cmd {
	case "ZADD":
		rpcCmd = rpc.CmdZAdd
	case "ZREM":
		rpcCmd = rpc.CmdZRem
	case "ZRANGE":
		rpcCmd = rpc.CmdZRange
	case "ZRANK":
		rpcCmd = rpc.CmdZRank
	case "ZREVRANK":
		rpcCmd = rpc.CmdZRevRank
	case "ZSCORE":
		rpcCmd = rpc.CmdZScore
	case "ZCARD":
		rpcCmd = rpc.CmdZCard
	default:
		return nil
	}
	acks := 0
	var firstResult []byte
	for _, r := range replicas {
		var b []byte
		if r.nodeID == c.NodeID {
			b = c.execZSetLocal(cmd, key, args)
		} else {
			b, _ = rpc.SendCommand(r.host, r.port, rpcCmd, args)
		}
		if b != nil {
			acks++
			firstResult = b
		}
		if acks >= required {
			break
		}
	}
	if acks < required {
		return resp.EncodeError("replication failed")
	}
	switch cmd {
	case "ZRANGE":
		items := parseLRangeResponse(firstResult)
		return resp.EncodeArray(items)
	case "ZADD", "ZREM", "ZRANK", "ZREVRANK", "ZCARD":
		n, _ := strconv.Atoi(string(firstResult))
		return resp.EncodeInteger(n)
	case "ZSCORE":
		return resp.EncodeBulkString(firstResult)
	}
	return nil
}

func (c *Coordinator) execZSetLocal(cmd string, key []byte, args [][]byte) []byte {
	switch cmd {
	case "ZADD":
		if len(args) >= 3 {
			n, _ := c.Storage.ZAdd(key, args[1:])
			return []byte(strconv.Itoa(n))
		}
	case "ZREM":
		if len(args) >= 2 {
			n, _ := c.Storage.ZRem(key, args[1:]...)
			return []byte(strconv.Itoa(n))
		}
	case "ZRANGE":
		if len(args) >= 3 {
			start, _ := strconv.Atoi(string(args[1]))
			stop, _ := strconv.Atoi(string(args[2]))
			withScores := len(args) >= 4 && bytes.EqualFold(args[3], []byte("WITHSCORES"))
			items, _ := c.Storage.ZRange(key, start, stop, withScores)
			size := 4
			for _, it := range items {
				size += 4 + len(it)
			}
			buf := make([]byte, size)
			binary.BigEndian.PutUint32(buf[0:4], uint32(len(items)))
			off := 4
			for _, it := range items {
				binary.BigEndian.PutUint32(buf[off:], uint32(len(it)))
				off += 4
				copy(buf[off:], it)
				off += len(it)
			}
			return buf[:off]
		}
	case "ZRANK":
		if len(args) >= 2 {
			rank, _ := c.Storage.ZRank(key, args[1])
			return []byte(strconv.Itoa(rank))
		}
	case "ZREVRANK":
		if len(args) >= 2 {
			rank, _ := c.Storage.ZRevRank(key, args[1])
			return []byte(strconv.Itoa(rank))
		}
	case "ZSCORE":
		if len(args) >= 2 {
			score, ok, _ := c.Storage.ZScore(key, args[1])
			if ok {
				return []byte(strconv.FormatFloat(score, 'f', -1, 64))
			}
		}
	case "ZCARD":
		n, _ := c.Storage.ZCard(key)
		return []byte(strconv.Itoa(n))
	}
	return nil
}

// parseLRangeResponse decodes RPC wire format: [4-byte item count][4-byte len][bytes]... per item.
// Used for LRANGE, HMGET, HGETALL, ZRANGE responses.
func parseLRangeResponse(b []byte) [][]byte {
	if len(b) < 4 {
		return nil
	}
	n := binary.BigEndian.Uint32(b[0:4])
	var items [][]byte
	off := 4
	for i := 0; i < int(n) && off+4 <= len(b); i++ {
		alen := binary.BigEndian.Uint32(b[off:])
		off += 4
		if off+int(alen) > len(b) {
			break
		}
		items = append(items, b[off:off+int(alen)])
		off += int(alen)
	}
	return items
}
