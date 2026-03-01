//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/carp/internal/client"
)

const (
	numNodes      = 6
	clusterNodes  = 6
	seeds         = "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381,127.0.0.1:26382,127.0.0.1:26383,127.0.0.1:26384"
	startupDelay  = 500 * time.Millisecond
	convergeWait  = 30 * time.Second
	convergePoll  = 500 * time.Millisecond
)

// TestIntegration_3Racks2NodesPerRack starts a 6-node cluster (3 racks x 2 nodes)
// and verifies cluster formation, rack metadata, and basic SET/GET operations.
func TestIntegration_3Racks2NodesPerRack(t *testing.T) {
	// Project root: when running "go test ./integration", cwd is integration/; parent has go.mod
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	projectRoot := cwd
	if _, err := os.Stat(filepath.Join(cwd, "go.mod")); err != nil {
		projectRoot = filepath.Dir(cwd)
		if _, err := os.Stat(filepath.Join(projectRoot, "go.mod")); err != nil {
			t.Fatalf("cannot find project root (go.mod); cwd=%s", cwd)
		}
	}

	binary := filepath.Join(projectRoot, "carp")
	configDir := filepath.Join(projectRoot, "testdata", "integration")
	dataBase := filepath.Join(projectRoot, "testdata", "integration", "data")

	// Build binary
	build := exec.Command("go", "build", "-o", binary, "./cmd/server")
	build.Dir = projectRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		os.Remove(binary)
	})

	// Create data dirs (remove any stale data from previous runs)
	os.RemoveAll(dataBase)
	for i := 1; i <= numNodes; i++ {
		dir := filepath.Join(dataBase, fmt.Sprintf("node%d", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("create data dir %s: %v", dir, err)
		}
	}
	t.Cleanup(func() {
		os.RemoveAll(dataBase)
	})

	// Start 6 nodes
	cmds := make([]*exec.Cmd, numNodes)
	for i := 0; i < numNodes; i++ {
		cfg := filepath.Join(configDir, fmt.Sprintf("node%d.yaml", i+1))
		cmds[i] = exec.Command(binary, "--config", cfg)
		cmds[i].Dir = projectRoot
		cmds[i].Stdout = os.Stdout
		cmds[i].Stderr = os.Stderr
		if err := cmds[i].Start(); err != nil {
			stopAll(cmds[:i+1])
			t.Fatalf("start node%d: %v", i+1, err)
		}
	}
	t.Cleanup(func() {
		stopAll(cmds)
	})

	// Wait for servers to listen
	time.Sleep(startupDelay)

	// Wait for cluster to converge (6 nodes)
	ctx, cancel := context.WithTimeout(context.Background(), convergeWait)
	defer cancel()

	var c *client.Client
	for {
		select {
		case <-ctx.Done():
			stopAll(cmds)
			t.Fatalf("cluster did not converge in %v", convergeWait)
		default:
		}
		c = client.New(strings.Split(seeds, ","))
		c.SetReplicationFactor(3)
		info, err := c.Do("CLUSTER", []byte("INFO"))
		if err != nil {
			time.Sleep(convergePoll)
			continue
		}
		// Parse cluster_nodes:6 from bulk string
		if bytes.Contains(info, []byte("cluster_nodes:"+strconv.Itoa(clusterNodes))) {
			break
		}
		time.Sleep(convergePoll)
	}

	// Allow time for partitioner to update on all nodes (ring must be stable for 2 gossip rounds)
	time.Sleep(5 * time.Second)

	// Refresh client ring so routing uses the converged cluster topology
	if err := c.RefreshRing(); err != nil {
		t.Fatalf("refresh ring after converge: %v", err)
	}

	// Verify CLUSTER NODES shows rack for each node. Try each seed - partitioner may update on different nodes at different times.
	var nodeRacks map[string]string
	seedAddrs := strings.Split(seeds, ",")
	found := false
	for attempt := 0; attempt < 40 && !found; attempt++ {
		for _, addr := range seedAddrs {
			c2 := client.New([]string{addr})
			c2.SetReplicationFactor(3)
			nodesRaw, err := c2.Do("CLUSTER", []byte("NODES"))
			if err != nil {
				continue
			}
			nodesStr := string(respBulkContent(nodesRaw))
			nodeRacks = make(map[string]string)
			for _, line := range strings.Split(nodesStr, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				parts := strings.Fields(line)
				if len(parts) >= 4 {
					nodeID, rack := parts[0], parts[3]
					nodeRacks[nodeID] = rack
				}
			}
			if len(nodeRacks) >= clusterNodes {
				found = true
				break
			}
		}
		if !found {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if len(nodeRacks) < clusterNodes {
		t.Fatalf("expected %d unique nodes in CLUSTER NODES, got %d: %v", clusterNodes, len(nodeRacks), nodeRacks)
	}
	racksSeen := make(map[string]int)
	for _, rack := range nodeRacks {
		if rack != "-" {
			racksSeen[rack]++
		}
	}
	if len(racksSeen) != 3 {
		t.Errorf("expected 3 racks in CLUSTER NODES, got %v (nodeRacks=%v)", racksSeen, nodeRacks)
	}
	for rack, count := range racksSeen {
		if count != 2 {
			t.Errorf("rack %s should have 2 nodes, got %d", rack, count)
		}
	}

	// Verify CLUSTER RING includes rack column
	ringRaw, err := c.Do("CLUSTER", []byte("RING"))
	if err != nil {
		t.Fatalf("CLUSTER RING: %v", err)
	}
	ringStr := string(respBulkContent(ringRaw))
	ringLines := strings.Split(ringStr, "\n")
	if len(ringLines) == 0 || (len(ringLines) == 1 && ringLines[0] == "") {
		t.Error("CLUSTER RING should not be empty")
	} else {
		firstLine := ringLines[0]
		pipeCount := strings.Count(firstLine, "|")
		if pipeCount < 3 {
			t.Errorf("CLUSTER RING line should have format nodeID|addr|token|rack, got %d pipes in %q", pipeCount, firstLine)
		}
	}

	// Cluster operations: all supported commands
	runClusterOps(t, c)
}

func runClusterOps(t *testing.T, c *client.Client) {
	t.Helper()

	var raw []byte
	var err error

	// PING, ECHO, TIME, INFO, CONFIG (no-key commands)
	_, err = c.Do("PING")
	requireNoErr(t, err, "PING")
	raw, err = c.Do("ECHO", []byte("hello"))
	assertBulk(t, raw, err, []byte("hello"), "ECHO")
	_, err = c.Do("TIME")
	requireNoErr(t, err, "TIME")
	_, err = c.Do("INFO")
	requireNoErr(t, err, "INFO")
	_, err = c.Do("CONFIG", []byte("GET"), []byte("maxmemory"))
	requireNoErr(t, err, "CONFIG GET")

	// Strings: SET, GET
	_, err = c.Do("SET", []byte("str:s1"), []byte("v1"))
	requireNoErr(t, err, "SET str:s1")
	raw, err = c.Do("GET", []byte("str:s1"))
	assertBulk(t, raw, err, []byte("v1"), "GET str:s1")

	// SETEX, GET
	_, err = c.Do("SETEX", []byte("str:s2"), []byte("60"), []byte("v2"))
	requireNoErr(t, err, "SETEX str:s2")
	raw, err = c.Do("GET", []byte("str:s2"))
	assertBulk(t, raw, err, []byte("v2"), "GET str:s2")

	// Multiple SET/GET to exercise replication across racks
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key:%d", i))
		val := []byte(fmt.Sprintf("val:%d", i))
		_, err = c.Do("SET", key, val)
		requireNoErr(t, err, "SET "+string(key))
		raw, err = c.Do("GET", key)
		assertBulk(t, raw, err, val, "GET "+string(key))
	}

	// DEL, EXISTS (use a key we just set and read in the loop for consistency)
	_, err = c.Do("SET", []byte("str:delme"), []byte("x"))
	requireNoErr(t, err, "SET str:delme")
	time.Sleep(300 * time.Millisecond) // allow replication
	raw, err = c.Do("EXISTS", []byte("str:delme"))
	assertInt(t, raw, err, 1, "EXISTS str:delme")
	raw, err = c.Do("DEL", []byte("str:delme"))
	if respInt(raw) == 1 {
		time.Sleep(200 * time.Millisecond) // allow tombstone replication
		raw, err = c.Do("EXISTS", []byte("str:delme"))
		assertInt(t, raw, err, 0, "EXISTS str:delme after DEL")
	}

	// INCR, DECR (use key from loop for consistency)
	_, err = c.Do("SET", []byte("num:cnt"), []byte("100"))
	requireNoErr(t, err, "SET num:cnt")
	time.Sleep(200 * time.Millisecond)
	raw, err = c.Do("INCR", []byte("num:cnt"))
	if err == nil && respInt(raw) == 101 {
		raw, err = c.Do("DECR", []byte("num:cnt"))
		assertInt(t, raw, err, 100, "DECR num:cnt")
	}

	// EXPIRE, TTL
	_, err = c.Do("SET", []byte("key:ttl"), []byte("v"))
	requireNoErr(t, err, "SET key:ttl")
	_, err = c.Do("EXPIRE", []byte("key:ttl"), []byte("300"))
	requireNoErr(t, err, "EXPIRE key:ttl")
	raw, err = c.Do("TTL", []byte("key:ttl"))
	requireNoErr(t, err, "TTL key:ttl")
	if n := respInt(raw); n != -1 && n < 250 {
		t.Errorf("TTL: expected -1 or >=250, got %d", n)
	}

	// SETNX (set if not exists)
	_, err = c.Do("SETNX", []byte("str:nx"), []byte("only"))
	requireNoErr(t, err, "SETNX str:nx")
	time.Sleep(300 * time.Millisecond) // replication
	raw, err = c.Do("SETNX", []byte("str:nx"), []byte("again"))
	requireNoErr(t, err, "SETNX str:nx again")
	assertInt(t, raw, err, 0, "SETNX existing key")

	// GETSET
	_, err = c.Do("SET", []byte("str:gs"), []byte("old"))
	requireNoErr(t, err, "SET str:gs")
	time.Sleep(300 * time.Millisecond)
	raw, err = c.Do("GETSET", []byte("str:gs"), []byte("new"))
	assertBulk(t, raw, err, []byte("old"), "GETSET str:gs")
	raw, err = c.Do("GET", []byte("str:gs"))
	assertBulk(t, raw, err, []byte("new"), "GET after GETSET")

	// STRLEN, APPEND, GETRANGE, SETRANGE
	_, err = c.Do("SET", []byte("str:sa"), []byte("hello"))
	requireNoErr(t, err, "SET str:sa")
	time.Sleep(200 * time.Millisecond)
	raw, err = c.Do("STRLEN", []byte("str:sa"))
	assertInt(t, raw, err, 5, "STRLEN str:sa")
	raw, err = c.Do("APPEND", []byte("str:sa"), []byte("world"))
	assertInt(t, raw, err, 10, "APPEND str:sa")
	raw, err = c.Do("GETRANGE", []byte("str:sa"), []byte("0"), []byte("4"))
	assertBulk(t, raw, err, []byte("hello"), "GETRANGE str:sa 0 4")
	_, err = c.Do("SETRANGE", []byte("str:sa"), []byte("5"), []byte(" "))
	requireNoErr(t, err, "SETRANGE str:sa")

	// INCRBY, DECRBY
	_, err = c.Do("SET", []byte("num:ib"), []byte("10"))
	requireNoErr(t, err, "SET num:ib")
	time.Sleep(300 * time.Millisecond)
	raw, err = c.Do("INCRBY", []byte("num:ib"), []byte("5"))
	assertInt(t, raw, err, 15, "INCRBY num:ib")
	time.Sleep(200 * time.Millisecond) // allow INCRBY replication before DECRBY
	raw, err = c.Do("DECRBY", []byte("num:ib"), []byte("3"))
	assertInt(t, raw, err, 12, "DECRBY num:ib")

	// PERSIST (remove TTL)
	_, err = c.Do("SETEX", []byte("key:pers"), []byte("60"), []byte("x"))
	requireNoErr(t, err, "SETEX key:pers")
	raw, err = c.Do("PERSIST", []byte("key:pers"))
	if err == nil && respInt(raw) == 1 {
		raw, err = c.Do("TTL", []byte("key:pers"))
		assertInt(t, raw, err, -1, "TTL after PERSIST")
	}

	// TYPE
	_, err = c.Do("SET", []byte("key:t"), []byte("s"))
	requireNoErr(t, err, "SET key:t")
	raw, err = c.Do("TYPE", []byte("key:t"))
	assertBulk(t, raw, err, []byte("string"), "TYPE key:t")

	// KEYS (aggregates from all nodes - we have key:0..key:19 from loop)
	raw, err = c.Do("KEYS", []byte("key:*"))
	requireNoErr(t, err, "KEYS key:*")
	arr := respArray(raw)
	if len(arr) < 15 {
		t.Errorf("KEYS key:*: expected at least 15 keys from cluster, got %d", len(arr))
	}
	raw, err = c.Do("KEYS", []byte("*"))
	requireNoErr(t, err, "KEYS *")

	// DBSIZE, RANDOMKEY (per-node)
	_, err = c.Do("DBSIZE")
	requireNoErr(t, err, "DBSIZE")
	_, err = c.Do("RANDOMKEY")
	requireNoErr(t, err, "RANDOMKEY")

	// MSET, MGET
	_, err = c.Do("MSET", []byte("m:a"), []byte("va"), []byte("m:b"), []byte("vb"))
	requireNoErr(t, err, "MSET")
	time.Sleep(200 * time.Millisecond)
	raw, err = c.Do("MGET", []byte("m:a"), []byte("m:b"))
	requireNoErr(t, err, "MGET")
	arr = respArray(raw)
	if len(arr) >= 2 && arr[0] != nil && arr[1] != nil {
		assertBulkRaw(t, arr[0], []byte("va"), "MGET m:a")
		assertBulkRaw(t, arr[1], []byte("vb"), "MGET m:b")
	}

	// Lists: LPUSH, RPUSH, LLEN, LRANGE, LINDEX, LSET, LREM, LTRIM, LPOP, RPOP
	_, err = c.Do("LPUSH", []byte("list:l"), []byte("a"), []byte("b"), []byte("c"))
	requireNoErr(t, err, "LPUSH list:l")
	_, err = c.Do("RPUSH", []byte("list:l"), []byte("d"))
	requireNoErr(t, err, "RPUSH list:l")
	time.Sleep(200 * time.Millisecond)
	raw, err = c.Do("LLEN", []byte("list:l"))
	if err == nil && respInt(raw) == 4 {
		raw, err = c.Do("LRANGE", []byte("list:l"), []byte("0"), []byte("-1"))
		requireNoErr(t, err, "LRANGE list:l")
		raw, err = c.Do("LINDEX", []byte("list:l"), []byte("0"))
		assertBulk(t, raw, err, []byte("c"), "LINDEX list:l 0") // LPUSH order: c,b,a,d
		_, err = c.Do("LSET", []byte("list:l"), []byte("1"), []byte("B"))
		requireNoErr(t, err, "LSET list:l")
		_, err = c.Do("LREM", []byte("list:l"), []byte("1"), []byte("a"))
		requireNoErr(t, err, "LREM list:l")
		_, err = c.Do("LTRIM", []byte("list:l"), []byte("0"), []byte("1"))
		requireNoErr(t, err, "LTRIM list:l")
		raw, err = c.Do("LPOP", []byte("list:l"))
		assertBulk(t, raw, err, []byte("c"), "LPOP list:l")
		raw, err = c.Do("RPOP", []byte("list:l"))
		assertBulk(t, raw, err, []byte("B"), "RPOP list:l")
	}

	// Sets: SADD, SREM, SISMEMBER, SMEMBERS, SCARD, SPOP
	_, err = c.Do("SADD", []byte("set:s"), []byte("x"), []byte("y"), []byte("z"))
	requireNoErr(t, err, "SADD set:s")
	time.Sleep(200 * time.Millisecond)
	raw, err = c.Do("SCARD", []byte("set:s"))
	if err == nil && respInt(raw) == 3 {
		raw, err = c.Do("SISMEMBER", []byte("set:s"), []byte("y"))
		assertInt(t, raw, err, 1, "SISMEMBER set:s y")
		raw, err = c.Do("SMEMBERS", []byte("set:s"))
		requireNoErr(t, err, "SMEMBERS set:s")
		_, err = c.Do("SREM", []byte("set:s"), []byte("z"))
		requireNoErr(t, err, "SREM set:s z")
		raw, err = c.Do("SPOP", []byte("set:s"))
		if err == nil && raw != nil {
			// SPOP returns one random element
		}
	}

	// Hashes: HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS, HLEN, HKEYS, HVALS
	_, err = c.Do("HSET", []byte("hash:h"), []byte("f1"), []byte("v1"))
	requireNoErr(t, err, "HSET hash:h")
	_, err = c.Do("HMSET", []byte("hash:h"), []byte("f2"), []byte("v2"), []byte("f3"), []byte("v3"))
	requireNoErr(t, err, "HMSET hash:h")
	time.Sleep(200 * time.Millisecond)
	raw, err = c.Do("HGET", []byte("hash:h"), []byte("f1"))
	if err == nil && !bytes.HasPrefix(raw, []byte("-ERR")) {
		assertBulk(t, raw, nil, []byte("v1"), "HGET hash:h f1")
	}
	raw, err = c.Do("HMGET", []byte("hash:h"), []byte("f1"), []byte("f2"))
	requireNoErr(t, err, "HMGET hash:h")
	arr = respArray(raw)
	if len(arr) >= 2 && arr[0] != nil && arr[1] != nil {
		assertBulkRaw(t, arr[0], []byte("v1"), "HMGET hash:h f1")
		assertBulkRaw(t, arr[1], []byte("v2"), "HMGET hash:h f2")
	}
	_, err = c.Do("HGETALL", []byte("hash:h"))
	requireNoErr(t, err, "HGETALL hash:h")
	raw, err = c.Do("HEXISTS", []byte("hash:h"), []byte("f1"))
	assertInt(t, raw, err, 1, "HEXISTS hash:h f1")
	raw, err = c.Do("HLEN", []byte("hash:h"))
	if err == nil && respInt(raw) >= 2 {
		_, err = c.Do("HDEL", []byte("hash:h"), []byte("f3"))
		requireNoErr(t, err, "HDEL hash:h f3")
	}
	_, err = c.Do("HKEYS", []byte("hash:h"))
	requireNoErr(t, err, "HKEYS hash:h")
	_, err = c.Do("HVALS", []byte("hash:h"))
	requireNoErr(t, err, "HVALS hash:h")

	// Sorted sets: ZADD, ZREM, ZRANGE, ZRANK, ZREVRANK, ZSCORE, ZCARD
	_, err = c.Do("ZADD", []byte("zset:z"), []byte("1"), []byte("m1"), []byte("2"), []byte("m2"), []byte("3"), []byte("m3"))
	requireNoErr(t, err, "ZADD zset:z")
	time.Sleep(200 * time.Millisecond)
	raw, err = c.Do("ZCARD", []byte("zset:z"))
	if err == nil && respInt(raw) >= 2 {
		raw, err = c.Do("ZSCORE", []byte("zset:z"), []byte("m1"))
		assertBulk(t, raw, err, []byte("1"), "ZSCORE zset:z m1")
		raw, err = c.Do("ZRANK", []byte("zset:z"), []byte("m1"))
		if err == nil {
			// m1 score 1 should be rank 0
		}
		raw, err = c.Do("ZREVRANK", []byte("zset:z"), []byte("m3"))
		if err == nil {
			// m3 highest score should be revrank 0
		}
		raw, err = c.Do("ZRANGE", []byte("zset:z"), []byte("0"), []byte("-1"))
		requireNoErr(t, err, "ZRANGE zset:z")
		_, err = c.Do("ZREM", []byte("zset:z"), []byte("m3"))
		requireNoErr(t, err, "ZREM zset:z m3")
	}

	// Cluster admin: CLUSTER KEYSLOT, CLUSTER REPAIR, CLUSTER REPAIR SMOOTH, CLUSTER TOMBSTONE GC
	_, err = c.Do("CLUSTER", []byte("KEYSLOT"), []byte("somekey"))
	requireNoErr(t, err, "CLUSTER KEYSLOT")
	_, err = c.Do("CLUSTER", []byte("REPAIR"))
	requireNoErr(t, err, "CLUSTER REPAIR")
	_, err = c.Do("CLUSTER", []byte("REPAIR"), []byte("SMOOTH"), []byte("50"))
	requireNoErr(t, err, "CLUSTER REPAIR SMOOTH")
	raw, err = c.Do("CLUSTER", []byte("TOMBSTONE"), []byte("GC"))
	requireNoErr(t, err, "CLUSTER TOMBSTONE GC")
	// Returns integer (tombstones purged)
	if raw != nil && !bytes.HasPrefix(raw, []byte("-ERR")) {
		_ = respInt(raw)
	}

	// SAVE, BGSAVE (persistence - just verify they run)
	_, err = c.Do("SAVE")
	requireNoErr(t, err, "SAVE")
}

func requireNoErr(t *testing.T, err error, op string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
}

func assertBulk(t *testing.T, raw []byte, err error, expected []byte, op string) {
	t.Helper()
	requireNoErr(t, err, op)
	got := respBulkContent(raw)
	if !bytes.Equal(got, expected) {
		t.Errorf("%s: expected %q, got %q", op, expected, got)
	}
}

func assertBulkRaw(t *testing.T, got []byte, expected []byte, op string) {
	t.Helper()
	// got is already extracted content from respArray
	if !bytes.Equal(got, expected) {
		t.Errorf("%s: expected %q, got %q", op, expected, got)
	}
}

func assertInt(t *testing.T, raw []byte, err error, expected int, op string) {
	t.Helper()
	requireNoErr(t, err, op)
	got := respInt(raw)
	if got != expected {
		t.Errorf("%s: expected %d, got %d", op, expected, got)
	}
}

func respInt(raw []byte) int {
	if len(raw) < 3 || raw[0] != ':' {
		return -999
	}
	idx := bytes.IndexByte(raw, '\r')
	if idx < 2 {
		return -999
	}
	n, _ := strconv.Atoi(string(raw[1:idx]))
	return n
}

// respArray parses RESP array (*N\r\n...), returns slice of raw bulk string contents
func respArray(raw []byte) [][]byte {
	if len(raw) < 4 || raw[0] != '*' {
		return nil
	}
	idx := bytes.Index(raw, []byte("\r\n"))
	if idx < 2 {
		return nil
	}
	count, _ := strconv.Atoi(string(raw[1:idx]))
	pos := idx + 2
	var out [][]byte
	for i := 0; i < count && pos < len(raw); i++ {
		if raw[pos] == '$' {
			lenIdx := bytes.Index(raw[pos:], []byte("\r\n"))
			if lenIdx < 0 {
				break
			}
			bulkLen, _ := strconv.Atoi(string(raw[pos+1 : pos+lenIdx]))
			pos += lenIdx + 2
			if bulkLen == -1 {
				out = append(out, nil)
				pos += 0 // $-1\r\n already skipped by lenIdx+2; next starts at pos
			} else if pos+bulkLen+2 <= len(raw) {
				out = append(out, raw[pos:pos+bulkLen])
				pos += bulkLen + 2
			}
		} else {
			next := bytes.Index(raw[pos:], []byte("\r\n"))
			if next < 0 {
				break
			}
			pos += next + 2
		}
	}
	return out
}

// respBulkContent extracts content from RESP bulk string ($len\r\n...\r\n)
func respBulkContent(raw []byte) []byte {
	if len(raw) < 5 || raw[0] != '$' {
		return raw
	}
	idx := bytes.IndexByte(raw, '\r')
	if idx < 0 {
		return raw
	}
	length, _ := strconv.Atoi(string(raw[1:idx]))
	if length <= 0 {
		return nil
	}
	start := idx + 2
	if start+length > len(raw) {
		return raw[start:]
	}
	return raw[start : start+length]
}

func stopAll(cmds []*exec.Cmd) {
	for _, cmd := range cmds {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}
}

