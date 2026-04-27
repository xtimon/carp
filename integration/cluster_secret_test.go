//go:build integration

package integration

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/carp/internal/client"
	"github.com/carp/internal/clusterauth"
)

// TestIntegration_ClusterSecret_RejectsUnauthenticated brings up a single
// node with CARP_CLUSTER_SECRET set and proves that a rogue connection
// without the secret cannot speak RPC or gossip to it.
func TestIntegration_ClusterSecret_RejectsUnauthenticated(t *testing.T) {
	const (
		respPort   = 36379
		gossipPort = 37000
		rpcPort    = 37379
		secret     = "integration-test-secret"
	)

	binPath := buildServerBinary(t, "carp-secret-test")
	dataDir := freshDataDir(t, "secret-node")
	respAddr := fmt.Sprintf("127.0.0.1:%d", respPort)

	const password = "secret-test-pw"
	startServer(t, binPath, []string{
		"NODE_ID=secret-node",
		fmt.Sprintf("PORT=%d", respPort),
		fmt.Sprintf("GOSSIP_PORT=%d", gossipPort),
		fmt.Sprintf("RPC_PORT=%d", rpcPort),
		"REPLICATION_FACTOR=1",
		"VNODES=8",
		"CLUSTER_NAME=carp-secret-test",
		"DIR=" + dataDir,
		"CARP_CLUSTER_SECRET=" + secret,
		"CARP_REQUIREPASS=" + bcryptHash(t, password),
	}, respAddr)
	// Give RPC + gossip ports a moment too (RESP coming up is the readiness signal).
	time.Sleep(300 * time.Millisecond)

	// 1. Sanity: authenticated client RESP path still works.
	c := client.New([]string{respAddr})
	c.SetReplicationFactor(1)
	c.SetCredentials("default", password)
	if _, err := c.Do("SET", []byte("k"), []byte("v")); err != nil {
		t.Fatalf("RESP SET should succeed: %v", err)
	}

	// 2. Rogue RPC with NO secret framing must be dropped.
	t.Run("rogue_rpc_no_secret", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", rpcPort), 2*time.Second)
		if err != nil {
			t.Fatalf("dial RPC: %v", err)
		}
		defer conn.Close()
		// Send a legacy-format RPC frame (no HMAC); server is in HMAC mode and
		// should treat the bytes as MAC + body, then fail the MAC check.
		body := []byte{0x01, 0x00, 0x00} // CmdGet, 0 args
		legacyFrame := make([]byte, 4+len(body))
		binary.BigEndian.PutUint32(legacyFrame[0:4], uint32(len(body)))
		copy(legacyFrame[4:], body)
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write(legacyFrame); err != nil {
			t.Fatalf("write: %v", err)
		}
		buf := make([]byte, 4)
		if _, err := conn.Read(buf); err == nil {
			t.Fatal("expected rogue RPC to be dropped, got a response")
		}
	})

	// 3. Rogue RPC with WRONG secret must be dropped.
	t.Run("rogue_rpc_wrong_secret", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", rpcPort), 2*time.Second)
		if err != nil {
			t.Fatalf("dial RPC: %v", err)
		}
		defer conn.Close()
		body := []byte{0x01, 0x00, 0x00}
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		if err := clusterauth.WriteFrame(conn, []byte("wrong-secret"), body); err != nil {
			t.Fatalf("write frame: %v", err)
		}
		buf := make([]byte, 4)
		if _, err := conn.Read(buf); err == nil {
			t.Fatal("expected rogue RPC to be dropped, got a response")
		}
	})

	// 4. Rogue gossip with NO secret must be dropped.
	t.Run("rogue_gossip_no_secret", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", gossipPort), 2*time.Second)
		if err != nil {
			t.Fatalf("dial gossip: %v", err)
		}
		defer conn.Close()
		payload := []byte(`{"from":"attacker","cluster_name":"carp-secret-test","nodes":[]}`)
		legacyFrame := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(legacyFrame[0:4], uint32(len(payload)))
		copy(legacyFrame[4:], payload)
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write(legacyFrame); err != nil {
			t.Fatalf("write: %v", err)
		}
		buf := make([]byte, 4)
		if _, err := conn.Read(buf); err == nil {
			t.Fatal("expected rogue gossip to be dropped, got a response")
		}
	})
}
