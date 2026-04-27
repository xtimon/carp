//go:build integration

package integration

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/carp/internal/auth"
	"github.com/carp/internal/resp"
)

// TestIntegration_AuthRateLimit verifies that a single connection is dropped
// after auth.MaxFailedAuth wrong AUTH attempts.
func TestIntegration_AuthRateLimit(t *testing.T) {
	const (
		respPort   = 36679
		gossipPort = 37300
		rpcPort    = 37679
		password   = "right-password"
	)

	binPath := buildServerBinary(t, "carp-rl-test")
	dataDir := freshDataDir(t, "rl-node")
	respAddr := fmt.Sprintf("127.0.0.1:%d", respPort)

	startServer(t, binPath, []string{
		"NODE_ID=rl-node",
		fmt.Sprintf("PORT=%d", respPort),
		fmt.Sprintf("GOSSIP_PORT=%d", gossipPort),
		fmt.Sprintf("RPC_PORT=%d", rpcPort),
		"REPLICATION_FACTOR=1",
		"VNODES=8",
		"CLUSTER_NAME=carp-rl-test",
		"DIR=" + dataDir,
		"CARP_CLUSTER_SECRET=rl-test-secret",
		"CARP_REQUIREPASS=" + bcryptHash(t, password),
	}, respAddr)

	conn, err := net.Dial("tcp", respAddr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	rr := resp.NewResponseReader(conn)

	// Fire MaxFailedAuth wrong AUTHs; each should return WRONGPASS.
	for i := 0; i < auth.MaxFailedAuth; i++ {
		if _, err := conn.Write(resp.EncodeCommand("AUTH", []byte("wrong"))); err != nil {
			t.Fatalf("write attempt %d: %v", i, err)
		}
		raw, err := rr.ReadResponse()
		if err != nil {
			t.Fatalf("read attempt %d: %v", i, err)
		}
		if !strings.Contains(string(raw), "WRONGPASS") {
			t.Errorf("attempt %d: expected WRONGPASS, got %q", i, raw)
		}
	}

	// The next read should fail because the server has dropped the connection.
	// Write may succeed (TCP buffer) so we only check the read.
	conn.Write(resp.EncodeCommand("AUTH", []byte("wrong")))
	if _, err := rr.ReadResponse(); err == nil {
		t.Fatal("expected connection to be closed after MaxFailedAuth, but a response came back")
	}

	// A fresh connection should still be accepted (rate limit is per-conn).
	conn2, err := net.Dial("tcp", respAddr)
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer conn2.Close()
	conn2.SetDeadline(time.Now().Add(2 * time.Second))
	conn2.Write(resp.EncodeCommand("PING"))
	rr2 := resp.NewResponseReader(conn2)
	raw, err := rr2.ReadResponse()
	if err != nil {
		t.Fatalf("PING on fresh conn: %v", err)
	}
	if !strings.Contains(string(raw), "PONG") {
		t.Errorf("fresh conn PING expected PONG, got %q", raw)
	}
}
