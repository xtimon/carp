//go:build integration

package integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/carp/internal/client"
)

// TestIntegration_ClientAuth_RequirePass starts a single node with
// CARP_REQUIREPASS set and verifies AUTH gating end-to-end through the real
// RESP listener.
func TestIntegration_ClientAuth_RequirePass(t *testing.T) {
	const (
		respPort   = 36479
		gossipPort = 37100
		rpcPort    = 37479
		password   = "secret-pw"
	)

	binPath := buildServerBinary(t, "carp-auth-test")
	dataDir := freshDataDir(t, "auth-node")
	respAddr := fmt.Sprintf("127.0.0.1:%d", respPort)
	hash := bcryptHash(t, password)

	startServer(t, binPath, []string{
		"NODE_ID=auth-node",
		fmt.Sprintf("PORT=%d", respPort),
		fmt.Sprintf("GOSSIP_PORT=%d", gossipPort),
		fmt.Sprintf("RPC_PORT=%d", rpcPort),
		"REPLICATION_FACTOR=1",
		"VNODES=8",
		"CLUSTER_NAME=carp-auth-test",
		"DIR=" + dataDir,
		"CARP_CLUSTER_SECRET=auth-test-secret",
		"CARP_REQUIREPASS=" + hash,
	}, respAddr)

	t.Run("set_without_auth_rejected", func(t *testing.T) {
		c := client.New([]string{respAddr})
		c.SetReplicationFactor(1)
		raw, err := c.Do("SET", []byte("k"), []byte("v"))
		if err != nil {
			t.Fatalf("SET: %v", err)
		}
		if !strings.Contains(string(raw), "NOAUTH") {
			t.Errorf("expected NOAUTH error, got %q", raw)
		}
	})

	t.Run("ping_allowed_without_auth", func(t *testing.T) {
		c := client.New([]string{respAddr})
		c.SetReplicationFactor(1)
		raw, err := c.Do("PING")
		if err != nil {
			t.Fatalf("PING: %v", err)
		}
		if !strings.Contains(string(raw), "PONG") {
			t.Errorf("expected PONG, got %q", raw)
		}
	})

	t.Run("auth_wrong_password_rejected", func(t *testing.T) {
		c := client.New([]string{respAddr})
		c.SetReplicationFactor(1)
		raw, err := c.Do("AUTH", []byte("wrong"))
		if err != nil {
			t.Fatalf("AUTH: %v", err)
		}
		if !strings.Contains(string(raw), "WRONGPASS") {
			t.Errorf("expected WRONGPASS, got %q", raw)
		}
	})

	t.Run("set_with_credentials_succeeds", func(t *testing.T) {
		c := client.New([]string{respAddr})
		c.SetReplicationFactor(1)
		c.SetCredentials("", password) // default user
		raw, err := c.Do("SET", []byte("k"), []byte("v"))
		if err != nil {
			t.Fatalf("SET: %v", err)
		}
		if !strings.Contains(string(raw), "OK") {
			t.Errorf("expected SET OK with credentials, got %q", raw)
		}
		raw, err = c.Do("GET", []byte("k"))
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		if !strings.Contains(string(raw), "v") {
			t.Errorf("expected GET v, got %q", raw)
		}
	})
}
