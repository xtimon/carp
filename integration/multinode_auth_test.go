//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carp/internal/client"
	"gopkg.in/yaml.v3"
)

// TestIntegration_MultiNodeAuthAndSecret brings up a 3-node cluster with
// BOTH cluster_secret and auth.users configured. This is the realistic
// production deployment shape and exercises:
//
//   - Cluster convergence with gossip-over-HMAC (proves gossip handshake
//     works when both sides have the same secret).
//   - Cross-node RPC with HMAC (replication factor = 3 forces every write
//     to call into the other two nodes via internal RPC).
//   - ACL consistency across nodes — a scoped user's denial fires on
//     whichever node happens to coordinate the request.
func TestIntegration_MultiNodeAuthAndSecret(t *testing.T) {
	const (
		secret      = "multi-node-secret"
		clusterName = "carp-multinode-test"
		adminPW     = "multi-admin-pw"
		appPW       = "multi-app-pw"
		convergeWaitFor = 25 * time.Second
		convergePoll    = 500 * time.Millisecond
	)

	type nodeSpec struct {
		id         string
		respPort   int
		gossipPort int
		rpcPort    int
	}
	nodes := []nodeSpec{
		{"mn1", 36879, 37500, 37879},
		{"mn2", 36880, 37501, 37880},
		{"mn3", 36881, 37502, 37881},
	}

	binPath := buildServerBinary(t, "carp-multinode-test")
	adminHash := bcryptHash(t, adminPW)
	appHash := bcryptHash(t, appPW)

	seedNodes := make([]map[string]interface{}, 0, len(nodes))
	for _, n := range nodes {
		seedNodes = append(seedNodes, map[string]interface{}{
			"host":        "127.0.0.1",
			"gossip_port": n.gossipPort,
		})
	}

	for _, n := range nodes {
		dir := freshDataDir(t, "multinode-"+n.id)
		cfg := map[string]interface{}{
			"node_id":            n.id,
			"host":               "127.0.0.1",
			"port":               n.respPort,
			"gossip_port":        n.gossipPort,
			"rpc_port":           n.rpcPort,
			"replication_factor": 3,
			"vnodes":             16,
			"cluster_name":       clusterName,
			"cluster_secret":     secret,
			"dir":                dir,
			"seed_nodes":         seedNodes,
			"auth": map[string]interface{}{
				"users": []map[string]interface{}{
					{"name": "admin", "password_hash": adminHash, "role": "admin"},
					{"name": "app", "password_hash": appHash, "role": "readwrite", "keys": []string{"app:*"}},
				},
			},
		}
		cfgBytes, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshal %s: %v", n.id, err)
		}
		cfgPath := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(cfgPath, cfgBytes, 0644); err != nil {
			t.Fatalf("write config %s: %v", n.id, err)
		}
		respAddr := fmt.Sprintf("127.0.0.1:%d", n.respPort)
		startServerWithConfig(t, binPath, cfgPath, respAddr)
	}

	// Wait for the cluster to converge. CLUSTER INFO is pre-auth, so the
	// client doesn't need credentials yet.
	seedAddrs := make([]string, len(nodes))
	for i, n := range nodes {
		seedAddrs[i] = fmt.Sprintf("127.0.0.1:%d", n.respPort)
	}

	deadline := time.Now().Add(convergeWaitFor)
	var infoClient *client.Client
	for time.Now().Before(deadline) {
		infoClient = client.New(seedAddrs)
		infoClient.SetReplicationFactor(3)
		info, err := infoClient.Do("CLUSTER", []byte("INFO"))
		if err == nil && bytes.Contains(info, []byte(fmt.Sprintf("cluster_nodes:%d", len(nodes)))) {
			break
		}
		time.Sleep(convergePoll)
	}
	info, _ := infoClient.Do("CLUSTER", []byte("INFO"))
	if !bytes.Contains(info, []byte(fmt.Sprintf("cluster_nodes:%d", len(nodes)))) {
		t.Fatalf("cluster did not converge in %v: %q", convergeWaitFor, info)
	}

	// Allow the partitioner to settle (existing 6-node test does the same —
	// gossip needs two stable rounds before pushing the ring to peers).
	time.Sleep(5 * time.Second)
	if err := infoClient.RefreshRing(); err != nil {
		t.Fatalf("RefreshRing: %v", err)
	}

	clientFor := func(user, pw string) *client.Client {
		c := client.New(seedAddrs)
		c.SetReplicationFactor(3)
		c.SetCredentials(user, pw)
		return c
	}

	t.Run("admin_replicates_across_nodes", func(t *testing.T) {
		c := clientFor("admin", adminPW)
		// Each SET goes to all 3 replicas via inter-node RPC. If HMAC framing
		// were broken between nodes, replication would fail and we'd see
		// "replication failed".
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("admin:k%d", i)
			val := fmt.Sprintf("v%d", i)
			raw, err := c.Do("SET", []byte(key), []byte(val))
			if err != nil {
				t.Fatalf("SET %s: %v", key, err)
			}
			if !strings.Contains(string(raw), "OK") {
				t.Errorf("SET %s expected OK, got %q", key, raw)
			}
			raw, err = c.Do("GET", []byte(key))
			if err != nil {
				t.Fatalf("GET %s: %v", key, err)
			}
			if !strings.Contains(string(raw), val) {
				t.Errorf("GET %s expected %q, got %q", key, val, raw)
			}
		}
	})

	t.Run("scoped_user_works_anywhere_in_ring", func(t *testing.T) {
		c := clientFor("app", appPW)
		// Use varied keys so we hit different vnodes / coordinators. The ACL
		// check must run wherever the request lands.
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("app:k%d", i)
			raw, err := c.Do("SET", []byte(key), []byte("v"))
			if err != nil {
				t.Fatalf("SET %s: %v", key, err)
			}
			if !strings.Contains(string(raw), "OK") {
				t.Errorf("scoped SET %s expected OK, got %q", key, raw)
			}
		}
	})

	t.Run("scoped_user_denied_outside_prefix_anywhere", func(t *testing.T) {
		c := clientFor("app", appPW)
		// "other:*" keys hash to different vnodes than "app:*" — at least
		// some will land on a different coordinator than the connection
		// owner, proving ACL fires regardless of routing.
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("other:k%d", i)
			raw, _ := c.Do("SET", []byte(key), []byte("v"))
			if !strings.Contains(string(raw), "NOPERM") {
				t.Errorf("scoped SET %s expected NOPERM, got %q", key, raw)
			}
		}
	})
}
