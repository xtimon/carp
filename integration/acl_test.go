//go:build integration

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carp/internal/client"
	"gopkg.in/yaml.v3"
)

// TestIntegration_ACL_RolesAndKeyScoping spins up a single node with three
// users (admin, app1, viewer) defined inline via YAML and exercises:
//   - readonly user can GET but not SET
//   - readwrite user scoped to "app1:*" can SET in its prefix and is denied
//     elsewhere; KEYS returns only its prefix
//   - admin user can run admin commands
func TestIntegration_ACL_RolesAndKeyScoping(t *testing.T) {
	const (
		respPort   = 36579
		gossipPort = 37200
		rpcPort    = 37579
		adminPW    = "admin-pw"
		app1PW     = "app1-pw"
		viewerPW   = "viewer-pw"
	)

	binPath := buildServerBinary(t, "carp-acl-test")
	dataDir := freshDataDir(t, "acl-node")
	respAddr := fmt.Sprintf("127.0.0.1:%d", respPort)

	cfg := map[string]interface{}{
		"node_id":            "acl-node",
		"host":               "127.0.0.1",
		"port":               respPort,
		"gossip_port":        gossipPort,
		"rpc_port":           rpcPort,
		"replication_factor": 1,
		"vnodes":             8,
		"cluster_name":       "carp-acl-test",
		"dir":                dataDir,
		"auth": map[string]interface{}{
			"users": []map[string]interface{}{
				{"name": "admin", "password_hash": bcryptHash(t, adminPW), "role": "admin"},
				{"name": "app1", "password_hash": bcryptHash(t, app1PW), "role": "readwrite", "keys": []string{"app1:*"}},
				{"name": "viewer", "password_hash": bcryptHash(t, viewerPW), "role": "readonly"},
			},
		},
	}
	cfgPath := filepath.Join(dataDir, "config.yaml")
	cfgBytes, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(cfgPath, cfgBytes, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	startServerWithConfig(t, binPath, cfgPath, respAddr)

	clientFor := func(user, pw string) *client.Client {
		c := client.New([]string{respAddr})
		c.SetReplicationFactor(1)
		c.SetCredentials(user, pw)
		return c
	}

	// Seed some keys via admin so subsequent reads have something to find.
	admin := clientFor("admin", adminPW)
	for _, k := range []string{"app1:hello", "app1:greeting", "app2:secret", "shared"} {
		if _, err := admin.Do("SET", []byte(k), []byte("v")); err != nil {
			t.Fatalf("seed SET %s: %v", k, err)
		}
	}

	t.Run("readonly_can_get_cannot_set", func(t *testing.T) {
		c := clientFor("viewer", viewerPW)
		raw, _ := c.Do("GET", []byte("app1:hello"))
		if !strings.Contains(string(raw), "v") {
			t.Errorf("viewer GET expected 'v', got %q", raw)
		}
		raw, _ = c.Do("SET", []byte("any"), []byte("x"))
		if !strings.Contains(string(raw), "NOPERM") {
			t.Errorf("viewer SET expected NOPERM, got %q", raw)
		}
	})

	t.Run("scoped_user_writes_in_prefix_only", func(t *testing.T) {
		c := clientFor("app1", app1PW)
		raw, _ := c.Do("SET", []byte("app1:fresh"), []byte("v"))
		if !strings.Contains(string(raw), "OK") {
			t.Errorf("app1 SET in own prefix expected OK, got %q", raw)
		}
		raw, _ = c.Do("SET", []byte("app2:nope"), []byte("v"))
		if !strings.Contains(string(raw), "NOPERM") {
			t.Errorf("app1 SET outside prefix expected NOPERM, got %q", raw)
		}
		raw, _ = c.Do("GET", []byte("app2:secret"))
		if !strings.Contains(string(raw), "NOPERM") {
			t.Errorf("app1 GET outside prefix expected NOPERM, got %q", raw)
		}
	})

	t.Run("scoped_user_keys_filtered", func(t *testing.T) {
		c := clientFor("app1", app1PW)
		raw, err := c.Do("KEYS", []byte("*"))
		if err != nil {
			t.Fatalf("KEYS: %v", err)
		}
		if strings.Contains(string(raw), "app2:secret") || strings.Contains(string(raw), "shared") {
			t.Errorf("scoped KEYS leaked keys outside prefix: %q", raw)
		}
		if !strings.Contains(string(raw), "app1:") {
			t.Errorf("scoped KEYS expected app1:* keys, got %q", raw)
		}
	})

	t.Run("readwrite_cannot_run_admin", func(t *testing.T) {
		c := clientFor("app1", app1PW)
		raw, _ := c.Do("FLUSHDB")
		if !strings.Contains(string(raw), "NOPERM") {
			t.Errorf("app1 FLUSHDB expected NOPERM, got %q", raw)
		}
	})

	t.Run("admin_can_run_admin", func(t *testing.T) {
		// Don't actually FLUSHDB — verify with BGSAVE which is also @admin
		// but doesn't wipe state.
		c := clientFor("admin", adminPW)
		raw, _ := c.Do("BGSAVE")
		// Either "Background saving started" or "Background save disabled"
		// is fine — both mean we passed ACL.
		if strings.Contains(string(raw), "NOPERM") {
			t.Errorf("admin BGSAVE rejected by ACL: %q", raw)
		}
	})
}
