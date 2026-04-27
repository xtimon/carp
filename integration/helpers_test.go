//go:build integration

package integration

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// projectRoot returns the repo root (the dir containing go.mod) discovered
// from the test's cwd. Tests run with cwd = integration/ when invoked via
// `go test ./integration/...`, so we walk up one level if needed.
func projectRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cwd, "go.mod")); err == nil {
		return cwd
	}
	parent := filepath.Dir(cwd)
	if _, err := os.Stat(filepath.Join(parent, "go.mod")); err == nil {
		return parent
	}
	t.Fatalf("cannot find project root from %s", cwd)
	return ""
}

// buildServerBinary builds cmd/server into projectRoot/<name>. Cleans up the
// binary on test completion. Each test passes a unique name so binaries don't
// collide if go test runs them in parallel.
func buildServerBinary(t *testing.T, name string) string {
	t.Helper()
	root := projectRoot(t)
	out := filepath.Join(root, name)
	cmd := exec.Command("go", "build", "-o", out, "./cmd/server")
	cmd.Dir = root
	if msg, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, msg)
	}
	t.Cleanup(func() { os.Remove(out) })
	return out
}

// freshDataDir creates a clean testdata/integration/data/<name> dir and
// registers it for cleanup.
func freshDataDir(t *testing.T, name string) string {
	t.Helper()
	root := projectRoot(t)
	dir := filepath.Join(root, "testdata", "integration", "data", name)
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// startServer launches the carp server with the given env-var overrides and
// waits for the RESP port to accept connections. The process is killed on
// test cleanup.
func startServer(t *testing.T, binPath string, env []string, respAddr string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(binPath)
	cmd.Dir = projectRoot(t)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	if err := waitForPort(respAddr, 5*time.Second); err != nil {
		t.Fatalf("server never came up at %s: %v", respAddr, err)
	}
	return cmd
}

// startServerWithConfig is like startServer but uses --config <path> instead
// of env vars. Used by tests that need YAML structures the env layer doesn't
// expose (auth.users, seed_nodes).
func startServerWithConfig(t *testing.T, binPath, configPath, respAddr string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(binPath, "--config", configPath)
	cmd.Dir = projectRoot(t)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	if err := waitForPort(respAddr, 5*time.Second); err != nil {
		t.Fatalf("server never came up at %s: %v", respAddr, err)
	}
	return cmd
}

func waitForPort(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("port %s not ready within %v", addr, timeout)
}

// bcryptHash returns the bcrypt hash of pw at MinCost (fast for tests).
func bcryptHash(t *testing.T, pw string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	return string(h)
}
