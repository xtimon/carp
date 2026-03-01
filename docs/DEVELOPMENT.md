# Development Guide

This document covers building, testing, and developing CARP.

## Prerequisites

- **Go 1.21** or later
- Standard Go toolchain (`go build`, `go test`)

## Building

```bash
# All binaries
make build

# Or individually
go build -o carp ./cmd/server
go build -o carp-cli ./cmd/cli
go build -o carp-bench ./cmd/bench
```

### Binaries

| Binary | Purpose |
|--------|---------|
| `carp` | Server (cluster node) |
| `carp-cli` | Ring-aware Redis CLI |
| `carp-bench` | Ring-aware benchmark tool |

## Running

### Single Node

```bash
./carp --config config/single-node.yaml
# Then: redis-cli -p 6379
```

### 3-Node Cluster

```bash
# Terminal 1
./carp --config config/node1.yaml

# Terminal 2
./carp --config config/node2.yaml

# Terminal 3
./carp --config config/node3.yaml
```

Or use the helper:

```bash
make run-cluster   # Builds and prints instructions; use --start to launch node1
```

## Testing

### Unit Tests

```bash
make test
# or
go test ./...
```

### Tests with Coverage

```bash
make test-cover
# or
go test -cover ./...
```

### Integration Tests (6-Node Cluster)

```bash
make test-integration
# or
go test -tags=integration ./integration -v -timeout 90s
```

The integration test:

- Builds the server binary
- Starts 6 nodes (3 racks × 2 nodes) from `testdata/integration/node1.yaml`–`node6.yaml`
- Verifies cluster formation, rack metadata, SET/GET, and more
- Cleans up on exit

### Benchmarks

```bash
make bench
# or
go test -bench=. -benchmem ./internal/storage/... ./internal/coordinator/... ./internal/resp/...
```

## Project Structure

```
carp/
├── cmd/
│   ├── server/     # Server entry point
│   ├── cli/        # Ring-aware CLI
│   └── bench/      # Ring-aware benchmark
├── internal/
│   ├── client/     # Ring-aware client (failover, reconnection)
│   ├── resp/       # RESP2 protocol
│   ├── storage/    # Per-node KV engine
│   ├── partitioner/# Murmur3 consistent hashing, vnodes
│   ├── rebalance/  # Key migration on topology change
│   ├── cluster/    # Gossip membership
│   ├── rpc/        # Node-to-node protocol
│   └── coordinator/# Request routing, consistency
├── config/         # YAML configs
├── testdata/       # Test fixtures
├── integration/    # Integration tests
└── docs/           # Documentation
```

## Key Packages

### `internal/coordinator/`

- Routes Redis commands to replicas
- Implements consistency (ONE, QUORUM, ALL)
- Handles CLUSTER commands

### `internal/cluster/`

- Gossip-based membership
- Failure detection (15s threshold)
- Ring stability before partitioner updates

### `internal/partitioner/`

- Murmur3 token for keys
- Vnodes for distribution
- Rack-aware replica placement

### `internal/rebalance/`

- Orphan key detection
- Repair (push keys to replicas)
- Smooth repair (vnode-by-vnode)

### `internal/storage/`

- Strings, Lists, Sets, Hashes, Sorted Sets
- TTL, tombstones
- RDB persistence
- DumpKey/RestoreKey for migration

## Adding a New Command

1. **Storage**: Add handler in `internal/storage/` if it's a new data operation.
2. **RPC**: Add command code and handler in `internal/rpc/` for cross-node ops.
3. **Coordinator**: Add case in `internal/coordinator/coordinator.go` in `Execute()`.
4. **RESP**: Commands are parsed by `resp.Reader`; no change needed if using standard RESP arrays.
5. **Tests**: Add unit tests; update `docs/COMMANDS.md` and README.

## Code Style

- Use `go fmt` and `gofmt`-compatible style.
- Keep packages focused; avoid circular imports.
- Log with `log.Printf` for server events.

## Makefile Targets

| Target | Description |
|--------|-------------|
| `build` | Build server, CLI, bench |
| `test` | Run unit tests |
| `test-integration` | Run 6-node integration test |
| `test-cover` | Run tests with coverage |
| `bench` | Run Go benchmarks |
| `run` | Build and run single node |
| `run-cluster` | Build and print cluster instructions |
| `clean` | Remove binaries |
| `install` | Install to GOPATH/bin |
| `deps` | Download dependencies |
