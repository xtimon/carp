# CARP

[![CI](https://github.com/xtimon/carp/actions/workflows/ci.yml/badge.svg)](https://github.com/xtimon/carp/actions/workflows/ci.yml)

**Cassandra Architecture for Redis Protocol** — A Redis-compatible key-value store built with Apache Cassandra-style architecture in Go.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/ARCHITECTURE.md) | System design, components, data flow |
| [Configuration](docs/CONFIGURATION.md) | Config options and environment variables |
| [Commands Reference](docs/COMMANDS.md) | Supported Redis commands |
| [Development](docs/DEVELOPMENT.md) | Build, test, project structure |
| [Deployment](docs/DEPLOYMENT.md) | Docker, production tips |
| [Contributing](CONTRIBUTING.md) | How to contribute |

## Features

- **Peer-to-peer cluster** (no master) with gossip-based membership
- **Consistent hashing** (Murmur3) with **virtual nodes (vnodes)** for better distribution
- **Rebalancing**: orphaned keys migrate to correct owners when topology changes
- **Replication** across nodes with configurable replication factor
- **Coordinator pattern**: any node can receive client requests and coordinate writes/reads
- **Full RESP2 protocol**: works with `redis-cli` and any Redis client

## Architecture (Cassandra-style)

| Cassandra Concept | Implementation |
|-------------------|----------------|
| **Default consistency** | QUORUM (quorum of replicas) when not specified |
| **Partitioning** | Murmur3 hash of key → token on ring; vnodes for even distribution |
| **Replication** | SimpleStrategy: RF replicas clockwise from token; rack-aware placement spreads replicas across racks |
| **Gossip** | Periodic state exchange; uses seeds and discovered peers as targets |
| **Coordinator** | Any node receiving a request coordinates to replicas |
| **No SPOF** | No master; all nodes are equal |

## Supported Redis Commands

### Connection & Server
- `PING`, `HELLO`, `QUIT`, `ECHO`, `TIME`
- `INFO`, `CONFIG GET` (stub)
- `DBSIZE`, `FLUSHDB`, `RANDOMKEY`

### Keys
- `GET`, `SET`, `SETEX`, `SETNX`, `GETSET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `PERSIST`, `KEYS`, `TYPE`
- `MSET`, `MGET`
- `CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER RING`, `CLUSTER KEYSLOT key`, `CLUSTER LEAVE <nodename>` (graceful), `CLUSTER LEAVE FORCE <nodename>` (remove dead node), `CLUSTER REPAIR` (restore replication), `CLUSTER REPAIR SMOOTH [delay_ms]` (smooth repair vnode by vnode), `CLUSTER TOMBSTONE GC` (purge expired tombstones)
### Strings
- `STRLEN`, `APPEND`, `GETRANGE`, `SETRANGE`, `INCR`, `INCRBY`, `DECR`, `DECRBY`
- **Netflix-style idempotency**: `INCRBY key delta [idempotency_token]` and `DECRBY key delta [idempotency_token]` support an optional 3rd argument. When provided, retries with the same token return the cached result without over-counting (safe retry/hedge as in [Netflix's Distributed Counter Abstraction](https://netflixtechblog.com/netflixs-distributed-counter-abstraction-8d0c45eb66b2)).

### Lists
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LTRIM`

### Sets
- `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`, `SPOP`

### Hashes
- `HSET`, `HGET`, `HMSET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`

### Sorted Sets
- `ZADD`, `ZREM`, `ZRANGE`, `ZRANK`, `ZREVRANK`, `ZSCORE`, `ZCARD`

### Performance

- **carp-bench**: CLI tool to benchmark a running cluster (`make build` then `./carp-bench -h 127.0.0.1:6379 -c 10 -n 100000`)
- **Go benchmarks**: `make bench` or `go test -bench=. -benchmem ./internal/storage/... ./internal/coordinator/... ./internal/resp/...` for unit-level performance tests

## Quick Start

### Build

```bash
go build -o carp ./cmd/server
```

### Single node

```bash
./carp --config config/single-node.yaml
```

Then connect: `redis-cli -p 6379`

### Three-node cluster

Start each node in a separate terminal:

```bash
# Terminal 1
./carp --config config/node1.yaml

# Terminal 2
./carp --config config/node2.yaml

# Terminal 3
./carp --config config/node3.yaml
```

You should see `Discovered ...` in the logs when nodes find each other. Connect with `redis-cli -p 6379` (or 6380, 6381). For **automatic failover** when a node dies (e.g. run `CLUSTER LEAVE FORCE node3` from a live node to remove it), use `carp-cli` instead—it will try other nodes when the connected one is down.

### Leaving the cluster

**Graceful leave** (node is still running): Connect to that node and run:

```bash
redis-cli -p 6381 CLUSTER LEAVE node3
```

The node broadcasts its departure to peers, then shuts down. (Use the node's own name as the argument.)

**Remove a dead node** (can't connect to it): Connect to any live node and run:

```bash
redis-cli -p 6379 CLUSTER LEAVE FORCE node3
```

This broadcasts the removal so all peers drop the dead node from the ring. After removing a node (especially with 6+ nodes and RF>1), run `CLUSTER REPAIR` to restore replication for under-replicated keys:

```bash
redis-cli -p 6379 CLUSTER REPAIR
```

This pushes keys from nodes that have them to replicas that should have them but don't. Run it on any live node; it triggers repair on all nodes.

For large clusters, use **smooth repair** to process keys vnode by vnode with a delay, avoiding load spikes:

```bash
redis-cli -p 6379 CLUSTER REPAIR SMOOTH
redis-cli -p 6379 CLUSTER REPAIR SMOOTH 200   # 200ms delay between vnodes
```

## Ring-Aware Client & CLI

The project includes a **ring-aware client** (`carp-cli`) that routes requests directly to the node owning the key (Cassandra-style), with automatic failover when nodes are down:

- **Failover**: If a node fails, the client tries other nodes and refreshes the ring from them
- **Single-seed resilience**: Even with one seed (e.g. `-h 127.0.0.1:6379`), failover works once the ring has been fetched (the client remembers all nodes)

```bash
# Build the CLI
go build -o carp-cli ./cmd/cli

# Single seed (default: 127.0.0.1:6379)
./carp-cli SET mykey value
./carp-cli GET mykey

# Multiple seeds for resilience
./carp-cli -h 127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381 GET mykey

# Interactive mode (no args)
./carp-cli
```

### Ring-Aware Benchmark (`carp-bench`)

```bash
go build -o carp-bench ./cmd/bench

# Default: PING, SET, GET, INCR, LPUSH, LPOP, SADD, SPOP, HSET, HGET, ZADD, ZRANGE
./carp-bench -h 127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381

# Quick test
./carp-bench -h 127.0.0.1:6379 -n 10000 -t set,get -q

# With consistency level
./carp-bench -h 127.0.0.1:6379 -C ONE -n 50000 -t set,get
```

| Flag | Default | Description |
|------|---------|--------------|
| `-h` | 127.0.0.1:6379 | Seed addresses (comma-separated) |
| `-c` | 50 | Parallel clients |
| `-n` | 100000 | Total requests |
| `-d` | 3 | Data size (bytes) for SET/GET |
| `-r` | 100000 | Random keyspace size |
| `-t` | ping,set,get,... | Comma-separated tests |
| `-C` | (server default) | Consistency: ONE, QUORUM, ALL |
| `-q` | false | Quiet (throughput only) |
| `-D` | - | Run for duration (e.g. 30s) |

For local clusters, use `-c 10` (default) to avoid exhausting ephemeral ports ("can't assign requested address"). Use `-c 50` or higher only when nodes run on different hosts.

**Features:**
- **Direct routing**: Key-based commands (GET, SET, etc.) go straight to the primary replica
- **Replica fallback**: If the primary is down, tries other replicas
- **Ring refresh**: Refetches cluster topology on failure and retries
- **Reconnection**: Retries with exponential backoff on connection errors
- **Consistency levels**: Session-level ONE, QUORUM (default), or ALL:

```bash
# Via flag
./carp-cli -c ONE GET mykey

# Interactive: CONSISTENCY ONE then commands
./carp-cli
127.0.0.1:6379> CONSISTENCY ONE
OK
127.0.0.1:6379> GET mykey
```

The server also accepts `CONSISTENCY ONE|QUORUM|ALL` (e.g. via redis-cli) for per-connection override.

## Project Structure

```
cmd/
  server/main.go     # Server entry point
  cli/main.go       # Ring-aware CLI
  bench/main.go     # Ring-aware benchmark
internal/
  client/           # Ring-aware client with reconnection
  resp/             # RESP2 protocol
  storage/          # Per-node KV engine (with DumpKey/RestoreKey for migration)
  partitioner/      # Murmur3 consistent hashing with vnodes
  rebalance/        # Migrates orphaned keys when nodes join/leave
  cluster/          # Gossip membership
  rpc/              # Node-to-node protocol
  coordinator/      # Request routing
config/
  single-node.yaml
  node1.yaml, node2.yaml, node3.yaml
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | node1 | Unique node identifier |
| `HOST` | 127.0.0.1 | Bind address |
| `PORT` | 6379 | Redis (RESP) port |
| `GOSSIP_PORT` | 7000 | Gossip protocol port |
| `RPC_PORT` | 7379 | Internal RPC port |
| `RACK` | - | Logical grouping (e.g., datacenter rack, availability zone). Replicas are spread across racks for fault tolerance. |
| `REPLICATION_FACTOR` | 3 | Replicas per key |
| `VNODES` | 64 | Virtual nodes per physical node (better key distribution) |
| `CLUSTER_NAME` | carp | Cluster name (shown in CLUSTER INFO). Nodes with different cluster names will not join the same ring. |
| `SEEDS` | - | Override seeds: `host:gossip_port host:gossip_port`. Gossip also uses discovered peers as targets, so a node with no seeds can still join if it receives an inbound gossip connection. |

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details. Compatible with Apache Cassandra and other Apache projects.
