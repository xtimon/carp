# CARP Architecture

This document describes the internal architecture of CARP (Cassandra Architecture for Redis Protocol), a distributed key-value store that combines Redis protocol compatibility with Apache Cassandra-style distributed systems design.

## Overview

CARP is a **peer-to-peer** distributed system with **no single point of failure**. Every node can:

- Accept client connections (RESP/Redis protocol)
- Coordinate reads and writes to replicas
- Participate in gossip-based cluster membership
- Own a portion of the key space via consistent hashing

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Layer                                   │
│  redis-cli / carp-cli / any Redis client (RESP2 protocol)                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Coordinator (any node)                              │
│  - Parses RESP commands                                                     │
│  - Routes to correct replica(s) via partitioner                             │
│  - Enforces consistency level (ONE, QUORUM, ALL)                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
┌───────────────────────┐ ┌────────────────────────┐ ┌────────────────────────┐
│    Gossip Protocol    │ │    Partitioner         │ │    RPC (node-to-node)  │
│  - Membership         │ │  - Murmur3 hashing     │ │  - Storage ops         │
│  - Failure detection  │ │  - Virtual nodes       │ │  - Repair coordination │
│  - Ring topology      │ │  - Rack-aware replicas │ │  - Migration           │
└───────────────────────┘ └────────────────────────┘ └────────────────────────┘
                    │                 │
                    └────────┬────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Local Storage (per node)                            │
│  - Strings, Lists, Sets, Hashes, Sorted Sets                                │
│  - TTL / Expiration                                                         │
│  - RDB persistence (Redis-style dump.rdb)                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Gossip Protocol (`internal/cluster/`)

Cassandra-inspired membership and failure detection:

- **EndpointState** per node: `NodeID`, `Host`, `Port`, `GossipPort`, `State`, `Generation`, `Heartbeat`, `Rack`
- **Generation**: Startup timestamp; distinguishes restarts from the same node
- **Heartbeat**: Monotonic version; higher = newer; merge uses `(gen, ver)` for conflict resolution
- **Failure detection**: Node marked DOWN if no successful gossip for 15+ seconds
- **Gossip targets**: Random live node, random unreachable (probabilistic), random seed
- **Ring stability**: Partitioner updated only when ring unchanged for 2 consecutive rounds

Nodes never remove themselves from state; `CLUSTER LEAVE <nodename>` broadcasts departure and the node shuts down.

### 2. Partitioner (`internal/partitioner/`)

- **Hash function**: Murmur3 32-bit (Cassandra-compatible)
- **Token range**: Key → `TokenForKey(key)` → token on ring
- **Virtual nodes (vnodes)**: Each physical node owns N vnodes for even distribution (default 64–256)
- **Replica placement**: RF replicas clockwise from primary; rack-aware when racks are configured (replicas spread across racks)

### 3. Coordinator (`internal/coordinator/`)

- Receives RESP commands from clients
- For key-based commands: computes replicas via partitioner, routes to correct node(s)
- **Consistency levels**:
  - **ONE**: Single replica acknowledgment
  - **QUORUM**: ⌊RF/2⌋ + 1 replicas (default)
  - **ALL**: All RF replicas must acknowledge
- Fan-out to replicas via RPC; aggregates responses per consistency

### 4. RPC (`internal/rpc/`)

Binary protocol for node-to-node communication:

- Connection pooling per address (limits port exhaustion)
- Command codes for storage ops (Get, Set, Del, Exists, LPush, etc.)
- Used for cross-node reads/writes and repair/migration

### 5. Rebalance (`internal/rebalance/`)

When topology changes (node join/leave):

- **Orphaned keys**: Keys whose primary replica moved; local node may still hold them
- **Repair**: Pushes keys from owning nodes to replicas that should have them
- **Smooth repair**: Processes vnodes one by one with configurable delay to avoid load spikes

### 6. Storage (`internal/storage/`)

Per-node key-value engine:

- **Data structures**: Strings, Lists, Sets, Hashes, Sorted Sets
- **TTL**: Expiration with tombstone cleanup
- **Persistence**: RDB format (Redis-compatible dump/load)
- **Migration**: `DumpKey`/`RestoreKey` for repair

## Data Flow

### Write (e.g., SET key value)

1. Client sends `SET key value` to any node
2. Coordinator hashes `key` → token; partitioner returns RF replicas
3. Coordinator sends RPC to all replicas
4. Waits for required acks (ONE/QUORUM/ALL)
5. Returns OK to client

### Read (e.g., GET key)

1. Client sends `GET key` to any node
2. Coordinator hashes `key` → token; partitioner returns replicas
3. Coordinator reads from enough replicas per consistency (e.g., QUORUM = 2 of 3)
4. Returns value (or nil) to client

### Ring-Aware Client (`carp-cli`)

- Fetches `CLUSTER RING` from seeds
- Routes key-based commands **directly** to the primary replica
- On failure: retries other replicas, refreshes ring, reconnects
- Reduces coordinator load and latency

## Ports

| Port Type   | Default | Purpose                          |
|-------------|---------|----------------------------------|
| Redis (RESP)| 6379    | Client connections               |
| Gossip      | 7000    | Cluster membership protocol      |
| RPC         | 7379    | Internal node-to-node storage ops|

## Failure Modes and Recovery

- **Node crash**: Gossip marks it DOWN; replicas serve reads/writes; `CLUSTER REPAIR` restores under-replicated keys
- **Graceful leave**: `CLUSTER LEAVE <nodename>` (run on the leaving node) broadcasts departure; node shuts down; peers update ring
- **Dead node removal**: `CLUSTER LEAVE FORCE node3` (from any live node) removes `node3` from ring; then run `CLUSTER REPAIR`

## References

- [Apache Cassandra Architecture](https://cassandra.apache.org/doc/latest/architecture/)
- [Netflix Distributed Counter Abstraction](https://netflixtechblog.com/netflixs-distributed-counter-abstraction-8d0c45eb66b2) (INCRBY idempotency)
