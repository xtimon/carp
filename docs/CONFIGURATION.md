# Configuration Reference

CARP uses YAML configuration files with environment variable expansion. Configuration precedence: **defaults → file → environment variables** (env wins).

## Configuration File Format

### Full Example

```yaml
cluster_name: carp
node_id: node1
host: 127.0.0.1
rack: rack1
port: 6379
gossip_port: 7000
rpc_port: 7379
replication_factor: 3
vnodes: 64
seed_nodes:
  - host: 127.0.0.1
    gossip_port: 7000
  - host: 127.0.0.1
    gossip_port: 7001
  - host: 127.0.0.1
    gossip_port: 7002
dir: ./data
dbfilename: dump.rdb
save_interval: 60
tombstone_grace_seconds: 60
```

### Environment Variable Expansion

Config values support `${VAR}` and `${VAR:-default}`:

```yaml
port: ${PORT:-6379}
host: ${HOST:-127.0.0.1}
cluster_name: ${CLUSTER_NAME:-carp}
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `cluster_name` | string | `carp` | Cluster identifier. Nodes with different cluster names do not join the same ring. |
| `node_id` | string | `node1` | Unique identifier for this node. Must be unique across the cluster. |
| `host` | string | `127.0.0.1` | Bind address for Redis, Gossip, and RPC. Use `0.0.0.0` for all interfaces. |
| `bind_address` | string | (empty) | If set, overrides `host` for binding (legacy/env compatibility). |
| `rack` | string | (empty) | Logical grouping (e.g., datacenter rack, availability zone). Replicas are spread across racks when set. |
| `port` | int | 6379 | Redis (RESP) client port. |
| `gossip_port` | int | 7000 | Gossip protocol port for cluster membership. |
| `rpc_port` | int | 7379 | Internal RPC port for node-to-node storage operations. |
| `replication_factor` | int | 3 | Number of replicas per key. |
| `vnodes` | int | 64 | Virtual nodes per physical node. Higher values improve key distribution. |
| `seed_nodes` | list | `[]` | Bootstrap nodes for gossip. Each entry: `host`, `gossip_port`. |
| `dir` | string | `./data` | Data directory for RDB persistence. |
| `dbfilename` | string | `dump.rdb` | RDB filename inside `dir`. |
| `save_interval` | int | 0 | Auto-save interval in seconds. `0` = disabled. |
| `tombstone_grace_seconds` | int | 60 | Grace period before purging tombstones. `0` = no GC. |

## Environment Variables

These override config file values when set:

| Variable | Description |
|----------|-------------|
| `NODE_ID` | Node identifier |
| `HOST` | Bind address |
| `PORT` | Redis port |
| `GOSSIP_PORT` | Gossip port |
| `RPC_PORT` | RPC port |
| `RACK` | Rack/availability zone |
| `REPLICATION_FACTOR` | Replication factor |
| `VNODES` | Virtual nodes per node |
| `CLUSTER_NAME` | Cluster name |
| `SEEDS` | Override seeds: `host:gossip_port host:gossip_port` (space-separated) |
| `DIR` | Data directory |
| `DBFILENAME` | RDB filename |
| `SAVE_INTERVAL` | Auto-save interval (seconds) |
| `TOMBSTONE_GRACE_SECONDS` | Tombstone grace period |

## Example Configurations

### Single Node (Development)

```yaml
cluster_name: carp
node_id: node1
host: 127.0.0.1
port: 6379
gossip_port: 7000
rpc_port: 7379
replication_factor: 1
vnodes: 64
seed_nodes: []   # No seeds = standalone
dir: ./data
dbfilename: dump.rdb
```

### 3-Node Cluster (node1)

```yaml
cluster_name: carp
node_id: node1
host: 127.0.0.1
port: 6379
gossip_port: 7000
rpc_port: 7379
replication_factor: 3
vnodes: 64
seed_nodes:
  - host: 127.0.0.1
    gossip_port: 7000
  - host: 127.0.0.1
    gossip_port: 7001
  - host: 127.0.0.1
    gossip_port: 7002
```

### 6-Node Cluster with Racks (3 racks × 2 nodes)

```yaml
# node1.yaml (rack1)
node_id: node1
rack: rack1
port: 6379
gossip_port: 7000
# ... seeds include all 6 gossip ports

# node2.yaml (rack1)
node_id: node2
rack: rack1
port: 6380
gossip_port: 7001

# node3.yaml (rack2)
node_id: node3
rack: rack2
port: 6381
gossip_port: 7002
# ... etc.
```

## Docker

The Dockerfile uses `config/single-node.yaml`. Bind to all interfaces with:

```bash
docker run -p 6379:6379 -p 7000:7000 -p 7379:7379 -e HOST=0.0.0.0 carp
```
