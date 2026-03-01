# Deployment Guide

This document covers deploying CARP in development and production environments.

## Single Node (Development)

```bash
go build -o carp ./cmd/server
./carp --config config/single-node.yaml
```

Connect with `redis-cli -p 6379`.

## Cluster (Local Development)

1. Build: `make build`
2. Start each node in a separate terminal:

   ```bash
   ./carp --config config/node1.yaml   # Terminal 1
   ./carp --config config/node2.yaml   # Terminal 2
   ./carp --config config/node3.yaml   # Terminal 3
   ```

3. Verify: `redis-cli -p 6379 CLUSTER INFO`
4. Use ring-aware client: `./carp-cli -h 127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381`

## Docker

### Build

```bash
docker build -t carp .
```

### Run Single Node

```bash
docker run -d --name carp \
  -p 6379:6379 -p 7000:7000 -p 7379:7379 \
  -e HOST=0.0.0.0 \
  carp
```

### Run with Persistence

```bash
docker run -d --name carp \
  -p 6379:6379 -p 7000:7000 -p 7379:7379 \
  -e HOST=0.0.0.0 \
  -v carp-data:/app/data \
  -e DIR=/app/data \
  -e SAVE_INTERVAL=60 \
  carp
```

### Run Cluster with Docker Compose

Create `docker-compose.yaml`:

```yaml
version: "3.8"
services:
  carp1:
    build: .
    ports:
      - "6379:6379"
      - "7000:7000"
      - "7379:7379"
    environment:
      NODE_ID: node1
      HOST: carp1
      PORT: 6379
      GOSSIP_PORT: 7000
      RPC_PORT: 7379
      SEEDS: "carp2:7001 carp3:7002"
      REPLICATION_FACTOR: 3
    volumes:
      - carp1-data:/app/data

  carp2:
    build: .
    ports:
      - "6380:6379"
      - "7001:7000"
      - "7380:7379"
    environment:
      NODE_ID: node2
      HOST: carp2
      PORT: 6379
      GOSSIP_PORT: 7000
      RPC_PORT: 7379
      SEEDS: "carp1:7000 carp3:7002"
      REPLICATION_FACTOR: 3
    volumes:
      - carp2-data:/app/data

  carp3:
    build: .
    ports:
      - "6381:6379"
      - "7002:7000"
      - "7381:7379"
    environment:
      NODE_ID: node3
      HOST: carp3
      PORT: 6379
      GOSSIP_PORT: 7000
      RPC_PORT: 7379
      SEEDS: "carp1:7000 carp2:7001"
      REPLICATION_FACTOR: 3
    volumes:
      - carp3-data:/app/data

volumes:
  carp1-data:
  carp2-data:
  carp3-data:
```

Start: `docker-compose up -d`

## Production Considerations

### Ports

Expose these ports for each node:

| Port | Purpose | Required |
|------|---------|----------|
| 6379 (or custom) | Redis (RESP) | Yes |
| 7000 (or custom) | Gossip | Yes (for clusters) |
| 7379 (or custom) | RPC | Yes (for clusters) |

### Racks

For fault tolerance, assign nodes to logical racks (e.g., availability zones):

```yaml
rack: us-east-1a   # node1, node2
rack: us-east-1b   # node3, node4
rack: us-east-1c   # node5, node6
```

Replicas are spread across racks when `rack` is set.

### Persistence

- Set `dir` and `dbfilename` for RDB persistence.
- Set `save_interval` for periodic background saves (e.g., 60 seconds).
- Ensure sufficient disk space; RDB is a snapshot of all data.

### Seeds

- Configure at least one seed per node for initial discovery.
- Seeds use **gossip_port**, not Redis port (e.g., `127.0.0.1:7000`).
- More seeds improve bootstrap resilience.

### Replication Factor

- `RF=1`: No replication; single node owns each key.
- `RF=3`: Common for production; quorum = 2 acks.
- Higher RF improves durability but increases write load.

### Monitoring

- `CLUSTER INFO` and `CLUSTER NODES` for cluster health.
- `INFO` for server stats.
- Check logs for `Discovered`, `DOWN`, `Repair`, and errors.

### Graceful Shutdown

Use `CLUSTER LEAVE <nodename>` (connect to the node and run it, specifying that node's name) to gracefully leave the cluster before stopping the process. The node will save state and shut down.
