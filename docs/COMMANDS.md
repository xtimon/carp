# Redis Commands Reference

CARP supports a subset of Redis commands over the RESP2 protocol. Use `redis-cli` or `carp-cli` to interact with a CARP cluster.

## Consistency Levels

Before running commands, you can set per-connection consistency:

```
CONSISTENCY ONE     # Single replica (fastest, weak consistency)
CONSISTENCY QUORUM  # Quorum of replicas (default)
CONSISTENCY ALL     # All replicas must ack (strongest)
```

---

## Connection & Server

| Command | Description | Example |
|---------|-------------|---------|
| `PING` | Health check | `PING` |
| `HELLO` | Protocol handshake | `HELLO 2` |
| `QUIT` | Close connection | `QUIT` |
| `ECHO message` | Echo message | `ECHO hello` |
| `TIME` | Server time | `TIME` |
| `INFO` | Server info | `INFO` |
| `CONFIG GET key` | (Stub) Config value | `CONFIG GET port` |
| `DBSIZE` | Key count | `DBSIZE` |
| `FLUSHDB` | Delete all keys | `FLUSHDB` |
| `RANDOMKEY` | Random key | `RANDOMKEY` |

---

## Keys

| Command | Description | Example |
|---------|-------------|---------|
| `GET key` | Get string value | `GET mykey` |
| `SET key value` | Set string | `SET mykey value` |
| `SETEX key seconds value` | Set with TTL | `SETEX session 3600 data` |
| `SETNX key value` | Set if not exists | `SETNX lock 1` |
| `GETSET key value` | Get and set | `GETSET counter 0` |
| `DEL key [key ...]` | Delete keys | `DEL k1 k2` |
| `EXISTS key [key ...]` | Key exists | `EXISTS mykey` |
| `EXPIRE key seconds` | Set TTL | `EXPIRE key 60` |
| `TTL key` | Time to live | `TTL key` |
| `PERSIST key` | Remove TTL | `PERSIST key` |
| `KEYS pattern` | Match keys (glob) | `KEYS user:*` |
| `TYPE key` | Key type | `TYPE mykey` |
| `MSET k1 v1 k2 v2 ...` | Multi-set | `MSET a 1 b 2` |
| `MGET key [key ...]` | Multi-get | `MGET a b c` |

---

## Strings

| Command | Description | Example |
|---------|-------------|---------|
| `STRLEN key` | String length | `STRLEN mykey` |
| `APPEND key value` | Append to string | `APPEND log "\n"` |
| `GETRANGE key start end` | Substring | `GETRANGE s 0 4` |
| `SETRANGE key offset value` | Overwrite at offset | `SETRANGE s 0 x` |
| `INCR key` | Increment by 1 | `INCR counter` |
| `INCRBY key delta` | Increment by delta | `INCRBY counter 5` |
| `INCRBY key delta [idempotency_token]` | Idempotent increment (Netflix-style) | `INCRBY c 1 abc123` |
| `DECR key` | Decrement by 1 | `DECR counter` |
| `DECRBY key delta` | Decrement by delta | `DECRBY counter 5` |
| `DECRBY key delta [idempotency_token]` | Idempotent decrement | `DECRBY c 1 xyz789` |

---

## Lists

| Command | Description | Example |
|---------|-------------|---------|
| `LPUSH key value [value ...]` | Push left | `LPUSH list a b c` |
| `RPUSH key value [value ...]` | Push right | `RPUSH list x` |
| `LPOP key` | Pop left | `LPOP list` |
| `RPOP key` | Pop right | `RPOP list` |
| `LLEN key` | List length | `LLEN list` |
| `LRANGE key start stop` | Range (0-based) | `LRANGE list 0 -1` |
| `LINDEX key index` | Index | `LINDEX list 0` |
| `LSET key index value` | Set at index | `LSET list 0 x` |
| `LREM key count value` | Remove matches | `LREM list 0 x` |
| `LTRIM key start stop` | Trim list | `LTRIM list 0 99` |

---

## Sets

| Command | Description | Example |
|---------|-------------|---------|
| `SADD key member [member ...]` | Add members | `SADD tags a b c` |
| `SREM key member [member ...]` | Remove members | `SREM tags a` |
| `SISMEMBER key member` | Is member | `SISMEMBER tags a` |
| `SMEMBERS key` | All members | `SMEMBERS tags` |
| `SCARD key` | Set size | `SCARD tags` |
| `SPOP key` | Random pop | `SPOP tags` |

---

## Hashes

| Command | Description | Example |
|---------|-------------|---------|
| `HSET key field value` | Set field | `HSET user name John` |
| `HGET key field` | Get field | `HGET user name` |
| `HMSET key field value [field value ...]` | Multi-set | `HMSET user n John a 25` |
| `HMGET key field [field ...]` | Multi-get | `HMGET user n a` |
| `HGETALL key` | All fields/values | `HGETALL user` |
| `HDEL key field [field ...]` | Delete fields | `HDEL user tmp` |
| `HEXISTS key field` | Field exists | `HEXISTS user name` |
| `HLEN key` | Hash size | `HLEN user` |
| `HKEYS key` | All keys | `HKEYS user` |
| `HVALS key` | All values | `HVALS user` |

---

## Sorted Sets

| Command | Description | Example |
|---------|-------------|---------|
| `ZADD key score member [score member ...]` | Add/update | `ZADD rank 100 alice` |
| `ZREM key member [member ...]` | Remove | `ZREM rank alice` |
| `ZRANGE key start stop [WITHSCORES]` | Range (asc) | `ZRANGE rank 0 -1` |
| `ZRANK key member` | Rank (0-based asc) | `ZRANK rank alice` |
| `ZREVRANK key member` | Rank (desc) | `ZREVRANK rank alice` |
| `ZSCORE key member` | Score | `ZSCORE rank alice` |
| `ZCARD key` | Sorted set size | `ZCARD rank` |

---

## Cluster Management

| Command | Description | Example |
|---------|-------------|---------|
| `CLUSTER INFO` | Cluster summary | `CLUSTER INFO` |
| `CLUSTER NODES` | Node list | `CLUSTER NODES` |
| `CLUSTER RING` | Token ring (for clients) | `CLUSTER RING` |
| `CLUSTER KEYSLOT key` | Slot for key | `CLUSTER KEYSLOT mykey` |
| `CLUSTER LEAVE nodename` | Graceful leave (run on the node that's leaving; nodename must match this node) | `CLUSTER LEAVE node3` |
| `CLUSTER LEAVE FORCE nodename` | Remove dead node from cluster (run from any live node) | `CLUSTER LEAVE FORCE node3` |
| `CLUSTER REPAIR` | Restore replication | `CLUSTER REPAIR` |
| `CLUSTER REPAIR SMOOTH [delay_ms]` | Smooth repair (vnode by vnode) | `CLUSTER REPAIR SMOOTH 200` |
| `CLUSTER TOMBSTONE GC` | Purge expired tombstones | `CLUSTER TOMBSTONE GC` |

---

## Persistence

| Command | Description | Example |
|---------|-------------|---------|
| `SAVE` | Sync save to RDB | `SAVE` |
| `BGSAVE` | Background save | `BGSAVE` |

---

## Unsupported / Limitations

- No Lua scripting
- No pub/sub
- No transactions (MULTI/EXEC)
- No streams
- `KEYS` is O(N); avoid on large keyspaces in production
