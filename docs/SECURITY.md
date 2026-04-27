# Security

CARP has three independent security surfaces. Each can be enabled on its own; existing deployments without any of them keep working unchanged.

| Surface | What it protects | Mechanism |
|---|---|---|
| Inter-node (RPC + gossip) | A stranger on the network can't write keys via internal RPC or join the ring as a phantom node | HMAC-SHA256 on every frame, shared `cluster_secret` |
| Client → server | A connecting client must present credentials before issuing data commands | Redis-style `AUTH` / `HELLO 2 AUTH` |
| Authorization (ACL) | Authenticated users can only run commands and touch keys their role permits | Per-user `role` + `keys` patterns |

> **Threat model.** v1 protects against unauthorized clients on the same network and unauthorized peers attempting to join the cluster. It does **not** provide transport encryption (TLS), replay protection on inter-node frames, or hinted handoff for missed writes. Deploy on a private network until those gaps are closed.

---

## Inter-node authentication (cluster secret)

Every RPC and gossip frame is authenticated with HMAC-SHA256 when `cluster_secret` is set. A peer without the secret (or with the wrong one) gets its connection silently dropped.

### Enable

```yaml
# carp.yaml on every node
cluster_secret: ${CARP_CLUSTER_SECRET}
```

…or via environment variable: `CARP_CLUSTER_SECRET=...`

All nodes in the cluster must share the same secret. Mixed mode (some nodes with, some without) does not work — the framing is incompatible.

### What it does

- Every RPC frame: `[4B body length][32B HMAC-SHA256(body)][body]`
- Every gossip frame: same wrapper around the JSON payload
- Frames over 64 MiB are rejected before any allocation
- HMAC mismatch → connection closed, no error reply

### Limitations

- **No replay protection.** A captured frame can be replayed by an attacker on the same network. Treat the cluster network as trusted.
- **No rotation tooling.** Changing the secret requires a coordinated rolling restart with the cluster briefly running both secrets — not yet automated.
- **No transport encryption.** Use a private subnet, Wireguard, or similar.

---

## Client authentication

### Single-password mode (Redis `requirepass` shortcut)

The simplest setup — one password for the implicit `default` user, full access:

```yaml
requirepass: "$2a$12$..."   # bcrypt hash, NOT the plaintext password
```

Or `CARP_REQUIREPASS` env var. Generate the hash with any bcrypt tool, e.g.:

```bash
htpasswd -bnBC 12 "" "your-password" | tr -d ':\n'
```

Clients then `AUTH <password>`:

```
$ redis-cli -p 6379
127.0.0.1:6379> SET k v
(error) NOAUTH Authentication required.
127.0.0.1:6379> AUTH your-password
OK
127.0.0.1:6379> SET k v
OK
```

In `requirepass` mode, the `default` user gets the `admin` role (full access). Use multi-user mode if you need anything more restrictive.

### Multi-user mode

Configure named users with explicit roles in YAML:

```yaml
auth:
  users:
    - name: default
      role: none                # locked when other users exist
    - name: ops
      password_hash: "$2a$12$..."
      role: admin
    - name: app1
      password_hash: "$2a$12$..."
      role: readwrite
      keys:
        - "app1:*"               # may only touch keys with this prefix
    - name: monitor
      password_hash: "$2a$12$..."
      role: readonly
```

Clients send `AUTH <user> <password>`:

```
127.0.0.1:6379> AUTH app1 app1-password
OK
127.0.0.1:6379> SET app1:foo bar
OK
127.0.0.1:6379> SET app2:foo bar
(error) NOPERM this user has no permissions to access one of the keys used as arguments
```

When using the bundled Go client (`carp-cli` / `carp-bench`), call `client.SetCredentials(user, pw)` — credentials are pipelined automatically on every connection.

### Default user policy (Redis-compatible)

| Configuration | `default` user behavior |
|---|---|
| No `requirepass`, no `auth.users` | Unauthenticated, full access (current "no auth" deployment) |
| `requirepass` only | Has the configured password, role `admin` |
| `auth.users` defined | Locked (`role: none`) unless explicitly granted |

This means turning on auth is opt-in. Existing clusters keep working until you add credentials.

### Pre-auth allowed commands

These commands are accepted from any client even without `AUTH`, so health probes and ring-aware client bootstrap keep working:

- `PING`, `AUTH`, `HELLO`, `QUIT`
- `INFO`
- `CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER RING`, `CLUSTER KEYSLOT`, `CLUSTER TOKEN`, `CLUSTER KEYNODE`

Mutating CLUSTER subcommands (`LEAVE`, `REPAIR`, `TOMBSTONE GC`) require auth and the `admin` role.

---

## Authorization (ACL)

### Roles

| Role | Allowed categories | Typical use |
|---|---|---|
| `admin` | `@read` + `@write` + `@admin` | Operators |
| `readwrite` | `@read` + `@write` (default if `role:` omitted) | Application instances |
| `readonly` | `@read` | Dashboards, monitors |
| `none` | nothing | Disabled / locked-down accounts |

### Command categories

Every command is in exactly one category. The full list is in [`internal/auth/acl.go`](../internal/auth/acl.go); the gist:

- `@read`: `GET`, `EXISTS`, `TTL`, `KEYS`, `MGET`, `LRANGE`, `HGETALL`, `ZRANGE`, `INFO`, `CLUSTER NODES`, `CLUSTER RING`, `CONFIG GET`, …
- `@write`: `SET`, `DEL`, `EXPIRE`, `INCR`, `LPUSH`, `SADD`, `HSET`, `ZADD`, …
- `@admin`: `FLUSHDB`, `SAVE`, `BGSAVE`, `SHUTDOWN`, `CLUSTER LEAVE`, `CLUSTER REPAIR`, `CLUSTER TOMBSTONE GC`, `CONFIG SET`, …

A role that doesn't allow the command's category gets:

```
(error) NOPERM this user has no permissions to run the 'flushdb' command
```

### Key-prefix scoping

A user's `keys` list restricts which keys they may touch. Two pattern shapes are supported in v1:

- `*` — matches every key (default when `keys` is omitted)
- `prefix*` — matches anything starting with `prefix`

Multiple patterns are OR-ed. Single-key commands check the first arg; `MSET` / `DEL` / `MGET` check every key argument; `KEYS` filters its result to keys the user is allowed to see (no leak of out-of-scope namespaces).

```yaml
keys:
  - "session:*"
  - "cache:public:*"
```

Out-of-scope key access is rejected with:

```
(error) NOPERM this user has no permissions to access one of the keys used as arguments
```

### Internal RPC bypasses ACL

ACL is enforced once at the coordinator (the node that received the client request). Inter-node RPC frames don't carry user identity — they're trusted because they're authenticated by the cluster secret. This keeps the hot path simple and means there's exactly one place to audit access decisions.

---

## Brute-force protection

Each connection has a per-conn AUTH attempt budget (default 5). Once exceeded, the server logs and closes the connection. A new TCP connection resets the counter — the limit is per-conn, not per-IP, so a client that fat-fingers once isn't penalized for the rest of its session.

```
[auth] AUTH failure for user="default" peer=10.0.0.5:54012 attempts=5
[auth] dropping 10.0.0.5:54012: 5 failed AUTH attempts
```

---

## Logging

Security-relevant events are logged at info level, prefixed for easy filtering:

| Prefix | Event |
|---|---|
| `[auth]` | AUTH success/failure, rate-limit drops |
| `[acl]` | Role denial (`reason=role`) and key denial (`reason=keys`) |
| `[server] Inter-node HMAC enabled` | Cluster secret active |
| `[server] Client AUTH required` | At least one user has a password configured |

Example denial line:

```
[acl] denied user="app1" cmd=SET key="app2:foo" reason=keys
```

---

## Out of scope (v1)

These are intentionally deferred — the deployment guide treats them as known gaps:

- **TLS** on the client port and between nodes
- **Hinted handoff** when a quorum write fails (manual `CLUSTER REPAIR` is the workaround)
- **Cluster-secret rotation** without a rolling restart
- **Runtime ACL changes** via an `ACL SETUSER` command (today: edit YAML, restart node)
- **Named namespaces** (multi-tenant `CARP NS USE …`) — use key-prefix scoping for now
- **Per-IP rate limits** — only per-connection AUTH limit exists today

---

## Quick reference

- Configuration keys: [CONFIGURATION.md](CONFIGURATION.md#authentication--access-control)
- AUTH / HELLO command syntax: [COMMANDS.md](COMMANDS.md#authentication)
- Multi-node deployment with secret: [DEPLOYMENT.md](DEPLOYMENT.md#production-authentication)
