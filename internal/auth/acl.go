package auth

import (
	"bytes"
	"strings"
)

// Category is the access class a command belongs to. The category, plus the
// user's Role, determines whether the command is allowed.
type Category int

const (
	// CatNoAuth is reserved for commands intercepted before Execute is reached
	// (PING, AUTH, HELLO, QUIT). Any command marked CatNoAuth and somehow
	// dispatched to ACL evaluation is treated as denied — fail closed.
	CatNoAuth Category = iota
	CatRead             // GET, EXISTS, KEYS, INFO, CLUSTER NODES, ...
	CatWrite            // SET, DEL, INCR, LPUSH, ZADD, ...
	CatAdmin            // CLUSTER LEAVE, FLUSHDB, BGSAVE, CONFIG, ...
)

// commandCategory returns the category for a given command. cmd is upper-case;
// args are the rest of the request and are used to disambiguate subcommands
// (e.g. CLUSTER NODES is read; CLUSTER LEAVE is admin).
func commandCategory(cmd string, args [][]byte) Category {
	if c, ok := readCmds[cmd]; ok {
		return c
	}
	if c, ok := writeCmds[cmd]; ok {
		return c
	}
	if c, ok := adminCmds[cmd]; ok {
		return c
	}
	if cmd == "CLUSTER" {
		if len(args) < 1 {
			return CatAdmin
		}
		switch strings.ToUpper(string(args[0])) {
		case "INFO", "NODES", "RING", "KEYSLOT", "TOKEN", "KEYNODE":
			return CatRead
		}
		return CatAdmin // LEAVE, REPAIR, TOMBSTONE, anything unknown → admin
	}
	if cmd == "CONFIG" {
		if len(args) >= 1 && strings.EqualFold(string(args[0]), "GET") {
			return CatRead
		}
		return CatAdmin
	}
	// Unknown command — let the coordinator's "unknown command" reply through.
	// Treating it as Read is safe since Execute will reject it anyway and we
	// don't want ACL to mask the real error message.
	return CatRead
}

var readCmds = map[string]Category{
	"GET": CatRead, "EXISTS": CatRead, "TTL": CatRead, "TYPE": CatRead,
	"STRLEN": CatRead, "GETRANGE": CatRead,
	"KEYS": CatRead, "MGET": CatRead, "DBSIZE": CatRead, "RANDOMKEY": CatRead,
	"INFO": CatRead, "ECHO": CatRead, "TIME": CatRead,
	"LLEN": CatRead, "LRANGE": CatRead, "LINDEX": CatRead,
	"SISMEMBER": CatRead, "SMEMBERS": CatRead, "SCARD": CatRead,
	"HGET": CatRead, "HMGET": CatRead, "HGETALL": CatRead, "HEXISTS": CatRead,
	"HLEN": CatRead, "HKEYS": CatRead, "HVALS": CatRead,
	"ZRANGE": CatRead, "ZRANK": CatRead, "ZREVRANK": CatRead,
	"ZSCORE": CatRead, "ZCARD": CatRead,
}

var writeCmds = map[string]Category{
	"SET": CatWrite, "SETEX": CatWrite, "SETNX": CatWrite, "GETSET": CatWrite,
	"APPEND": CatWrite, "SETRANGE": CatWrite,
	"INCR": CatWrite, "DECR": CatWrite, "INCRBY": CatWrite, "DECRBY": CatWrite,
	"DEL": CatWrite, "EXPIRE": CatWrite, "PERSIST": CatWrite,
	"MSET": CatWrite,
	"LPUSH": CatWrite, "RPUSH": CatWrite, "LPOP": CatWrite, "RPOP": CatWrite,
	"LSET": CatWrite, "LREM": CatWrite, "LTRIM": CatWrite,
	"SADD": CatWrite, "SREM": CatWrite, "SPOP": CatWrite,
	"HSET": CatWrite, "HMSET": CatWrite, "HDEL": CatWrite,
	"ZADD": CatWrite, "ZREM": CatWrite,
}

var adminCmds = map[string]Category{
	"FLUSHDB":  CatAdmin,
	"SAVE":     CatAdmin,
	"BGSAVE":   CatAdmin,
	"SHUTDOWN": CatAdmin,
}

// Role is a built-in policy bundle. Roles can be combined later (e.g. add
// per-user denies) but in v1 each user has exactly one role.
type Role int

const (
	RoleNone      Role = iota // explicit no-access; used for the locked default user
	RoleReadOnly              // @read
	RoleReadWrite             // @read + @write (NOT @admin)
	RoleAdmin                 // @read + @write + @admin
)

// ParseRole returns the role for a YAML string. Empty defaults to readwrite,
// which preserves the "any authenticated user can do data ops" semantic from
// Phase 2 when the operator hasn't yet adopted ACL.
func ParseRole(s string) (Role, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "readwrite":
		return RoleReadWrite, true
	case "admin":
		return RoleAdmin, true
	case "readonly":
		return RoleReadOnly, true
	case "none":
		return RoleNone, true
	}
	return RoleNone, false
}

func (r Role) allows(c Category) bool {
	switch c {
	case CatRead:
		return r == RoleReadOnly || r == RoleReadWrite || r == RoleAdmin
	case CatWrite:
		return r == RoleReadWrite || r == RoleAdmin
	case CatAdmin:
		return r == RoleAdmin
	}
	return false // CatNoAuth and anything else: fail closed
}

// KeyPattern is a compiled access pattern. v1 supports two shapes:
//   - "*" matches every key
//   - "<prefix>*" matches anything starting with prefix; trailing wildcard only
//
// More flexible glob/regex support is intentionally deferred — covers the 80%
// case (per-app prefix scoping) without dragging in a regex engine.
type KeyPattern struct {
	all    bool
	prefix []byte
}

// CompilePattern parses a single pattern string.
func CompilePattern(s string) KeyPattern {
	if s == "*" || s == "" {
		return KeyPattern{all: true}
	}
	if strings.HasSuffix(s, "*") {
		return KeyPattern{prefix: []byte(s[:len(s)-1])}
	}
	// Exact-match pattern: prefix == full key, no trailing wildcard. Treat
	// as a degenerate prefix (caller can still add an explicit "*" suffix).
	return KeyPattern{prefix: []byte(s)}
}

// Matches reports whether key satisfies the pattern.
func (p KeyPattern) Matches(key []byte) bool {
	if p.all {
		return true
	}
	if p.prefix == nil {
		return false
	}
	return bytes.HasPrefix(key, p.prefix)
}

// CompilePatterns compiles a slice of pattern strings; nil/empty slice yields
// nil (interpreted by AllowsKey as "all keys allowed" for non-restricted users).
func CompilePatterns(ss []string) []KeyPattern {
	if len(ss) == 0 {
		return nil
	}
	out := make([]KeyPattern, 0, len(ss))
	for _, s := range ss {
		out = append(out, CompilePattern(s))
	}
	return out
}
