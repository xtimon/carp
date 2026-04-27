// Package auth holds client-facing authentication state: the user registry
// and per-connection sessions. ACL policy (roles, key prefixes) lands in
// Phase 3; this file is just identity + bcrypt verification.
package auth

import (
	"errors"
	"sync"

	"golang.org/x/crypto/bcrypt"
)

// User is a configured principal. PasswordHash is bcrypt; an empty hash plus
// NoPass=true marks an unauthenticated user (only allowed for `default` when
// no other user has a password configured).
type User struct {
	Name         string
	PasswordHash string
	NoPass       bool
	Role         Role
	Keys         []KeyPattern // empty = all keys (admin/readwrite/readonly default)
}

// Verify checks pw against the bcrypt hash. NoPass users accept anything.
func (u *User) Verify(pw []byte) bool {
	if u.NoPass {
		return true
	}
	if u.PasswordHash == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), pw) == nil
}

// AllowsCommand reports whether the user's role permits this command. Pass
// cmd in upper-case and the original args (used for CLUSTER/CONFIG subcommand
// disambiguation).
func (u *User) AllowsCommand(cmd string, args [][]byte) bool {
	if u == nil {
		return false
	}
	return u.Role.allows(commandCategory(cmd, args))
}

// AllowsKey reports whether the user is permitted to access this key.
// Empty Keys list = all keys allowed (the common case for unrestricted users).
func (u *User) AllowsKey(key []byte) bool {
	if u == nil {
		return false
	}
	if len(u.Keys) == 0 {
		return true
	}
	for _, p := range u.Keys {
		if p.Matches(key) {
			return true
		}
	}
	return false
}

// FilterKeys returns the subset of keys the user is allowed to see. Used for
// KEYS to ensure scoped users only get their own key namespace back.
func (u *User) FilterKeys(keys [][]byte) [][]byte {
	if u == nil {
		return nil
	}
	if len(u.Keys) == 0 {
		return keys
	}
	out := keys[:0]
	for _, k := range keys {
		if u.AllowsKey(k) {
			out = append(out, k)
		}
	}
	return out
}

// Registry is the live user table. It's read-mostly; writes only happen at
// startup, but we keep the lock for symmetry with future ACL reload work.
type Registry struct {
	mu    sync.RWMutex
	users map[string]*User
}

// NewRegistry builds a registry from the configured users. The `default`
// user is always present; if the caller didn't supply one, it is created
// with no password and no role (locked) so unauthenticated callers can't
// fall through to it. Caller is responsible for ensuring at least one user
// has a password — that's a server-startup concern, not a registry concern.
func NewRegistry(users []User) *Registry {
	r := &Registry{users: make(map[string]*User)}
	for i := range users {
		u := users[i]
		r.users[u.Name] = &u
	}
	if _, ok := r.users["default"]; !ok {
		r.users["default"] = &User{Name: "default", Role: RoleNone}
	}
	return r
}

// GuestUser returns a minimal-privilege synthetic user used by the RESP
// dispatcher for pre-auth-allowed commands (PING, INFO, CLUSTER metadata).
// The wire-level allowlist in handleRESP already restricts what this user
// can submit — Role=Admin here is fine because it never sees a write.
func GuestUser() *User {
	return &User{Name: "guest", NoPass: true, Role: RoleAdmin}
}

// Lookup returns the named user or nil.
func (r *Registry) Lookup(name string) *User {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.users[name]
}

// ErrAuthFailed is returned when AUTH credentials don't match.
var ErrAuthFailed = errors.New("WRONGPASS invalid username-password pair or user is disabled")

// Authenticate verifies credentials and returns the matched user.
func (r *Registry) Authenticate(name string, pw []byte) (*User, error) {
	u := r.Lookup(name)
	if u == nil || !u.Verify(pw) {
		return nil, ErrAuthFailed
	}
	return u, nil
}

// MaxFailedAuth is the per-connection AUTH attempt budget. Once exceeded,
// the connection should be closed by the dispatcher to slow brute-force.
const MaxFailedAuth = 5

// Session is per-connection auth state. It's a struct (not interface) so the
// hot path stays allocation-free.
type Session struct {
	user        *User
	failedAuth  int
}

// NewSession returns a fresh, unauthenticated session. AUTH must succeed
// before the dispatcher will run any data command on this session.
func NewSession() *Session {
	return &Session{}
}

// Authenticated reports whether this session is allowed to run commands.
func (s *Session) Authenticated() bool {
	return s.user != nil
}

// User returns the authenticated principal (nil if not authenticated).
func (s *Session) User() *User { return s.user }

// SetUser marks the session as authenticated as u and resets the failure counter.
func (s *Session) SetUser(u *User) {
	s.user = u
	s.failedAuth = 0
}

// RecordAuthFailure increments the failed-AUTH counter and returns the new total.
func (s *Session) RecordAuthFailure() int {
	s.failedAuth++
	return s.failedAuth
}

// FailedAuth returns the current failed-AUTH count.
func (s *Session) FailedAuth() int { return s.failedAuth }
