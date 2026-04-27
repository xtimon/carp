package auth

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func mustHash(t *testing.T, pw string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	return string(h)
}

func TestRegistry_DefaultUserIsLocked(t *testing.T) {
	// default user is always present but locked unless explicitly granted.
	r := NewRegistry([]User{
		{Name: "app1", PasswordHash: mustHash(t, "secret")},
	})
	def := r.Lookup("default")
	if def == nil {
		t.Fatal("default user should always exist")
	}
	if def.Role != RoleNone {
		t.Errorf("default role = %d, want RoleNone", def.Role)
	}
	if def.NoPass {
		t.Error("default user must not be NoPass — that would let anyone in")
	}
}

func TestNewSession_StartsUnauthenticated(t *testing.T) {
	s := NewSession()
	if s.Authenticated() {
		t.Error("new session should start unauthenticated")
	}
	if s.User() != nil {
		t.Error("new session should have no user")
	}
}

func TestRegistry_Authenticate_Success(t *testing.T) {
	r := NewRegistry([]User{
		{Name: "app1", PasswordHash: mustHash(t, "correct")},
	})
	u, err := r.Authenticate("app1", []byte("correct"))
	if err != nil || u == nil || u.Name != "app1" {
		t.Fatalf("expected app1 success, got user=%v err=%v", u, err)
	}
}

func TestRegistry_Authenticate_WrongPassword(t *testing.T) {
	r := NewRegistry([]User{
		{Name: "app1", PasswordHash: mustHash(t, "correct")},
	})
	if _, err := r.Authenticate("app1", []byte("wrong")); err == nil {
		t.Fatal("expected auth failure on wrong password")
	}
}

func TestRegistry_Authenticate_UnknownUser(t *testing.T) {
	r := NewRegistry([]User{
		{Name: "app1", PasswordHash: mustHash(t, "correct")},
	})
	if _, err := r.Authenticate("nobody", []byte("anything")); err == nil {
		t.Fatal("expected auth failure for unknown user")
	}
}

func TestSession_FailedAuthCounter(t *testing.T) {
	r := NewRegistry([]User{
		{Name: "app1", PasswordHash: mustHash(t, "p")},
	})
	s := NewSession()
	if s.FailedAuth() != 0 {
		t.Errorf("new session FailedAuth = %d, want 0", s.FailedAuth())
	}
	if got := s.RecordAuthFailure(); got != 1 {
		t.Errorf("RecordAuthFailure = %d, want 1", got)
	}
	s.RecordAuthFailure()
	if s.FailedAuth() != 2 {
		t.Errorf("FailedAuth = %d, want 2", s.FailedAuth())
	}
	// Successful AUTH should reset the counter so legitimate users who fat-finger
	// once aren't penalized for the rest of the session.
	u, _ := r.Authenticate("app1", []byte("p"))
	s.SetUser(u)
	if s.FailedAuth() != 0 {
		t.Errorf("FailedAuth after successful SetUser = %d, want 0", s.FailedAuth())
	}
}

func TestSession_SetUser(t *testing.T) {
	r := NewRegistry([]User{
		{Name: "app1", PasswordHash: mustHash(t, "p")},
	})
	s := NewSession()
	if s.Authenticated() {
		t.Fatal("locked session should start unauthenticated")
	}
	u, _ := r.Authenticate("app1", []byte("p"))
	s.SetUser(u)
	if !s.Authenticated() || s.User().Name != "app1" {
		t.Errorf("session should now be authenticated as app1, got %v", s.User())
	}
}
