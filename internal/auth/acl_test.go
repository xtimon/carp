package auth

import (
	"testing"
)

func TestCommandCategory(t *testing.T) {
	cases := []struct {
		cmd  string
		args []string
		want Category
	}{
		{"GET", nil, CatRead},
		{"SET", nil, CatWrite},
		{"DEL", nil, CatWrite},
		{"FLUSHDB", nil, CatAdmin},
		{"BGSAVE", nil, CatAdmin},
		{"CLUSTER", []string{"NODES"}, CatRead},
		{"CLUSTER", []string{"INFO"}, CatRead},
		{"CLUSTER", []string{"LEAVE", "node1"}, CatAdmin},
		{"CLUSTER", []string{"REPAIR"}, CatAdmin},
		{"CLUSTER", []string{}, CatAdmin}, // missing subcommand → admin (fail closed)
		{"CONFIG", []string{"GET", "maxmemory"}, CatRead},
		{"CONFIG", []string{"SET", "x", "y"}, CatAdmin},
	}
	for _, tc := range cases {
		args := make([][]byte, len(tc.args))
		for i, a := range tc.args {
			args[i] = []byte(a)
		}
		if got := commandCategory(tc.cmd, args); got != tc.want {
			t.Errorf("commandCategory(%s, %v) = %d, want %d", tc.cmd, tc.args, got, tc.want)
		}
	}
}

func TestRoleAllows(t *testing.T) {
	cases := []struct {
		role Role
		cat  Category
		want bool
	}{
		{RoleAdmin, CatRead, true},
		{RoleAdmin, CatWrite, true},
		{RoleAdmin, CatAdmin, true},
		{RoleReadWrite, CatRead, true},
		{RoleReadWrite, CatWrite, true},
		{RoleReadWrite, CatAdmin, false},
		{RoleReadOnly, CatRead, true},
		{RoleReadOnly, CatWrite, false},
		{RoleReadOnly, CatAdmin, false},
		{RoleNone, CatRead, false},
		{RoleNone, CatWrite, false},
		{RoleNone, CatAdmin, false},
	}
	for _, tc := range cases {
		if got := tc.role.allows(tc.cat); got != tc.want {
			t.Errorf("Role(%d).allows(%d) = %v, want %v", tc.role, tc.cat, got, tc.want)
		}
	}
}

func TestParseRole(t *testing.T) {
	cases := []struct {
		in    string
		want  Role
		valid bool
	}{
		{"admin", RoleAdmin, true},
		{"readwrite", RoleReadWrite, true},
		{"", RoleReadWrite, true}, // empty defaults to readwrite for backward compat
		{"readonly", RoleReadOnly, true},
		{"none", RoleNone, true},
		{"superuser", RoleNone, false},
	}
	for _, tc := range cases {
		got, ok := ParseRole(tc.in)
		if ok != tc.valid || got != tc.want {
			t.Errorf("ParseRole(%q) = (%d, %v), want (%d, %v)", tc.in, got, ok, tc.want, tc.valid)
		}
	}
}

func TestKeyPattern_Matches(t *testing.T) {
	cases := []struct {
		pattern string
		key     string
		want    bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"app1:*", "app1:foo", true},
		{"app1:*", "app1:", true},
		{"app1:*", "app2:foo", false},
		{"exact", "exact", true},
		{"exact", "exactly", true}, // current behavior: prefix match without trailing *
		{"exact", "exa", false},
	}
	for _, tc := range cases {
		p := CompilePattern(tc.pattern)
		if got := p.Matches([]byte(tc.key)); got != tc.want {
			t.Errorf("pattern %q on key %q = %v, want %v", tc.pattern, tc.key, got, tc.want)
		}
	}
}

func TestUser_AllowsCommand(t *testing.T) {
	cases := []struct {
		role Role
		cmd  string
		args []string
		want bool
	}{
		{RoleReadOnly, "GET", nil, true},
		{RoleReadOnly, "SET", nil, false},
		{RoleReadWrite, "SET", nil, true},
		{RoleReadWrite, "FLUSHDB", nil, false},
		{RoleAdmin, "FLUSHDB", nil, true},
		{RoleAdmin, "CLUSTER", []string{"LEAVE"}, true},
		{RoleReadWrite, "CLUSTER", []string{"NODES"}, true},
		{RoleReadWrite, "CLUSTER", []string{"LEAVE"}, false},
		{RoleNone, "GET", nil, false},
	}
	for _, tc := range cases {
		args := make([][]byte, len(tc.args))
		for i, a := range tc.args {
			args[i] = []byte(a)
		}
		u := &User{Role: tc.role}
		if got := u.AllowsCommand(tc.cmd, args); got != tc.want {
			t.Errorf("user[%d].AllowsCommand(%s,%v) = %v, want %v", tc.role, tc.cmd, tc.args, got, tc.want)
		}
	}
}

func TestUser_AllowsKey(t *testing.T) {
	u := &User{Role: RoleReadWrite, Keys: CompilePatterns([]string{"app1:*", "shared:*"})}
	if !u.AllowsKey([]byte("app1:foo")) {
		t.Error("app1:foo should be allowed")
	}
	if !u.AllowsKey([]byte("shared:x")) {
		t.Error("shared:x should be allowed")
	}
	if u.AllowsKey([]byte("app2:foo")) {
		t.Error("app2:foo should NOT be allowed")
	}

	// Unrestricted user (no Keys list) sees everything.
	free := &User{Role: RoleReadWrite}
	if !free.AllowsKey([]byte("any:thing")) {
		t.Error("unrestricted user should see any key")
	}

	// Nil user always denied.
	var nu *User
	if nu.AllowsKey([]byte("k")) {
		t.Error("nil user should not allow keys")
	}
}

func TestUser_FilterKeys(t *testing.T) {
	u := &User{Role: RoleReadOnly, Keys: CompilePatterns([]string{"app1:*"})}
	in := [][]byte{[]byte("app1:a"), []byte("app2:b"), []byte("app1:c"), []byte("other")}
	out := u.FilterKeys(in)
	if len(out) != 2 {
		t.Fatalf("expected 2 keys, got %d: %v", len(out), out)
	}
	if string(out[0]) != "app1:a" || string(out[1]) != "app1:c" {
		t.Errorf("filter dropped wrong keys: %v", out)
	}
}
