package resp

import (
	"bytes"
	"strings"
	"testing"
)

func TestEncodeSimpleString(t *testing.T) {
	got := EncodeSimpleString("OK")
	want := "+OK\r\n"
	if string(got) != want {
		t.Errorf("EncodeSimpleString(\"OK\") = %q, want %q", got, want)
	}
}

func TestEncodeError(t *testing.T) {
	got := EncodeError("something wrong")
	want := "-ERR something wrong\r\n"
	if string(got) != want {
		t.Errorf("EncodeError = %q, want %q", got, want)
	}
}

func TestEncodeInteger(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, ":0\r\n"},
		{42, ":42\r\n"},
		{-1, ":-1\r\n"},
	}
	for _, tt := range tests {
		got := string(EncodeInteger(tt.n))
		if got != tt.want {
			t.Errorf("EncodeInteger(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestEncodeBulkString(t *testing.T) {
	tests := []struct {
		s    []byte
		want string
	}{
		{nil, "$-1\r\n"},
		{[]byte(""), "$0\r\n\r\n"},
		{[]byte("hello"), "$5\r\nhello\r\n"},
	}
	for _, tt := range tests {
		got := string(EncodeBulkString(tt.s))
		if got != tt.want {
			t.Errorf("EncodeBulkString(%q) = %q, want %q", tt.s, got, tt.want)
		}
	}
}

func TestEncodeArray(t *testing.T) {
	items := [][]byte{[]byte("GET"), []byte("foo")}
	got := string(EncodeArray(items))
	want := "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
	if got != want {
		t.Errorf("EncodeArray = %q, want %q", got, want)
	}
}

func TestEncodeCommand(t *testing.T) {
	got := EncodeCommand("PING")
	want := "*1\r\n$4\r\nPING\r\n"
	if string(got) != want {
		t.Errorf("EncodeCommand(\"PING\") = %q, want %q", got, want)
	}

	got2 := EncodeCommand("SET", []byte("k"), []byte("v"))
	want2 := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
	if string(got2) != want2 {
		t.Errorf("EncodeCommand(\"SET\", \"k\", \"v\") = %q, want %q", got2, want2)
	}
}

func TestReader_ParseCommands(t *testing.T) {
	r := &Reader{}
	r.Feed([]byte("*1\r\n$4\r\nPING\r\n"))
	cmds := r.ParseCommands()
	if len(cmds) != 1 {
		t.Fatalf("expected 1 command, got %d", len(cmds))
	}
	if string(cmds[0][0]) != "PING" {
		t.Errorf("expected PING, got %s", cmds[0][0])
	}
}

func TestReader_ParseCommands_Multiple(t *testing.T) {
	r := &Reader{}
	r.Feed([]byte("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"))
	cmds := r.ParseCommands()
	if len(cmds) != 2 {
		t.Fatalf("expected 2 commands, got %d", len(cmds))
	}
	if string(cmds[0][0]) != "PING" {
		t.Errorf("cmd 0: expected PING, got %s", cmds[0][0])
	}
	if string(cmds[1][0]) != "GET" || string(cmds[1][1]) != "foo" {
		t.Errorf("cmd 1: expected GET foo, got %v", cmds[1])
	}
}

func TestReader_ParseCommands_Incremental(t *testing.T) {
	r := &Reader{}
	r.Feed([]byte("*2\r\n$3\r\n"))
	cmds := r.ParseCommands()
	if len(cmds) != 0 {
		t.Errorf("incomplete command should yield 0, got %d", len(cmds))
	}
	r.Feed([]byte("GET\r\n$3\r\nfoo\r\n"))
	cmds = r.ParseCommands()
	if len(cmds) != 1 {
		t.Fatalf("expected 1 command after complete, got %d", len(cmds))
	}
	if string(cmds[0][0]) != "GET" || string(cmds[0][1]) != "foo" {
		t.Errorf("expected GET foo, got %v", cmds[0])
	}
}

func TestFormatResponse_SimpleString(t *testing.T) {
	got := FormatResponse([]byte("+OK\r\n"))
	if got != "OK" {
		t.Errorf("FormatResponse(+OK) = %q, want OK", got)
	}
}

func TestFormatResponse_Error(t *testing.T) {
	got := FormatResponse([]byte("-ERR foo\r\n"))
	if !strings.HasPrefix(got, "-ERR") {
		t.Errorf("FormatResponse(-ERR) = %q", got)
	}
}

func TestFormatResponse_Integer(t *testing.T) {
	got := FormatResponse([]byte(":42\r\n"))
	if got != "42" {
		t.Errorf("FormatResponse(:42) = %q, want 42", got)
	}
}

func TestFormatResponse_BulkString(t *testing.T) {
	got := FormatResponse([]byte("$5\r\nhello\r\n"))
	if got != "hello" {
		t.Errorf("FormatResponse($5 hello) = %q, want hello", got)
	}
}

func TestFormatResponse_NilBulkString(t *testing.T) {
	got := FormatResponse([]byte("$-1\r\n"))
	if got != "(nil)" {
		t.Errorf("FormatResponse($-1) = %q, want (nil)", got)
	}
}

func TestRoundtrip_EncodeDecode(t *testing.T) {
	items := [][]byte{[]byte("SET"), []byte("mykey"), []byte("myvalue")}
	encoded := EncodeArray(items)
	r := &Reader{}
	r.Feed(encoded)
	cmds := r.ParseCommands()
	if len(cmds) != 1 {
		t.Fatalf("expected 1 command, got %d", len(cmds))
	}
	if !bytes.Equal(cmds[0][0], items[0]) || !bytes.Equal(cmds[0][1], items[1]) || !bytes.Equal(cmds[0][2], items[2]) {
		t.Errorf("roundtrip mismatch: got %v, want %v", cmds[0], items)
	}
}
