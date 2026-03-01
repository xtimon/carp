package rpc

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/carp/internal/storage"
)

func TestPackUnpackCommand_Roundtrip(t *testing.T) {
	cmd := byte(CmdSet)
	args := [][]byte{[]byte("foo"), []byte("bar")}
	packed := packCommand(cmd, args)
	gotCmd, gotArgs, err := unpackCommand(packed)
	if err != nil {
		t.Fatalf("unpackCommand: %v", err)
	}
	if gotCmd != cmd {
		t.Errorf("cmd = %d, want %d", gotCmd, cmd)
	}
	if !bytes.Equal(gotArgs[0], args[0]) || !bytes.Equal(gotArgs[1], args[1]) {
		t.Errorf("args = %v, want %v", gotArgs, args)
	}
}

func TestPackCommand_EmptyArgs(t *testing.T) {
	packed := packCommand(CmdGet, [][]byte{[]byte("k")})
	if len(packed) < 3 {
		t.Errorf("packed too short: %d", len(packed))
	}
	if packed[0] != CmdGet {
		t.Errorf("cmd byte = %d", packed[0])
	}
	n := binary.BigEndian.Uint16(packed[1:3])
	if n != 1 {
		t.Errorf("arg count = %d, want 1", n)
	}
}

func TestUnpackCommand_Truncated(t *testing.T) {
	_, _, err := unpackCommand([]byte{0, 0})
	if err == nil {
		t.Error("unpackCommand truncate should error")
	}
}

func TestHandleConn_Get(t *testing.T) {
	store := storage.New()
	store.Set([]byte("k"), []byte("v"), nil)
	h := &Handler{Store: store}

	client, server := net.Pipe()
	go HandleConn(server, h)

	// Send CmdGet for key "k"
	msg := packCommand(CmdGet, [][]byte{[]byte("k")})
	sendRPC(client, msg)

	resp := readRPC(client)
	if string(resp) != "v" {
		t.Errorf("Get response = %q, want v", resp)
	}
	client.Close()
}

func TestHandleConn_Set(t *testing.T) {
	store := storage.New()
	h := &Handler{Store: store}

	client, server := net.Pipe()
	go HandleConn(server, h)

	msg := packCommand(CmdSet, [][]byte{[]byte("x"), []byte("y")})
	sendRPC(client, msg)
	resp := readRPC(client)
	if string(resp) != "OK" {
		t.Errorf("Set response = %q, want OK", resp)
	}
	val, _ := store.Get([]byte("x"))
	if string(val) != "y" {
		t.Errorf("Get after Set = %q, want y", val)
	}
	client.Close()
}

func TestHandleConn_RestoreKey(t *testing.T) {
	store := storage.New()
	store.Set([]byte("src"), []byte("data"), nil)
	dump, _ := store.DumpKey([]byte("src"))
	store.FlushDB()

	h := &Handler{Store: store}
	client, server := net.Pipe()
	go HandleConn(server, h)

	msg := packCommand(CmdRestoreKey, [][]byte{[]byte("dst"), dump})
	sendRPC(client, msg)
	resp := readRPC(client)
	if string(resp) != "OK" {
		t.Errorf("RestoreKey response = %q, want OK", resp)
	}
	val, _ := store.Get([]byte("dst"))
	if string(val) != "data" {
		t.Errorf("restored value = %q, want data", val)
	}
	client.Close()
}

func TestHandleConn_OnRepair(t *testing.T) {
	store := storage.New()
	repairCount := 0
	h := &Handler{
		Store:    store,
		OnRepair: func() int { repairCount++; return 42 },
	}

	client, server := net.Pipe()
	go HandleConn(server, h)

	msg := packCommand(CmdRunRepair, nil)
	sendRPC(client, msg)
	resp := readRPC(client)
	if string(resp) != "42" {
		t.Errorf("RunRepair response = %q, want 42", resp)
	}
	if repairCount != 1 {
		t.Errorf("OnRepair called %d times, want 1", repairCount)
	}
	client.Close()
}

func sendRPC(w io.Writer, msg []byte) {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msg)))
	w.Write(lenBuf)
	w.Write(msg)
}

func readRPC(r io.Reader) []byte {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil
	}
	n := binary.BigEndian.Uint32(lenBuf)
	if n == 0 {
		return nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil
	}
	return buf
}
