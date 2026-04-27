package clusterauth

import (
	"bytes"
	"testing"
)

func TestFrame_RoundTrip_NoSecret(t *testing.T) {
	var buf bytes.Buffer
	body := []byte("hello world")
	if err := WriteFrame(&buf, nil, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := ReadFrame(&buf, nil)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("got %q, want %q", got, body)
	}
}

func TestFrame_RoundTrip_WithSecret(t *testing.T) {
	var buf bytes.Buffer
	secret := []byte("topsecret")
	body := []byte("hello world")
	if err := WriteFrame(&buf, secret, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := ReadFrame(&buf, secret)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("got %q, want %q", got, body)
	}
}

func TestFrame_WrongSecret_Rejected(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, []byte("right"), []byte("payload")); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, err := ReadFrame(&buf, []byte("wrong")); err == nil {
		t.Fatal("expected HMAC mismatch error, got nil")
	}
}

func TestFrame_TamperedBody_Rejected(t *testing.T) {
	var buf bytes.Buffer
	secret := []byte("k")
	if err := WriteFrame(&buf, secret, []byte("payload")); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	raw := buf.Bytes()
	// Flip a bit in the body (last byte).
	raw[len(raw)-1] ^= 0x01
	if _, err := ReadFrame(bytes.NewReader(raw), secret); err == nil {
		t.Fatal("expected HMAC mismatch on tampered body")
	}
}

func TestFrame_OversizeRejected(t *testing.T) {
	// Hand-craft a frame claiming 1GB body; ReadFrame must reject before allocating.
	raw := []byte{0x40, 0x00, 0x00, 0x00} // 0x40000000 = 1 GiB
	if _, err := ReadFrame(bytes.NewReader(raw), nil); err == nil {
		t.Fatal("expected oversize rejection")
	}
}

func TestFrame_SecretMismatch_OneSide(t *testing.T) {
	// Sender wrote without secret; reader expects one. ReadFrame should fail
	// when it tries to read the (missing) HMAC and consume body bytes as MAC.
	var buf bytes.Buffer
	if err := WriteFrame(&buf, nil, []byte("hello")); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, err := ReadFrame(&buf, []byte("k")); err == nil {
		t.Fatal("expected error when reader is HMAC-mode but sender isn't")
	}
}
