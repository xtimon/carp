// Package clusterauth wraps inter-node TCP frames with an optional HMAC.
// The same helper is used for RPC and gossip so both surfaces share one secret.
//
// Wire format with HMAC enabled:
//
//	[4B body length][32B HMAC-SHA256(body)][body...]
//
// Without HMAC (secret == nil) the frame is the legacy [4B length][body] —
// existing deployments that haven't set a secret keep working.
//
// Note: HMAC authenticates the frame but does not prevent replay. Replay
// resistance is out of scope for v1; deploy on a private network.
package clusterauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"sync"
)

const macSize = sha256.Size

// MaxFrameSize caps body length so a hostile peer can't push us into a
// multi-GB allocation by lying about length.
const MaxFrameSize = 64 * 1024 * 1024

// pooled HMACs avoid per-frame allocation in the steady state where every
// call uses the same cluster secret. We can only pool a hasher tied to one
// secret (hmac.New binds the key at construction; Reset clears running
// state but keeps the key), so we cache the secret used by the pool and
// fall back to a fresh hasher when callers pass a different secret (tests).
var (
	poolMu     sync.RWMutex
	poolSecret []byte
	hmacPool   sync.Pool
)

// SetPoolSecret tells the package which secret to pre-bind pooled HMAC
// instances with. Call this once at startup, after loading config, so the
// hot path can recycle hashers instead of allocating each frame. Calling
// with a different value resets the pool.
func SetPoolSecret(secret []byte) {
	poolMu.Lock()
	defer poolMu.Unlock()
	poolSecret = append([]byte(nil), secret...)
	hmacPool = sync.Pool{
		New: func() interface{} {
			return hmac.New(sha256.New, poolSecret)
		},
	}
}

// WriteFrame writes body as a single framed message. If secret is non-empty
// the frame is HMAC-authenticated, otherwise it falls back to the legacy
// length-prefixed format.
func WriteFrame(w io.Writer, secret, body []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(body)))
	if _, err := w.Write(lenBuf); err != nil {
		return err
	}
	if len(secret) > 0 {
		mac := computeMAC(secret, body)
		if _, err := w.Write(mac); err != nil {
			return err
		}
	}
	_, err := w.Write(body)
	return err
}

// ReadFrame reads one framed message. If secret is non-empty the HMAC is
// verified; mismatch returns an error and the caller should drop the conn.
func ReadFrame(r io.Reader, secret []byte) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	bodyLen := binary.BigEndian.Uint32(lenBuf)
	if bodyLen > MaxFrameSize {
		return nil, errors.New("frame too large")
	}
	var mac []byte
	if len(secret) > 0 {
		mac = make([]byte, macSize)
		if _, err := io.ReadFull(r, mac); err != nil {
			return nil, err
		}
	}
	if bodyLen == 0 {
		return nil, nil
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	if len(secret) > 0 {
		expected := computeMAC(secret, body)
		if !hmac.Equal(mac, expected) {
			return nil, errors.New("frame HMAC mismatch")
		}
	}
	return body, nil
}

func computeMAC(secret, body []byte) []byte {
	poolMu.RLock()
	canPool := len(poolSecret) > 0 && bytesEqual(secret, poolSecret)
	poolMu.RUnlock()
	if canPool {
		h := hmacPool.Get().(hash.Hash)
		h.Reset()
		h.Write(body)
		sum := h.Sum(nil)
		hmacPool.Put(h)
		return sum
	}
	h := hmac.New(sha256.New, secret)
	h.Write(body)
	return h.Sum(nil)
}

// bytesEqual avoids importing "bytes" just for one Equal call.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
