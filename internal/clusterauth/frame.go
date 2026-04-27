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
	"io"
)

const macSize = sha256.Size

// MaxFrameSize caps body length so a hostile peer can't push us into a
// multi-GB allocation by lying about length.
const MaxFrameSize = 64 * 1024 * 1024

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
	h := hmac.New(sha256.New, secret)
	h.Write(body)
	return h.Sum(nil)
}
