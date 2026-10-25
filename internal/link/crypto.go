package link

import (
	"crypto/ed25519"
	"crypto/sha256"

	"github.com/wayne930242/straw-relay/internal/canon"
)

// The context every signature starts with.
const (
	ContextEnvelope = "straw-link/envelope/1"
	ContextRoster   = "straw-link/roster/1"
	ContextRequest  = "straw-link/request/1"
)

// SignedBytes is context ‖ team id ‖ the rest, by plain concatenation.
func SignedBytes(context string, team []byte, rest ...[]byte) []byte {
	out := append([]byte(context), team...)
	for _, r := range rest {
		out = append(out, r...)
	}
	return out
}

// RequestBytes is what a relay request's signature covers: the request context, the team, and
// enc(method, path with query, time in ms, sha256(body)).
func RequestBytes(team []byte, method, path string, timeMS int64, body []byte) []byte {
	sum := sha256.Sum256(body)
	return SignedBytes(ContextRequest, team, canon.Enc(canon.Str(method), canon.Str(path), canon.Int(uint64(timeMS)), sum[:]))
}

// VerifySig reports whether sig is an Ed25519 signature of msg by pub; false, never a panic, for anything malformed.
func VerifySig(pub, msg, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, msg, sig)
}
