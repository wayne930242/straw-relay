// Package link holds the parts of Straw Link's byte contract that the relay can check from plaintext: device ids and key records,
// request signatures, roster entries and their rules, and envelope headers. The reference implementation is pi-straw-boss's src/link.
package link

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"regexp"

	"github.com/wayne930242/straw-relay/internal/canon"
)

const alphabet = "abcdefghijklmnopqrstuvwxyz234567"

// Base32 is RFC 4648 base32 in lower case, without padding.
func Base32(b []byte) string {
	out := make([]byte, 0, (len(b)*8+4)/5)
	var acc uint32
	bits := 0
	for _, x := range b {
		acc = acc<<8 | uint32(x)
		bits += 8
		for bits >= 5 {
			out = append(out, alphabet[(acc>>(bits-5))&31])
			bits -= 5
		}
		acc &= 1<<bits - 1
	}
	if bits > 0 {
		out = append(out, alphabet[(acc<<(5-bits))&31])
	}
	return string(out)
}

// DeviceID is the first 16 bytes of sha256(signing public key) in base32: 26 characters.
func DeviceID(signPub []byte) string {
	sum := sha256.Sum256(signPub)
	return Base32(sum[:16])
}

// The 26th character carries 3 bits and 2 padding bits that must be zero, so each id has one spelling.
var idPattern = regexp.MustCompile(`^[a-z2-7]{25}[aeimquy4]$`)

// IDOK reports whether s is a canonical device id.
func IDOK(s string) bool { return idPattern.MatchString(s) }

// KeyRecord is the public half of a device.
type KeyRecord struct {
	ID      string
	SignPub []byte
	BoxPub  []byte
}

// DecodeKeyRecord reads enc(id, signing public key, encryption public key); the id has to be the hash of the signing key.
func DecodeKeyRecord(b []byte) (KeyRecord, error) {
	f, err := canon.Dec(b, 3)
	if err != nil {
		return KeyRecord{}, err
	}
	if len(f[1]) != ed25519.PublicKeySize || len(f[2]) != 32 {
		return KeyRecord{}, fmt.Errorf("%w: a key is 32 bytes", canon.ErrMalformed)
	}
	r := KeyRecord{ID: string(f[0]), SignPub: f[1], BoxPub: f[2]}
	if r.ID != DeviceID(r.SignPub) {
		return KeyRecord{}, fmt.Errorf("%w: the id is not the hash of the signing key", canon.ErrMalformed)
	}
	return r, nil
}
