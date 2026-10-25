// Package canon is Straw Link's canonical encoding: every signed or hashed structure is a byte string of fields, each behind a 4-byte
// big-endian length. Integers are 8-byte big-endian (at most 2^53-1), a bool is one byte 0 or 1, and a list is one field holding a
// 4-byte count followed by its items as fields. A decoder rejects cut fields, trailing bytes, and a wrong count.
package canon

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// MaxSafeInt is the largest integer a JavaScript client can sign exactly.
const MaxSafeInt = 1<<53 - 1

// ErrMalformed is wrapped by every decoding error.
var ErrMalformed = errors.New("malformed canonical encoding")

func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
}

// Enc is a structure: each field behind its length.
func Enc(fields ...[]byte) []byte {
	n := 0
	for _, f := range fields {
		n += 4 + len(f)
	}
	out := make([]byte, 0, n)
	for _, f := range fields {
		out = binary.BigEndian.AppendUint32(out, uint32(len(f)))
		out = append(out, f...)
	}
	return out
}

// Int is an 8-byte big-endian unsigned integer; wrap it with Enc as a field.
func Int(n uint64) []byte { return binary.BigEndian.AppendUint64(nil, n) }

// Str is text as it travels.
func Str(s string) []byte { return []byte(s) }

// Bool is one byte, 0 or 1.
func Bool(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

// List is the item count, then each item as a field.
func List(items ...[]byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(items))), Enc(items...)...)
}

// Dec returns the fields of a structure. With count >= 0 it must hold exactly that many.
func Dec(b []byte, count int) ([][]byte, error) {
	var out [][]byte
	for at := 0; at < len(b); {
		if len(b)-at < 4 {
			return nil, malformed("cut length")
		}
		n := int(binary.BigEndian.Uint32(b[at:]))
		at += 4
		if n < 0 || len(b)-at < n {
			return nil, malformed("cut field")
		}
		out = append(out, b[at:at+n:at+n])
		at += n
	}
	if count >= 0 && len(out) != count {
		return nil, malformed("expected %d fields, got %d", count, len(out))
	}
	return out, nil
}

// ReadInt reads an 8-byte integer no larger than 2^53-1.
func ReadInt(f []byte) (int64, error) {
	if len(f) != 8 {
		return 0, malformed("an integer is 8 bytes")
	}
	v := binary.BigEndian.Uint64(f)
	if v > MaxSafeInt {
		return 0, malformed("integer beyond 2^53 - 1")
	}
	return int64(v), nil
}

// ReadBool reads a one-byte bool.
func ReadBool(f []byte) (bool, error) {
	if len(f) != 1 || f[0] > 1 {
		return false, malformed("a bool is one byte, 0 or 1")
	}
	return f[0] == 1, nil
}

// ReadList reads the items of a list field; the count has to match the items exactly.
func ReadList(f []byte) ([][]byte, error) {
	if len(f) < 4 {
		return nil, malformed("cut list")
	}
	return Dec(f[4:], int(binary.BigEndian.Uint32(f)))
}
