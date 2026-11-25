// Package ids makes notification ids: 26 Crockford base32 characters,
// 48 bits of milliseconds then 80 random bits, so ids sort by creation
// time (like a ULID) and are safe to show to callers.
package ids

import (
	"crypto/rand"
	"time"
)

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// New returns a new id with the given prefix, e.g. "ntf_01J…".
func New(prefix string) string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	_, _ = rand.Read(b[6:])
	// 128 bits → 26 base32 characters (the first carries 3 bits).
	out := make([]byte, 26)
	var acc uint64
	var bits uint
	j := 25
	for i := 15; i >= 0; i-- {
		acc |= uint64(b[i]) << bits
		bits += 8
		for bits >= 5 && j >= 0 {
			out[j] = alphabet[acc&31]
			acc >>= 5
			bits -= 5
			j--
		}
	}
	if j >= 0 {
		out[j] = alphabet[acc&31]
	}
	return prefix + string(out)
}
