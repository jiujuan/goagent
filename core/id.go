package core

import (
	"crypto/rand"
	"encoding/hex"
)

// NewID returns a short random hex identifier suitable for event and
// invocation IDs. It draws from crypto/rand so IDs are unique without a
// central counter or wall-clock dependency.
func NewID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}
