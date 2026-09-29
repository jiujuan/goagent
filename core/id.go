package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// NewID returns a short random hex identifier suitable for event and
// invocation IDs. It draws from crypto/rand so IDs are unique without a
// central counter or wall-clock dependency.
func NewID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

// MaxThreadIDLen bounds a thread id's length in bytes. The id is used verbatim
// as a file name and a directory name, so the limit keeps it clear of the 255
// byte name limit any target filesystem imposes.
const MaxThreadIDLen = 64

// CheckThreadID reports whether id may name a thread's files and directories.
// A thread id is supplied by the caller (agent.OnThread, agent.Resume) and used
// verbatim as one file name in the checkpointer and one directory name in a
// workspace, so an id carrying path syntax would place a thread's data outside
// that store: only [A-Za-z0-9_-], 1 to MaxThreadIDLen bytes, is accepted.
func CheckThreadID(id string) error {
	if id == "" {
		return fmt.Errorf("core: thread id is empty; pass a non-empty id, or omit agent.OnThread to get a generated one")
	}
	if len(id) > MaxThreadIDLen {
		return fmt.Errorf("core: thread id %q is %d bytes; the limit is %d", id, len(id), MaxThreadIDLen)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return fmt.Errorf("core: thread id %q contains %q; only A-Z a-z 0-9 - _ are allowed", id, r)
		}
	}
	return nil
}
