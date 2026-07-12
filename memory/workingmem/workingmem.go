// Package workingmem provides working memory: a small, structured scratchpad
// (current goal, todos, key facts) that lives in session State and therefore
// survives context compaction. The compaction middleware (ADR 0007) replaces
// old messages with a summary, but State is untouched — so durable task context
// belongs here, not in the message log. See ADR 0017.
//
// The scratchpad is stored under a single reserved State key as a JSON string.
// Encoding the whole snapshot as one string (rather than storing []Todo etc.
// directly) keeps it stable across the FileStore JSON round-trip, where a
// []Todo would otherwise reload as []any of map[string]any.
package workingmem

import (
	"encoding/json"

	"github.com/jiujuan/goagent/session"
)

// stateKey is the reserved session-State key holding the encoded Snapshot. It
// is session-scoped (no app:/user:/temp: prefix) so it persists per session.
const stateKey = "wm:snapshot"

// Todo is one tracked task item.
type Todo struct {
	ID   string `json:"id"`
	Text string `json:"text"`
	Done bool   `json:"done,omitempty"`
}

// Snapshot is the full working-memory contents at a point in time.
type Snapshot struct {
	Goal  string            `json:"goal,omitempty"`
	Todos []Todo            `json:"todos,omitempty"`
	Notes map[string]string `json:"notes,omitempty"`
}

// Empty reports whether the snapshot holds nothing worth rendering.
func (s Snapshot) Empty() bool {
	return s.Goal == "" && len(s.Todos) == 0 && len(s.Notes) == 0
}

// WorkingMemory is a typed view over a session's working-memory scratchpad. It
// reads from session State; writes go through the UpdateTool so they flow into
// an event's StateDelta and persist (a direct State.Set is not persisted by the
// FileStore — see session.commit).
type WorkingMemory struct {
	s *session.Session
}

// For wraps a session's working memory.
func For(s *session.Session) *WorkingMemory { return &WorkingMemory{s: s} }

// Snapshot returns the current working memory.
func (w *WorkingMemory) Snapshot() Snapshot {
	if w.s == nil {
		return Snapshot{}
	}
	return readSnapshot(w.s.State())
}

// Goal returns the current goal (empty if unset).
func (w *WorkingMemory) Goal() string { return w.Snapshot().Goal }

// Todos returns the tracked todo items.
func (w *WorkingMemory) Todos() []Todo { return w.Snapshot().Todos }

// Notes returns the key/value facts.
func (w *WorkingMemory) Notes() map[string]string { return w.Snapshot().Notes }

// readSnapshot decodes the Snapshot from State, returning the zero Snapshot when
// absent or malformed.
func readSnapshot(st session.StateReader) Snapshot {
	v, ok := st.Get(stateKey)
	if !ok {
		return Snapshot{}
	}
	s, ok := v.(string)
	if !ok {
		return Snapshot{}
	}
	var snap Snapshot
	_ = json.Unmarshal([]byte(s), &snap)
	return snap
}

// encodeSnapshot serializes a Snapshot for storage in State.
func encodeSnapshot(snap Snapshot) string {
	b, _ := json.Marshal(snap)
	return string(b)
}
