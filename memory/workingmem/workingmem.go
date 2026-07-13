// Package workingmem provides working memory: a small, structured scratchpad
// (current goal, todos, key facts) that lives in run State.KV and therefore
// survives context compaction. Compaction (a ModifyRequest middleware) reshapes
// the message history, but State is untouched — so durable task context belongs
// here, not in the message log. See ADR 0017.
//
// The scratchpad is stored under a single reserved State.KV key as a JSON string
// (encoding the whole snapshot as one string keeps it stable across a file
// checkpoint's JSON round-trip).
package workingmem

import (
	"encoding/json"

	"github.com/jiujuan/goagent/core"
)

// stateKey is the reserved State.KV key holding the encoded Snapshot.
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

// WorkingMemory is a typed view over a run's working-memory scratchpad. It reads
// from State.KV; writes go through UpdateTool (which returns a StateOp so the
// loop persists it).
type WorkingMemory struct {
	st *core.State
}

// For wraps a run's State.
func For(st *core.State) *WorkingMemory { return &WorkingMemory{st: st} }

// Snapshot returns the current working memory.
func (w *WorkingMemory) Snapshot() Snapshot {
	if w.st == nil {
		return Snapshot{}
	}
	return readSnapshot(w.st)
}

// Goal returns the current goal (empty if unset).
func (w *WorkingMemory) Goal() string { return w.Snapshot().Goal }

// Todos returns the tracked todo items.
func (w *WorkingMemory) Todos() []Todo { return w.Snapshot().Todos }

// Notes returns the key/value facts.
func (w *WorkingMemory) Notes() map[string]string { return w.Snapshot().Notes }

// readSnapshot decodes the Snapshot from State.KV, returning the zero Snapshot
// when absent or malformed.
func readSnapshot(st *core.State) Snapshot {
	if st == nil || st.KV == nil {
		return Snapshot{}
	}
	v, ok := st.KV[stateKey]
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

// encodeSnapshot serializes a Snapshot for storage in State.KV.
func encodeSnapshot(snap Snapshot) string {
	b, _ := json.Marshal(snap)
	return string(b)
}
