package checkpoint

import "github.com/jiujuan/goagent/core"

// Fork creates a new checkpoint that branches off parent onto a new thread. It
// copies parent's State and links back via ParentID, so the snapshot tree
// records "the same history point, a different continuation" — the basis for
// time-travel and try-another-path.
//
// The returned checkpoint is not yet saved; the caller assigns it to a run and
// Saves it as the branch advances.
func Fork(parent *Checkpoint, newThreadID, newID string) *Checkpoint {
	return &Checkpoint{
		ID:       newID,
		ThreadID: newThreadID,
		ParentID: parent.ID,
		Step:     parent.Step,
		State:    cloneState(parent.State),
	}
}

// cloneState makes a safe copy of the slices/maps that matter for branching, so
// a forked run cannot mutate its parent's snapshot. Files is a backend handle
// and is shared intentionally (collaboration surface).
func cloneState(s core.State) core.State {
	out := core.State{Files: s.Files}
	if s.Messages != nil {
		out.Messages = append([]core.Message(nil), s.Messages...)
	}
	if s.Todos != nil {
		out.Todos = append([]core.Todo(nil), s.Todos...)
	}
	if s.KV != nil {
		out.KV = make(map[string]any, len(s.KV))
		for k, v := range s.KV {
			out.KV[k] = v
		}
	}
	return out
}
