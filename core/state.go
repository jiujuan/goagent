package core

// State is v2's explicit, checkpointable run state. v1 reconstructed state by
// replaying Event.Actions (event sourcing); v2 snapshots this struct at each
// step via a Checkpointer, which is simpler and the substrate for resume /
// branch / time-travel.
//
// State must stay serializable: Messages, Todos and KV round-trip through JSON;
// Files is a pluggable backend handle, excluded from inline serialization.
type State struct {
	Messages []Message      `json:"messages,omitempty"`
	Todos    []Todo         `json:"todos,omitempty"`
	Files    FileStore      `json:"-"`
	KV       map[string]any `json:"kv,omitempty"`
}

// FileStore is the minimal virtual-filesystem contract State depends on. It is
// declared in core so core stays dependency-free; concrete backends live in the
// vfs package.
type FileStore interface {
	Read(path string) ([]byte, error)
	Write(path string, data []byte) error
	List(prefix string) ([]string, error)
}

// Snapshottable is an optional FileStore capability (ADR-0026): the backend can
// export its full contents as a portable path→bytes map, so a durable
// checkpointer can persist the file state alongside the rest of State. Files is
// excluded from State's own JSON (it is a pluggable handle); the File
// checkpointer instead stores snapshots as content-addressed blob records. A
// backend that cannot export itself (a real directory, a remote store) omits
// this interface and is treated as externally managed: the caller re-supplies
// it via agent.WithRunFiles on resume.
type Snapshottable interface {
	Snapshot() map[string][]byte
}

// Restorable is the counterpart of Snapshottable: the backend can replace its
// contents from a map produced by Snapshot. The agent layer uses it to rebuild
// a restored run's file state.
type Restorable interface {
	Restore(files map[string][]byte) error
}

// Todo is one planning item backing the write_todos tool (deepagents-style
// planning that keeps a long-horizon agent focused).
type Todo struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Status string `json:"status"` // "pending" | "in_progress" | "completed"
}

// StateOp is a single declarative mutation a tool requests via Result.State.
// The loop applies ops immediately and visibly (vs v1's deferred Actions
// merge), so a tool reads back its own writes within the same turn.
type StateOp struct {
	Kind  StateOpKind
	Key   string
	Value any
}

// StateOpKind enumerates the supported mutations.
type StateOpKind int

const (
	// OpSetKV sets State.KV[Key] = Value.
	OpSetKV StateOpKind = iota
	// OpAddTodo appends Value (a Todo) to State.Todos.
	OpAddTodo
	// OpSetTodos replaces State.Todos with Value ([]Todo).
	OpSetTodos
)

// Apply folds the given ops into the State in order.
func (s *State) Apply(ops ...StateOp) {
	for _, op := range ops {
		switch op.Kind {
		case OpSetKV:
			if s.KV == nil {
				s.KV = map[string]any{}
			}
			s.KV[op.Key] = op.Value
		case OpAddTodo:
			if t, ok := op.Value.(Todo); ok {
				s.Todos = append(s.Todos, t)
			}
		case OpSetTodos:
			if ts, ok := op.Value.([]Todo); ok {
				s.Todos = ts
			}
		}
	}
}
