package session

import (
	"maps"

	"github.com/jiujuan/goagent/core"
)

// Snapshot is an immutable, point-in-time view of one Session. It captures the
// active event path, its projected model messages, and state under the same
// Session read lock, so consumers never combine history from one revision with
// state from another. Accessors return detached framework values.
//
// State values are copied at the map boundary. Values stored in State must be
// treated as immutable after Set; update slices, maps, and pointers by replacing
// the whole value rather than mutating an object already owned by the Session.
type Snapshot struct {
	id       string
	appName  string
	userID   string
	revision uint64
	leaf     string
	events   []*core.Event
	messages []core.Message
	state    map[string]any
}

// ID returns the session identifier captured by the snapshot.
func (s Snapshot) ID() string { return s.id }

// AppName returns the application name captured by the snapshot.
func (s Snapshot) AppName() string { return s.appName }

// UserID returns the user identifier captured by the snapshot.
func (s Snapshot) UserID() string { return s.userID }

// Revision returns the monotonically increasing in-process session revision.
func (s Snapshot) Revision() uint64 { return s.revision }

// Leaf returns the active event tip captured by the snapshot.
func (s Snapshot) Leaf() string { return s.leaf }

// Events returns detached copies of the active event path.
func (s Snapshot) Events() []*core.Event { return cloneEvents(s.events) }

// Messages returns detached copies of the projected model history.
func (s Snapshot) Messages() []core.Message { return core.CloneMessages(s.messages) }

// State returns an immutable state view detached from the live Session.
func (s Snapshot) State() StateReader {
	return snapshotState{values: maps.Clone(s.state)}
}

// WithState returns a copy whose state view is replaced by state. Branch
// runtimes use it to combine committed branch history with an in-memory overlay.
func (s Snapshot) WithState(state StateReader) Snapshot {
	if state == nil {
		s.state = map[string]any{}
	} else {
		s.state = state.All()
	}
	return s
}

// SnapshotAt captures the logical history and path-derived state ending at an
// arbitrary committed event, including detached branch tips and merge nodes.
func (s *Session) SnapshotAt(eventID string) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	path := s.logicalPathToLocked(eventID)
	return Snapshot{
		id:       s.ID,
		appName:  s.AppName,
		userID:   s.UserID,
		revision: s.revision,
		leaf:     eventID,
		events:   cloneEvents(path),
		messages: projectMessages(path),
		state:    s.stateAlongLocked(eventID),
	}
}

type snapshotState struct {
	values map[string]any
}

func (s snapshotState) Get(key string) (any, bool) {
	value, ok := s.values[key]
	return value, ok
}

func (s snapshotState) All() map[string]any { return maps.Clone(s.values) }

var _ StateReader = snapshotState{}
