// Package session models conversation history and state. History is an
// append-only event log, but each event carries a ParentID so the log forms a
// tree: the active conversation a model sees is the path from the active leaf
// back to the root. A purely linear session is the degenerate tree where every
// event's parent is its predecessor, so existing linear usage is unchanged.
// Branch switching, fork, and re-summarization (see docs/SESSION-TREE.md) layer
// on top of this model without breaking the Store contract.
package session

import (
	"maps"
	"slices"
	"strings"

	"github.com/jiujuan/goagent/core"
)

// State is a key/value store scoped to a session. Keys may carry a scope
// prefix: "app:" (shared across all users), "user:" (shared across a user's
// sessions), or "temp:" (discarded when the invocation ends). Unprefixed keys
// are session-scoped.
type State interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Delete(key string)
	All() map[string]any
}

// mapState is the default in-memory State.
type mapState struct {
	m map[string]any
}

// NewState returns an empty in-memory State.
func NewState() State { return &mapState{m: map[string]any{}} }

func (s *mapState) Get(k string) (any, bool) { v, ok := s.m[k]; return v, ok }
func (s *mapState) Set(k string, v any)      { s.m[k] = v }
func (s *mapState) Delete(k string)          { delete(s.m, k) }
func (s *mapState) All() map[string]any      { return maps.Clone(s.m) }

// Session is one conversation: identity, mutable state, and an append-only
// event tree. events holds every committed event across all branches in commit
// order; byID indexes them; leaf is the active branch tip. The active history
// (Messages/Events) is the path from leaf back to the root.
type Session struct {
	ID      string
	AppName string
	UserID  string

	state  State
	events []*core.Event
	byID   map[string]*core.Event
	leaf   string // ID of the active leaf event ("" when empty)
}

// newSession constructs an empty Session.
func newSession(appName, userID, id string) *Session {
	return &Session{
		ID:      id,
		AppName: appName,
		UserID:  userID,
		state:   NewState(),
		byID:    map[string]*core.Event{},
	}
}

// State returns the session's mutable state.
func (s *Session) State() State { return s.state }

// Leaf returns the ID of the active leaf event (empty for an empty session).
func (s *Session) Leaf() string { return s.leaf }

// activePath returns the events on the active branch, root-first.
func (s *Session) activePath() []*core.Event {
	return s.pathTo(s.leaf)
}

// pathTo walks from the event id back to the root via ParentID and returns the
// events root-first. Unknown or empty ids yield an empty path.
func (s *Session) pathTo(id string) []*core.Event {
	var rev []*core.Event
	seen := map[string]bool{} // guard against a malformed cyclic log
	for id != "" && !seen[id] {
		seen[id] = true
		e, ok := s.byID[id]
		if !ok {
			break
		}
		rev = append(rev, e)
		id = e.ParentID
	}
	slices.Reverse(rev)
	return rev
}

// Events returns the committed events on the active branch (root-first). The
// returned slice is freshly built; callers must not assume it aliases internal
// storage, and must not mutate the events.
func (s *Session) Events() []*core.Event { return s.activePath() }

// Messages projects the active branch to the message history a model sees,
// dropping events without a message (e.g. pure control events). If the path
// contains a summary node (Event.SummarizesTo), the one nearest the leaf takes
// effect: the prefix it covers is replaced by its summary message, and only the
// events after the cut are emitted verbatim. See docs/SESSION-TREE.md.
func (s *Session) Messages() []core.Message {
	path := s.activePath()

	// Find the summary node nearest the leaf and the index of the event it cuts.
	cut, summaryMsg := -1, (*core.Message)(nil)
	for i := len(path) - 1; i >= 0; i-- {
		if path[i].SummarizesTo == "" {
			continue
		}
		if ci := indexOfEvent(path, path[i].SummarizesTo); ci >= 0 {
			cut, summaryMsg = ci, path[i].Message
		}
		break
	}

	msgs := make([]core.Message, 0, len(path))
	if summaryMsg != nil {
		msgs = append(msgs, *summaryMsg)
		for i := cut + 1; i < len(path); i++ {
			if path[i].SummarizesTo != "" {
				continue // skip summary markers; their text already emitted (or superseded)
			}
			if path[i].Message != nil {
				msgs = append(msgs, *path[i].Message)
			}
		}
		return msgs
	}

	for _, e := range path {
		if e.Message != nil {
			msgs = append(msgs, *e.Message)
		}
	}
	return msgs
}

// indexOfEvent returns the index of the event with the given ID in path, or -1.
func indexOfEvent(path []*core.Event, id string) int {
	for i, e := range path {
		if e.ID == id {
			return i
		}
	}
	return -1
}

// commit appends an event to the tree under the current leaf (unless it already
// names a parent), advances the leaf, and applies the event's state delta.
// temp:-scoped state keys are not persisted.
//
// State is applied incrementally here, which for an append at the current leaf
// is identical to replaying the whole active path. Branch switching (which
// changes the active path without a linear append) must instead rebuild state
// with stateAlong; see docs/SESSION-TREE.md.
func (s *Session) commit(e *core.Event) {
	if e.ParentID == "" {
		e.ParentID = s.leaf
	}
	if s.byID == nil {
		s.byID = map[string]*core.Event{}
	}
	s.byID[e.ID] = e
	s.events = append(s.events, e)
	s.leaf = e.ID

	for k, v := range e.Actions.StateDelta {
		if strings.HasPrefix(k, "temp:") {
			continue
		}
		s.state.Set(k, v)
	}
}

// stateAlong builds the State derived from replaying the path ending at the
// given leaf event, root-first. It is the path-correct way to recompute state
// after switching the active branch (temp: keys are dropped). Phase-1 linear
// commits keep state incrementally instead; this is the mechanism branch
// switching (phase 2) uses.
func (s *Session) stateAlong(leaf string) State {
	st := NewState()
	for _, e := range s.pathTo(leaf) {
		for k, v := range e.Actions.StateDelta {
			if strings.HasPrefix(k, "temp:") {
				continue
			}
			st.Set(k, v)
		}
	}
	return st
}
