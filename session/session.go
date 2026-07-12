// Package session models conversation history and state. History is an
// append-only event log, but each event carries a ParentID so the log forms a
// tree: the active conversation a model sees is the path from the active leaf
// back to the root. A purely linear session is the degenerate tree where every
// event's parent is its predecessor, so existing linear usage is unchanged.
// Branch switching, fork, and re-summarization (see docs/SESSION-TREE.md) layer
// on top of this model without breaking the Store contract.
package session

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/jiujuan/goagent/core"
)

// State is a key/value store scoped to a session. Keys may carry a scope
// prefix: "app:" (shared across all users), "user:" (shared across a user's
// sessions), or "temp:" (discarded when the invocation ends). Unprefixed keys
// are session-scoped.
type StateReader interface {
	Get(key string) (any, bool)
	All() map[string]any
}

// State is the mutable state view exposed to legacy tools and executors. Every
// implementation must be safe for concurrent use. Values are immutable by
// contract after Set; callers update composite values by replacing them.
type State interface {
	StateReader
	Set(key string, value any)
	Delete(key string)
}

// mapState is the default in-memory State.
type mapState struct {
	mu sync.RWMutex
	m  map[string]any
}

// NewState returns an empty in-memory State.
func NewState() State { return &mapState{m: map[string]any{}} }

func (s *mapState) Get(k string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[k]
	return v, ok
}

func (s *mapState) Set(k string, v any) {
	s.mu.Lock()
	s.m[k] = v
	s.mu.Unlock()
}

func (s *mapState) Delete(k string) {
	s.mu.Lock()
	delete(s.m, k)
	s.mu.Unlock()
}

func (s *mapState) All() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.m)
}

// Session is one conversation: identity, mutable state, and an append-only
// event tree. events holds every committed event across all branches in commit
// order; byID indexes them; leaf is the active branch tip. The active history
// (Messages/Events) is the path from leaf back to the root.
type Session struct {
	ID      string
	AppName string
	UserID  string

	mu             sync.RWMutex
	revision       uint64
	state          map[string]any
	stateAPI       State
	events         []*core.Event
	byID           map[string]*core.Event
	leaf           string // ID of the active leaf event ("" when empty)
	invocationGate chan struct{}
}

// newSession constructs an empty Session.
func newSession(appName, userID, id string) *Session {
	s := &Session{
		ID:      id,
		AppName: appName,
		UserID:  userID,
		state:   map[string]any{},
		byID:    map[string]*core.Event{},
	}
	s.stateAPI = &sessionState{session: s}
	s.invocationGate = make(chan struct{}, 1)
	s.invocationGate <- struct{}{}
	return s
}

// BeginInvocation serializes top-level runs for this Session while allowing
// different sessions to run concurrently. The returned release function is
// idempotent and must be deferred by the caller. Waiting honors cancellation.
func (s *Session) BeginInvocation(ctx context.Context) (release func(), err error) {
	select {
	case <-s.invocationGate:
		var once sync.Once
		return func() {
			once.Do(func() { s.invocationGate <- struct{}{} })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// State returns the session's mutable state.
func (s *Session) State() State { return s.stateAPI }

// Leaf returns the ID of the active leaf event (empty for an empty session).
func (s *Session) Leaf() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaf
}

// Revision returns the current in-process revision. It advances after every
// committed event, checkout, and direct State mutation.
func (s *Session) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// Snapshot atomically captures history and state from the same revision.
func (s *Session) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	path := s.pathToLocked(s.leaf)
	return Snapshot{
		id:       s.ID,
		appName:  s.AppName,
		userID:   s.UserID,
		revision: s.revision,
		leaf:     s.leaf,
		events:   cloneEvents(path),
		messages: projectMessages(path),
		state:    maps.Clone(s.state),
	}
}

// activePath returns the events on the active branch, root-first.
func (s *Session) activePath() []*core.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneEvents(s.pathToLocked(s.leaf))
}

// pathTo walks from the event id back to the root via ParentID and returns the
// events root-first. Unknown or empty ids yield an empty path.
func (s *Session) pathTo(id string) []*core.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneEvents(s.pathToLocked(id))
}

func (s *Session) pathToLocked(id string) []*core.Event {
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
// returned events are detached copies and never alias the committed log.
func (s *Session) Events() []*core.Event { return s.activePath() }

// allEvents returns detached copies of every committed event in commit order.
func (s *Session) allEvents() []*core.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneEvents(s.events)
}

// Messages projects the active branch to the message history a model sees,
// dropping events without a message (e.g. pure control events). If the path
// contains a summary node (Event.SummarizesTo), the one nearest the leaf takes
// effect: the prefix it covers is replaced by its summary message, and only the
// events after the cut are emitted verbatim. See docs/SESSION-TREE.md.
func (s *Session) Messages() []core.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return projectMessages(s.pathToLocked(s.leaf))
}

func projectMessages(path []*core.Event) []core.Message {

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
		msgs = append(msgs, core.CloneMessage(*summaryMsg))
		for i := cut + 1; i < len(path); i++ {
			if path[i].SummarizesTo != "" {
				continue // skip summary markers; their text already emitted (or superseded)
			}
			if path[i].Message != nil {
				msgs = append(msgs, core.CloneMessage(*path[i].Message))
			}
		}
		return msgs
	}

	for _, e := range path {
		if e.Message != nil {
			msgs = append(msgs, core.CloneMessage(*e.Message))
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitLocked(e)
}

func (s *Session) commitLocked(e *core.Event) {
	if e.ParentID == "" {
		e.ParentID = s.leaf
	}
	if s.byID == nil {
		s.byID = map[string]*core.Event{}
	}
	owned := core.CloneEvent(e)
	s.byID[owned.ID] = owned
	s.events = append(s.events, owned)
	s.leaf = e.ID

	for k, v := range e.Actions.StateDelta {
		if strings.HasPrefix(k, "temp:") {
			continue
		}
		s.state[k] = v
	}
	s.revision++
}

// stateAlong builds the State derived from replaying the path ending at the
// given leaf event, root-first. It is the path-correct way to recompute state
// after switching the active branch (temp: keys are dropped). Phase-1 linear
// commits keep state incrementally instead; this is the mechanism branch
// switching (phase 2) uses.
func (s *Session) stateAlong(leaf string) State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := NewState()
	for k, v := range s.stateAlongLocked(leaf) {
		st.Set(k, v)
	}
	return st
}

func (s *Session) stateAlongLocked(leaf string) map[string]any {
	state := map[string]any{}
	for _, e := range s.pathToLocked(leaf) {
		for k, v := range e.Actions.StateDelta {
			if strings.HasPrefix(k, "temp:") {
				continue
			}
			state[k] = v
		}
	}
	return state
}

func cloneEvents(events []*core.Event) []*core.Event {
	out := make([]*core.Event, len(events))
	for i, event := range events {
		out[i] = core.CloneEvent(event)
	}
	return out
}

// sessionState routes every direct State operation through the owning Session
// lock. Keeping state and event-tree mutations under one lock lets Snapshot
// capture both from a single revision.
type sessionState struct {
	session *Session
}

func (s *sessionState) Get(key string) (any, bool) {
	s.session.mu.RLock()
	defer s.session.mu.RUnlock()
	value, ok := s.session.state[key]
	return value, ok
}

func (s *sessionState) Set(key string, value any) {
	s.session.mu.Lock()
	s.session.state[key] = value
	s.session.revision++
	s.session.mu.Unlock()
}

func (s *sessionState) Delete(key string) {
	s.session.mu.Lock()
	delete(s.session.state, key)
	s.session.revision++
	s.session.mu.Unlock()
}

func (s *sessionState) All() map[string]any {
	s.session.mu.RLock()
	defer s.session.mu.RUnlock()
	return maps.Clone(s.session.state)
}

var _ State = (*sessionState)(nil)
