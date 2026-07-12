package session

import (
	"context"
	"fmt"
	"slices"

	"github.com/jiujuan/goagent/core"
)

// Ref identifies a branch tip in a session's event tree.
type Ref struct {
	Name        string // human-facing label derived from the leaf event ID
	LeafEventID string // the tip event
	Active      bool   // whether this tip is the session's current active leaf
}

// TreeStore is the backward-compatible name of the graph navigation extension.
// It adds branch switching, graph-closure fork, and tip enumeration on top of
// Store. Merge secondary edges are included when computing tips and fork data.
//
//	if ts, ok := store.(TreeStore); ok { ... }
type TreeStore interface {
	Store
	// Checkout moves the session's active leaf to eventID, so the next Append
	// branches from there and Messages/State reflect that node's path. Derived
	// state is rebuilt along the new path. It errors if eventID is unknown.
	Checkout(ctx context.Context, s *Session, eventID string) error
	// Fork creates a new session (newSessionID, same app/user) seeded with a
	// copy of the path from the root to fromEventID, leaving the original
	// untouched. The returned session's leaf is the copied fromEventID.
	Fork(ctx context.Context, s *Session, fromEventID, newSessionID string) (*Session, error)
	// Branches lists the tree's tips (events that are no event's parent),
	// marking which one is the active leaf.
	Branches(ctx context.Context, s *Session) ([]Ref, error)
}

// checkout moves the active leaf to id and rebuilds derived state along the new
// path. It errors if id is not a known event.
func (s *Session) checkout(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkoutLocked(id)
}

func (s *Session) checkoutLocked(id string) error {
	if _, ok := s.byID[id]; !ok {
		return fmt.Errorf("session: checkout unknown event %q", id)
	}
	s.leaf = id
	s.state = s.stateAlongLocked(id)
	s.revision++
	return nil
}

// tips returns the IDs of events that are no event's parent (the tree's
// leaves), sorted for determinism.
func (s *Session) tips() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tipsLocked()
}

func (s *Session) tipsLocked() []string {
	hasChild := make(map[string]bool, len(s.events))
	for _, e := range s.events {
		if e.ParentID != "" {
			hasChild[e.ParentID] = true
		}
		for _, parent := range e.MergeParents {
			hasChild[parent] = true
		}
	}
	var tips []string
	for _, e := range s.events {
		if !hasChild[e.ID] {
			tips = append(tips, e.ID)
		}
	}
	slices.Sort(tips)
	return tips
}

// branchRefs builds the branch tip list, marking the active leaf.
func (s *Session) branchRefs() []Ref {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.branchRefsLocked()
}

func (s *Session) branchRefsLocked() []Ref {
	tips := s.tipsLocked()
	if s.leaf != "" && !slices.Contains(tips, s.leaf) {
		tips = append(tips, s.leaf)
		slices.Sort(tips)
	}
	refs := make([]Ref, 0, len(tips))
	for _, id := range tips {
		refs = append(refs, Ref{Name: branchName(id), LeafEventID: id, Active: id == s.leaf})
	}
	return refs
}

// branchName derives a short human-facing label for a tip event ID.
func branchName(id string) string {
	short := id
	if len(short) > 8 {
		short = short[len(short)-8:]
	}
	return "branch-" + short
}

// forkEvents returns detached copies of the events on the path root..fromID,
// root-first, for seeding a forked session. It errors if fromID is unknown.
// Events are immutable post-commit, so a shallow struct copy is enough to keep
// the two sessions from sharing mutable index state.
func (s *Session) forkEvents(fromID string) ([]*core.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.byID[fromID]; !ok {
		return nil, fmt.Errorf("session: fork from unknown event %q", fromID)
	}
	// A merge node depends on every detached branch reachable through its
	// secondary parents. Copy the logical projection, which is a deterministic
	// topological closure containing all referenced events before the merge.
	path := s.logicalPathToLocked(fromID)
	out := make([]*core.Event, len(path))
	for i, e := range path {
		out[i] = core.CloneEvent(e)
	}
	return out, nil
}

// seedSession builds a fresh Session and replays events (root-first, each
// carrying its ParentID) through commit, reconstructing the tree index, leaf,
// and derived state.
func seedSession(appName, userID, id string, events []*core.Event) *Session {
	ns := newSession(appName, userID, id)
	for _, e := range events {
		ns.commit(e)
	}
	return ns
}
