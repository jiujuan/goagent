package session

import (
	"context"
	"sync"

	"github.com/jiujuan/goagent/core"
)

// Store persists sessions and commits events to them. Implementations may be
// in-memory, file-backed (JSONL), or remote; the Runner depends only on this
// interface.
type Store interface {
	// GetOrCreate returns the session, creating an empty one if absent.
	GetOrCreate(ctx context.Context, appName, userID, sessionID string) (*Session, error)
	// Append commits an event: applies its side effects and persists it.
	Append(ctx context.Context, s *Session, e *core.Event) error
}

// InMemoryStore is a Store backed by a map. It is safe for concurrent use.
type InMemoryStore struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

// InMemory returns a new in-memory Store.
func InMemory() *InMemoryStore {
	return &InMemoryStore{sessions: map[string]*Session{}}
}

func key(appName, userID, sessionID string) string {
	return appName + "/" + userID + "/" + sessionID
}

// GetOrCreate implements Store.
func (st *InMemoryStore) GetOrCreate(_ context.Context, appName, userID, sessionID string) (*Session, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	k := key(appName, userID, sessionID)
	if s, ok := st.sessions[k]; ok {
		return s, nil
	}
	s := newSession(appName, userID, sessionID)
	st.sessions[k] = s
	return s, nil
}

// Append implements Store.
func (st *InMemoryStore) Append(_ context.Context, s *Session, e *core.Event) error {
	if e.ID == "" {
		e.ID = core.NewID("evt")
	}
	s.commit(e)
	return nil
}

// Checkout implements TreeStore.
func (st *InMemoryStore) Checkout(_ context.Context, s *Session, eventID string) error {
	return s.checkout(eventID)
}

// Fork implements TreeStore.
func (st *InMemoryStore) Fork(_ context.Context, s *Session, fromEventID, newSessionID string) (*Session, error) {
	events, err := s.forkEvents(fromEventID)
	if err != nil {
		return nil, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	ns := seedSession(s.AppName, s.UserID, newSessionID, events)
	st.sessions[key(s.AppName, s.UserID, newSessionID)] = ns
	return ns, nil
}

// Branches implements TreeStore.
func (st *InMemoryStore) Branches(_ context.Context, s *Session) ([]Ref, error) {
	return s.branchRefs(), nil
}

var _ TreeStore = (*InMemoryStore)(nil)
