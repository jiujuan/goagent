package session

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jiujuan/goagent/core"
)

// FileStore persists each session as an append-only JSONL file: one event per
// line. On load it replays the events to reconstruct both the message log and
// the derived state, so sessions survive process restarts. It keeps an
// in-memory cache so a loaded session is reused (and stays consistent with what
// is on disk) for the rest of the process lifetime.
//
// Layout: <dir>/<appName>/<userID>/<sessionID>.jsonl (path segments sanitized).
type FileStore struct {
	dir   string
	mu    sync.Mutex
	cache map[string]*Session
}

// NewFileStore creates a FileStore rooted at dir, creating the directory if
// needed.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("session: create store dir: %w", err)
	}
	return &FileStore{dir: dir, cache: map[string]*Session{}}, nil
}

// GetOrCreate implements Store. It returns the cached session, else loads it
// from disk (replaying events), else creates an empty one.
func (st *FileStore) GetOrCreate(_ context.Context, appName, userID, sessionID string) (*Session, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	k := key(appName, userID, sessionID)
	if s, ok := st.cache[k]; ok {
		return s, nil
	}
	s, err := st.load(appName, userID, sessionID)
	if err != nil {
		return nil, err
	}
	st.cache[k] = s
	return s, nil
}

// Append implements Store: it writes the event as one JSONL line and commits it
// to the in-memory session.
func (st *FileStore) Append(_ context.Context, s *Session, e *core.Event) error {
	// Hold the Session lock across parent selection, durable append, and the
	// in-memory commit. This serializes one session without blocking unrelated
	// sessions on FileStore's cache mutex.
	s.mu.Lock()
	defer s.mu.Unlock()

	if e.ID == "" {
		e.ID = core.NewID("evt")
	}
	// Stamp the parent before marshaling so the persisted line records the tree
	// edge; commit would otherwise set it after we've already written the JSON.
	if e.ParentID == "" {
		e.ParentID = s.leaf
	}
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("session: marshal event: %w", err)
	}

	path := st.path(s.AppName, s.UserID, s.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("session: create session dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("session: open session file: %w", err)
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return fmt.Errorf("session: write event: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("session: close session file: %w", err)
	}

	s.commitLocked(e)
	// Record the active leaf so a reload restores it. After an append the leaf
	// is the just-written event; after a prior Checkout this is what keeps the
	// active branch authoritative rather than "last line in the file".
	return st.writeRefsLocked(s)
}

// Checkout implements TreeStore: it moves the active leaf and persists it.
func (st *FileStore) Checkout(_ context.Context, s *Session, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkoutLocked(eventID); err != nil {
		return err
	}
	return st.writeRefsLocked(s)
}

// Fork implements TreeStore: it copies the root..fromEventID path into a new
// session, writes it to its own JSONL file, and caches it.
func (st *FileStore) Fork(_ context.Context, s *Session, fromEventID, newSessionID string) (*Session, error) {
	events, err := s.forkEvents(fromEventID)
	if err != nil {
		return nil, err
	}
	// Materializing a new cache key must be atomic with respect to GetOrCreate
	// for that key. Fork is rare, so the simple cache lock is preferable to
	// publishing a partially-written session or adding per-key lock machinery.
	st.mu.Lock()
	defer st.mu.Unlock()
	ns := seedSession(s.AppName, s.UserID, newSessionID, events)
	if err := st.writeAll(ns, events); err != nil {
		return nil, err
	}
	if err := st.writeRefs(ns); err != nil {
		return nil, err
	}
	st.cache[key(s.AppName, s.UserID, newSessionID)] = ns
	return ns, nil
}

// Branches implements TreeStore.
func (st *FileStore) Branches(_ context.Context, s *Session) ([]Ref, error) {
	return s.branchRefs(), nil
}

// writeAll (re)writes a session's whole JSONL file from the given events. Used
// by Fork to materialize a new session file.
func (st *FileStore) writeAll(s *Session, events []*core.Event) error {
	path := st.path(s.AppName, s.UserID, s.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("session: create session dir: %w", err)
	}
	var buf bytes.Buffer
	for _, e := range events {
		line, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("session: marshal event: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("session: write session file: %w", err)
	}
	return nil
}

// refsFile is the sidecar that records a session's mutable active-leaf pointer,
// keeping the event JSONL itself purely append-only.
type refsFile struct {
	Leaf string `json:"leaf"`
}

func (st *FileStore) refsPath(appName, userID, sessionID string) string {
	return filepath.Join(st.dir, safeSegment(appName), safeSegment(userID), safeSegment(sessionID)+".refs.json")
}

func (st *FileStore) writeRefs(s *Session) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return st.writeRefsLocked(s)
}

func (st *FileStore) writeRefsLocked(s *Session) error {
	path := st.refsPath(s.AppName, s.UserID, s.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("session: create session dir: %w", err)
	}
	data, err := json.Marshal(refsFile{Leaf: s.leaf})
	if err != nil {
		return fmt.Errorf("session: marshal refs: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("session: write refs: %w", err)
	}
	return nil
}

// readRefs returns the persisted active leaf, ok=false if no sidecar exists
// (e.g. a pre-tree session file).
func (st *FileStore) readRefs(appName, userID, sessionID string) (leaf string, ok bool, err error) {
	data, rerr := os.ReadFile(st.refsPath(appName, userID, sessionID))
	if errors.Is(rerr, os.ErrNotExist) {
		return "", false, nil
	}
	if rerr != nil {
		return "", false, fmt.Errorf("session: read refs: %w", rerr)
	}
	var rf refsFile
	if uerr := json.Unmarshal(data, &rf); uerr != nil {
		return "", false, fmt.Errorf("session: parse refs: %w", uerr)
	}
	return rf.Leaf, true, nil
}

// load reads a session's JSONL file (if present) and replays each event to
// rebuild the message tree and state.
func (st *FileStore) load(appName, userID, sessionID string) (*Session, error) {
	s := newSession(appName, userID, sessionID)
	path := st.path(appName, userID, sessionID)

	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("session: open session file: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		raw := bytes.TrimSpace(sc.Bytes())
		if len(raw) == 0 {
			continue
		}
		var e core.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("session: parse %s line %d: %w", path, line, err)
		}
		s.commit(&e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("session: read %s: %w", path, err)
	}

	// Restore the persisted active leaf (from a Checkout); absent for pre-tree
	// files, where the leaf stays the last replayed event.
	leaf, ok, err := st.readRefs(appName, userID, sessionID)
	if err != nil {
		return nil, err
	}
	if ok && leaf != "" {
		s.mu.Lock()
		if _, known := s.byID[leaf]; known {
			s.leaf = leaf
		}
		s.mu.Unlock()
	}
	// Replay rebuilt state incrementally across all lines, which mixes branches
	// for a non-linear log; recompute it along the active path to be correct.
	s.mu.Lock()
	s.state = s.stateAlongLocked(s.leaf)
	s.mu.Unlock()
	return s, nil
}

func (st *FileStore) path(appName, userID, sessionID string) string {
	return filepath.Join(st.dir, safeSegment(appName), safeSegment(userID), safeSegment(sessionID)+".jsonl")
}

// safeSegment makes a key segment safe to use as a single path component.
func safeSegment(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "..", "_")
	if s == "" {
		return "_"
	}
	return s
}

var _ Store = (*FileStore)(nil)
var _ TreeStore = (*FileStore)(nil)
