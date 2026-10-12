package runstore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// runFormatVersion is the on-disk format of a run journal line. A line whose version is
// not this one is refused rather than guessed at, because a run record drives scheduling:
// reading a future format as if it were this one could hand a worker a stale status
// (ADR-0034 D5).
const runFormatVersion = 1

const eventKindRecord = "record"

// maxRunIDLen bounds a RunID, which is used verbatim as one journal name and one lock name
// inside the runs directory. Same bound as a checkpoint thread id, so one caller can use
// either kind of id as a file name.
const maxRunIDLen = 64

// journalEvent is one append-only line: the record as it stood after the write, plus the
// version it replaced. PrevVersion lets replay detect a duplicate append of the same event
// after a failed Sync, so the same event never counts twice.
type journalEvent struct {
	Version     int        `json:"version"`
	Kind        string     `json:"kind"`
	PrevVersion uint64     `json:"prev_version"`
	Record      *RunRecord `json:"record,omitempty"`
}

// File keeps one append-only JSONL journal per RunID under a "runs" subdirectory of the
// store root, each with its own cross-process lock file. It shares nothing with the
// checkpoint journal: a run record is execution progress, and writing heartbeats into
// checkpoints would move a thread head without any state behind it (ADR-0034 D2).
//
// There is no read cache. Every operation re-reads the journal inside the lock, so two
// File instances — in one process or in two — cannot disagree about the current record by
// trusting a mutex or a stale map. A write is published only after its append and Sync
// succeed, which is why a failed write never reports success.
type File struct {
	dir  string
	runs string
	mu   sync.Mutex
	ops  fileOps
}

type fileOps struct {
	write func(*os.File, []byte) (int, error)
	sync  func(*os.File) error
}

var _ Store = (*File)(nil)

func defaultFileOps() fileOps {
	return fileOps{
		write: func(fh *os.File, b []byte) (int, error) { return fh.Write(b) },
		sync:  func(fh *os.File) error { return fh.Sync() },
	}
}

// NewFile opens, creating if needed, a file-backed run store rooted at dir. Records live
// in dir/runs, so the same directory can hold a checkpoint store without either side
// touching the other's files or lock names.
func NewFile(dir string) (*File, error) {
	runs := filepath.Join(dir, "runs")
	if err := os.MkdirAll(runs, 0o755); err != nil {
		return nil, fmt.Errorf("runstore: create runs dir: %w", err)
	}
	return &File{dir: dir, runs: runs, ops: defaultFileOps()}, nil
}

// path resolves a RunID to its journal, refusing anything that could escape the runs
// directory.
func (f *File) path(runID string) (string, error) {
	if err := checkRunID(runID); err != nil {
		return "", err
	}
	return filepath.Join(f.runs, runID+".jsonl"), nil
}

func (f *File) lockPath(runID string) (string, error) {
	if err := checkRunID(runID); err != nil {
		return "", err
	}
	return filepath.Join(f.runs, runID+".lock"), nil
}

// checkRunID accepts only characters that are safe as a file name on every supported
// platform. A RunID comes from the caller, so an id carrying path syntax would place one
// run's record outside the store.
func checkRunID(id string) error {
	if id == "" {
		return fmt.Errorf("runstore: run id is empty: %w", ErrInvalidRecord)
	}
	if len(id) > maxRunIDLen {
		return fmt.Errorf("runstore: run id %q is %d bytes; the limit is %d: %w", id, len(id), maxRunIDLen, ErrInvalidRecord)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return fmt.Errorf("runstore: run id %q contains %q; only A-Z a-z 0-9 - _ are allowed: %w", id, r, ErrInvalidRecord)
		}
	}
	return nil
}

// Create records a new run. See Store.Create.
func (f *File) Create(ctx context.Context, record RunRecord) (RunRecord, error) {
	var out RunRecord
	err := f.withRun(ctx, record.RunID, func(latest *RunRecord) error {
		created, err := prepareCreate(latest, record, nowStamp())
		if err != nil {
			return err
		}
		if latest != nil && created.Version == latest.Version {
			// Replay of a record that is already durable: nothing to append.
			out = created
			return nil
		}
		if err := f.appendEventLocked(record.RunID, created); err != nil {
			return err
		}
		out = created
		return nil
	})
	return out, err
}

// Get reads one record without writing anything. It repairs nothing: a torn tail is left
// for the next writer to truncate, so a read-only client cannot alter a store it may not
// own. See Store.Get.
func (f *File) Get(ctx context.Context, runID string) (RunRecord, error) {
	var out RunRecord
	err := f.withRunRead(ctx, runID, func(latest *RunRecord) error {
		if latest == nil {
			return runNotFound(runID)
		}
		cloned, err := latest.clone()
		if err != nil {
			return err
		}
		out = cloned
		return nil
	})
	return out, err
}

// CompareAndSwap replaces a record only at the version the caller read. See Store.
func (f *File) CompareAndSwap(ctx context.Context, runID string, expectedVersion uint64, next RunRecord) (RunRecord, error) {
	var out RunRecord
	err := f.withRun(ctx, runID, func(latest *RunRecord) error {
		updated, err := prepareCAS(latest, runID, expectedVersion, next, nowStamp())
		if err != nil {
			return err
		}
		if latest != nil && updated.Version == latest.Version {
			// A terminal retry of the same outcome: it is already durable, so appending
			// another event would be a rewrite.
			out = updated
			return nil
		}
		if err := f.appendEventLocked(runID, updated); err != nil {
			return err
		}
		out = updated
		return nil
	})
	return out, err
}

// withRun is the only lock order in this store: process-local mutex, then the per-run lock
// file. The critical section covers the scan, the tail repair and the append, so a second
// instance cannot observe a state that the first is about to replace.
func (f *File) withRun(ctx context.Context, runID string, fn func(latest *RunRecord) error) error {
	if _, err := f.path(runID); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	release, err := f.acquireProcessLock(ctx, runID)
	if err != nil {
		return err
	}
	defer release()
	latest, tailOffset, err := f.scanLocked(runID)
	if err != nil {
		return err
	}
	if tailOffset >= 0 {
		fp, err := f.path(runID)
		if err != nil {
			return err
		}
		if err := os.Truncate(fp, tailOffset); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("runstore: repair truncated tail: %w", err)
		}
	}
	return fn(latest)
}

// withRunRead takes the same locks but never repairs the journal.
func (f *File) withRunRead(ctx context.Context, runID string, fn func(latest *RunRecord) error) error {
	if _, err := f.path(runID); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	release, err := f.acquireProcessLock(ctx, runID)
	if err != nil {
		return err
	}
	defer release()
	latest, _, err := f.scanLocked(runID)
	if err != nil {
		return err
	}
	return fn(latest)
}

// scanLocked replays a journal into the record it currently describes. A final line
// without a newline is a torn tail: it returns that line's start offset so the writer can
// truncate it. A malformed line that did end with a newline, an unknown format version, a
// broken version chain or a record for another run is corruption in the middle and is
// reported.
func (f *File) scanLocked(runID string) (*RunRecord, int64, error) {
	fp, err := f.path(runID)
	if err != nil {
		return nil, -1, err
	}
	fh, err := os.Open(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, -1, nil
		}
		return nil, -1, err
	}
	defer fh.Close()

	var (
		latest *RunRecord
		last   *journalEvent
		offset int64
	)
	r := bufio.NewReader(fh)
	for {
		lineStart := offset
		line, readErr := r.ReadBytes('\n')
		if len(line) > 0 {
			if readErr == io.EOF {
				// Half a line at the end: the record it would have published is not
				// durable, so the journal keeps describing the previous one.
				return latest, lineStart, nil
			}
			offset += int64(len(line))
			line = line[:len(line)-1]
			if len(line) == 0 {
				if readErr != nil {
					return nil, -1, readErr
				}
				continue
			}
			ev, err := decodeEvent(runID, line)
			if err != nil {
				return nil, -1, err
			}
			if last == nil {
				if ev.PrevVersion != 0 {
					return nil, -1, fmt.Errorf("runstore: journal for run %q starts by replacing version %d, want 0", runID, ev.PrevVersion)
				}
			} else if ev.PrevVersion == last.PrevVersion && sameRecord(ev.Record, last.Record) {
				// The same event twice: an append that was retried after a Sync error.
				// Replaying it must not advance the record again.
				continue
			} else if ev.PrevVersion != last.Record.Version {
				return nil, -1, fmt.Errorf("runstore: journal for run %q replaces version %d after %d", runID, ev.PrevVersion, last.Record.Version)
			}
			last = ev
			latest = ev.Record
		}
		if readErr != nil {
			if readErr == io.EOF {
				return latest, -1, nil
			}
			return nil, -1, readErr
		}
	}
}

func decodeEvent(runID string, line []byte) (*journalEvent, error) {
	var probe struct {
		Version int    `json:"version"`
		Kind    string `json:"kind"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		return nil, fmt.Errorf("runstore: corrupt line in run %q journal: %w", runID, err)
	}
	if probe.Version != runFormatVersion {
		return nil, fmt.Errorf("runstore: unsupported run journal version %d in run %q", probe.Version, runID)
	}
	if probe.Kind != eventKindRecord {
		return nil, fmt.Errorf("runstore: unknown run journal kind %q in run %q", probe.Kind, runID)
	}
	var ev journalEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return nil, fmt.Errorf("runstore: corrupt line in run %q journal: %w", runID, err)
	}
	if ev.Record == nil {
		return nil, fmt.Errorf("runstore: run %q journal line has no record", runID)
	}
	if ev.Record.RunID != runID {
		return nil, fmt.Errorf("runstore: run %q journal line carries run id %q", runID, ev.Record.RunID)
	}
	if err := validateShape(*ev.Record); err != nil {
		return nil, fmt.Errorf("runstore: run %q journal line is not a valid record: %w", runID, err)
	}
	if ev.PrevVersion+1 != ev.Record.Version {
		return nil, fmt.Errorf("runstore: run %q version %d claims it replaced %d", runID, ev.Record.Version, ev.PrevVersion)
	}
	return &ev, nil
}

// sameRecord compares two replayed records by their encoded content, so a duplicate line
// must be byte-identical to the one it repeats.
func sameRecord(a, b *RunRecord) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(left) == string(right)
}

// appendEventLocked publishes one record by appending its line and syncing the file. It
// does not touch any state the caller has not already committed to: if this returns an
// error, the caller reports the write as failed.
func (f *File) appendEventLocked(runID string, record RunRecord) error {
	fp, err := f.path(runID)
	if err != nil {
		return err
	}
	if record.Version == 0 {
		return fmt.Errorf("runstore: run %q record has no version: %w", runID, ErrInvalidRecord)
	}
	ev := journalEvent{
		Version:     runFormatVersion,
		Kind:        eventKindRecord,
		PrevVersion: record.Version - 1,
		Record:      &record,
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("runstore: marshal run %q event: %w", runID, err)
	}
	fh, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	if err := f.writeLine(fh, line); err != nil {
		return fmt.Errorf("runstore: append run %q event: %w", runID, err)
	}
	if err := f.ops.sync(fh); err != nil {
		return fmt.Errorf("runstore: sync run %q journal: %w", runID, err)
	}
	return nil
}

func (f *File) writeLine(fh *os.File, line []byte) error {
	buf := append(append([]byte(nil), line...), '\n')
	for len(buf) > 0 {
		n, err := f.ops.write(fh, buf)
		if n > 0 {
			buf = buf[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
