package checkpoint

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jiujuan/goagent/core"
)

// The File backend keeps a rebuildable summary projection beside each thread journal
// (ADR-0034 D7). The journal stays the only source of truth: the sidecar is derived from it,
// validated against it, and discarded whenever it disagrees. A query never needs the sidecar
// to be correct, only current or repairable.
//
// One sidecar line carries one summary plus the journal byte offset consumed through that
// record, so the last complete line states exactly how much journal is reflected. A write
// that died halfway leaves a line without a newline; it is ignored, and the previous line's
// offset still describes a valid prefix. Because each line is self-describing, a projection
// can only ever be appended to — there is no header to rewrite.

const (
	// metadataSidecarSuffix is the name tail of one thread's projection file: the journal
	// stays "<thread>.jsonl" and its sidecar is "<thread>.meta.jsonl".
	metadataSidecarSuffix = ".meta.jsonl"

	// metadataSidecarVersion is the sidecar layout this build writes and reads.
	metadataSidecarVersion = 1
)

// metadataSidecarLine is one appended line of the projection.
type metadataSidecarLine struct {
	V      int    `json:"v"`
	Thread string `json:"thread"`
	// Offset is the journal size, in bytes, consumed through this line.
	Offset int64 `json:"offset"`
	// Items counts the summaries the projection holds once this line is applied.
	Items int `json:"items"`
	// Meta is the summary this line adds. A line without one only advances Offset, which is
	// how a journal holding no new checkpoint stops being re-read.
	Meta *CheckpointMetadata `json:"meta,omitempty"`
}

// sidecarEntry is one derived summary together with the journal offset it consumed through.
type sidecarEntry struct {
	metadata CheckpointMetadata
	offset   int64
}

// metaFields are the only checkpoint fields a summary needs. The tags mirror Checkpoint's
// JSON, so one struct reads both a durable commit line (nested under "checkpoint") and a
// legacy top-level line. Nothing here names a State, a message part or a file blob, so
// decoding a line cannot pull the conversation into memory.
type metaFields struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	ParentID  string    `json:"parent_id"`
	ParentRef *Ref      `json:"parent_ref"`
	Revision  uint64    `json:"revision"`
	Step      int       `json:"step"`
	RunID     string    `json:"run_id"`
	TurnID    string    `json:"turn_id"`
	CreatedAt time.Time `json:"created_at"`
	// Pending, Pause and InFlight are kept as raw bytes only to ask whether each
	// is present, so a rebuild never decodes a tool batch it does not need.
	Pending    *json.RawMessage `json:"pending"`
	Pause      *json.RawMessage `json:"pause"`
	InFlight   *json.RawMessage `json:"in_flight"`
	Completion *struct {
		Kind string `json:"kind"`
	} `json:"completion"`
}

func (m *metaFields) paused() bool {
	return rawPresent(m.Pending) || rawPresent(m.Pause)
}

func (m *metaFields) inFlight() bool {
	return rawPresent(m.InFlight)
}

func rawPresent(raw *json.RawMessage) bool {
	if raw == nil {
		return false
	}
	trimmed := string(*raw)
	return trimmed != "" && trimmed != "null"
}

// sidecarPath resolves a thread id to its projection file.
func (f *File) sidecarPath(threadID string) (string, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return "", err
	}
	return filepath.Join(f.dir, threadID+metadataSidecarSuffix), nil
}

// threadMetadata returns one thread's summaries, repairing the projection when it is behind,
// corrupt, or newer than the journal. It uses the same lock order as the write path:
// process-local mutex, then the per-thread lock file.
func (f *File) threadMetadata(ctx context.Context, threadID string) ([]CheckpointMetadata, error) {
	entries, err := f.threadMetadataEntries(ctx, threadID)
	if err != nil {
		return nil, err
	}
	items := make([]CheckpointMetadata, 0, len(entries))
	for _, entry := range entries {
		items = append(items, entry.metadata)
	}
	return items, nil
}

// threadMetadataEntries is threadMetadata for callers that need the covered offsets too.
func (f *File) threadMetadataEntries(ctx context.Context, threadID string) ([]sidecarEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := f.path(threadID); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	release, err := f.acquireProcessLock(ctx, threadID)
	if err != nil {
		return nil, err
	}
	defer release()
	return f.refreshMetadataLocked(threadID)
}

// refreshMetadataLocked brings one thread's projection up to date with its journal. The
// caller holds the thread lock. A projection that cannot be read is rebuilt; a journal that
// grew is read from the offset the projection already covers; and a failed projection write
// is left behind rather than becoming a query or commit error.
func (f *File) refreshMetadataLocked(threadID string) ([]sidecarEntry, error) {
	journal, err := f.path(threadID)
	if err != nil {
		return nil, err
	}
	size, err := f.journalSize(journal)
	if err != nil {
		return nil, err
	}

	entries, offset, trustworthy := f.readSidecarLocked(threadID)
	rebuild := false
	if trustworthy && offset > 0 {
		// An offset into the middle of a line would read half a record, so a projection has
		// to prove it stopped on a boundary. A journal rewritten in place fails this.
		atBoundary, err := f.offsetAtLineBoundary(journal, offset)
		if err != nil {
			return nil, err
		}
		if !atBoundary {
			trustworthy = false
		}
	}
	if !trustworthy || offset > size {
		// Unreadable, or claiming more journal than exists: the journal wins.
		entries, offset = nil, 0
		rebuild = true
		f.noteRebuildLocked()
	}

	if offset < size {
		seed := uint64(0)
		if last := len(entries); last > 0 {
			seed = entries[last-1].metadata.Revision
		}
		known := make([]CheckpointMetadata, 0, len(entries))
		for _, entry := range entries {
			known = append(known, entry.metadata)
		}
		added, next, err := f.readJournalSummariesLocked(threadID, offset, seed)
		if err != nil {
			return nil, err
		}
		entries = append(entries, added...)
		offset = next
		if err := f.writeSidecarLocked(threadID, entries, added, offset, rebuild); err != nil {
			// The journal is the truth and this call already holds the right summaries, so a
			// failed projection write is not an error here: the next read re-derives it.
			f.noteSidecarWriteFailureLocked()
		}
		return entries, nil
	}

	if rebuild {
		if err := f.writeSidecarLocked(threadID, entries, nil, offset, true); err != nil {
			f.noteSidecarWriteFailureLocked()
		}
	}
	return entries, nil
}

// journalSize reports the journal length, treating an absent file as empty.
func (f *File) journalSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("checkpoint: stat journal: %w", err)
	}
	return info.Size(), nil
}

// offsetAtLineBoundary reports whether the byte just before offset is a newline, that is,
// whether the projection stopped at the end of a complete record. Reading from the middle of
// a line would invent a record the journal never committed.
func (f *File) offsetAtLineBoundary(path string, offset int64) (bool, error) {
	fh, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("checkpoint: open journal for boundary check: %w", err)
	}
	defer fh.Close()
	buf := make([]byte, 1)
	if _, err := fh.ReadAt(buf, offset-1); err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, fmt.Errorf("checkpoint: read journal boundary: %w", err)
	}
	return buf[0] == '\n', nil
}

// readSidecarLocked loads the projection. A file that is missing, written for another thread
// or layout, or whose line sequence does not add up yields trustworthy=false: the caller
// rebuilds from the journal rather than guessing which prefix was real.
func (f *File) readSidecarLocked(threadID string) (entries []sidecarEntry, offset int64, trustworthy bool) {
	path, err := f.sidecarPath(threadID)
	if err != nil {
		return nil, 0, false
	}
	fh, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, false
		}
		return nil, 0, false
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var parsed metadataSidecarLine
		if err := json.Unmarshal(line, &parsed); err != nil {
			return nil, 0, false
		}
		if parsed.V != metadataSidecarVersion || parsed.Thread != threadID {
			return nil, 0, false
		}
		if parsed.Offset < offset || parsed.Items != len(entries)+boolInt(parsed.Meta != nil) {
			return nil, 0, false
		}
		if parsed.Meta != nil {
			entries = append(entries, sidecarEntry{metadata: cloneCheckpointMetadata(*parsed.Meta), offset: parsed.Offset})
		}
		offset = parsed.Offset
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, false
	}
	return entries, offset, true
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// writeSidecarLocked persists the projection: rebuild rewrites the whole file because the
// old contents could not be trusted, otherwise only the newly derived lines are appended.
// Either way a final line without a summary advances the covered offset on its own.
func (f *File) writeSidecarLocked(threadID string, entries []sidecarEntry, added []sidecarEntry, offset int64, rebuild bool) error {
	if !rebuild && len(added) == 0 {
		return nil
	}
	path, err := f.sidecarPath(threadID)
	if err != nil {
		return err
	}
	var lines []*metadataSidecarLine
	if rebuild {
		for i := range entries {
			metadata := entries[i].metadata
			lines = append(lines, &metadataSidecarLine{
				V:      metadataSidecarVersion,
				Thread: threadID,
				Offset: entries[i].offset,
				Items:  i + 1,
				Meta:   &metadata,
			})
		}
	} else {
		start := len(entries) - len(added)
		for i := range added {
			metadata := added[i].metadata
			lines = append(lines, &metadataSidecarLine{
				V:      metadataSidecarVersion,
				Thread: threadID,
				Offset: added[i].offset,
				Items:  start + i + 1,
				Meta:   &metadata,
			})
		}
	}
	lines = append(lines, &metadataSidecarLine{
		V:      metadataSidecarVersion,
		Thread: threadID,
		Offset: offset,
		Items:  len(entries),
	})

	flag := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if rebuild {
		flag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}
	fh, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint: open metadata sidecar: %w", err)
	}
	defer fh.Close()
	for _, line := range lines {
		encoded, err := json.Marshal(line)
		if err != nil {
			return fmt.Errorf("checkpoint: marshal metadata sidecar line: %w", err)
		}
		if err := f.writeSidecarLineLocked(fh, encoded); err != nil {
			return err
		}
	}
	return f.metaOps.sync(fh)
}

func (f *File) writeSidecarLineLocked(fh *os.File, line []byte) error {
	n, err := f.metaOps.write(fh, append(line, '\n'))
	if err != nil {
		return fmt.Errorf("checkpoint: write metadata sidecar: %w", err)
	}
	if n != len(line)+1 {
		return fmt.Errorf("checkpoint: write metadata sidecar: short write %d of %d", n, len(line)+1)
	}
	return nil
}

// readJournalSummariesLocked reads complete journal lines from an offset and projects them.
// A final line without a newline is not consumed: it may be a write in progress, and reading
// half a record would invent a visible checkpoint.
func (f *File) readJournalSummariesLocked(threadID string, from int64, seedRevision uint64) ([]sidecarEntry, int64, error) {
	path, err := f.path(threadID)
	if err != nil {
		return nil, 0, err
	}
	fh, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, from, nil
		}
		return nil, 0, fmt.Errorf("checkpoint: open journal for summaries: %w", err)
	}
	defer fh.Close()

	if _, err := fh.Seek(from, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("checkpoint: seek journal: %w", err)
	}
	reader := bufio.NewReader(fh)
	var (
		entries  []sidecarEntry
		consumed = from
		revision = seedRevision
	)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) == 0 || readErr == io.EOF {
			// Nothing left, or a final line without a newline: a write still in progress is
			// not a visible checkpoint, so the covered offset stays before it.
			return entries, consumed, nil
		}
		if readErr != nil {
			return nil, 0, fmt.Errorf("checkpoint: read journal for summaries in %s: %w", threadID, readErr)
		}
		consumed += int64(len(line))
		f.noteTailLineLocked()
		metadata, nextRevision, keep, err := projectJournalLine(line[:len(line)-1], threadID, revision)
		if err != nil {
			return nil, 0, err
		}
		revision = nextRevision
		if keep {
			entries = append(entries, sidecarEntry{metadata: metadata, offset: consumed})
		}
	}
}

// projectJournalLine turns one journal line into a summary when the line is a visible
// checkpoint. keep is false for records that carry none (blobs, claims, inbox traffic,
// approvals, tool boundaries).
func projectJournalLine(body []byte, threadID string, revision uint64) (metadata CheckpointMetadata, nextRevision uint64, keep bool, err error) {
	nextRevision = revision
	var probe struct {
		metaFields
		Blob       string      `json:"blob"`
		Version    int         `json:"version"`
		Kind       string      `json:"kind"`
		Checkpoint *metaFields `json:"checkpoint"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return CheckpointMetadata{}, nextRevision, false, fmt.Errorf("checkpoint: corrupt line in %s: %w", threadID, err)
	}
	if probe.Blob != "" {
		return CheckpointMetadata{}, nextRevision, false, nil
	}

	fields := probe.Checkpoint
	legacy := false
	if fields == nil {
		if probe.Kind != "" {
			return CheckpointMetadata{}, nextRevision, false, nil // durable record without a checkpoint
		}
		if probe.ID == "" && probe.ThreadID == "" {
			return CheckpointMetadata{}, nextRevision, false, nil
		}
		fields = &probe.metaFields
		legacy = true
	}
	if fields.ThreadID != threadID {
		if legacy {
			// Old sanitized files could hold another thread's legacy line, and decodeLine
			// skips those; the projection has to agree with History.
			return CheckpointMetadata{}, nextRevision, false, nil
		}
		return CheckpointMetadata{}, nextRevision, false, fmt.Errorf("checkpoint: durable checkpoint thread mismatch in %s", threadID)
	}
	if fields.Revision == 0 {
		nextRevision = revision + 1
	} else if fields.Revision > revision {
		nextRevision = fields.Revision
	}
	metadata = checkpointMetadataOf(&Checkpoint{
		ID:         fields.ID,
		ThreadID:   fields.ThreadID,
		ParentID:   fields.ParentID,
		ParentRef:  fields.ParentRef,
		Revision:   nextRevision,
		Step:       fields.Step,
		RunID:      fields.RunID,
		TurnID:     fields.TurnID,
		CreatedAt:  fields.CreatedAt,
		Pause:      summaryPauseMarker(fields),
		InFlight:   summaryInFlightMarker(fields),
		Completion: summaryCompletionMarker(fields),
	})
	return metadata, nextRevision, true, nil
}

// summaryPauseMarker and summaryCompletionMarker hand checkpointMetadataOf the presence of a
// pause and the kind of a terminal record without materialising a pending tool batch or a
// frozen result.
func summaryPauseMarker(fields *metaFields) *Pause {
	if !fields.paused() {
		return nil
	}
	return &Pause{Phase: "summary"}
}

// summaryInFlightMarker hands checkpointMetadataOf the presence of an in-flight
// batch without materialising its calls. Only the flag is projected, so a
// rebuild stays as cheap as the pause marker already was.
func summaryInFlightMarker(fields *metaFields) *InFlightBatch {
	if !fields.inFlight() {
		return nil
	}
	return &InFlightBatch{Step: fields.Step}
}

func summaryCompletionMarker(fields *metaFields) *Completion {
	if fields.Completion == nil {
		return nil
	}
	return &Completion{Kind: fields.Completion.Kind}
}
