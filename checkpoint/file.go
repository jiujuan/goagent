package checkpoint

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/jiujuan/goagent/core"
)

// File is a JSONL-file Checkpointer: every thread uses one append-only file.
// Durable records intentionally share that file with ordinary checkpoints. A
// commit record is the visibility boundary for a checkpoint and its inbox acks;
// blobs may precede it, but are harmless until a committed checkpoint references
// them. All entry points use the same in-process mutex and per-thread lock file
// so separate File instances coordinate too.
type File struct {
	dir string
	mu  sync.Mutex

	// known is only a post-Sync cache for blob hashes. The journal remains the
	// source of truth and a failed write never changes this map.
	known map[string]map[string]bool
	ops   fileOps
}

type fileOps struct {
	write func(*os.File, []byte) (int, error)
	sync  func(*os.File) error
}

func defaultFileOps() fileOps {
	return fileOps{write: func(f *os.File, b []byte) (int, error) { return f.Write(b) }, sync: func(f *os.File) error { return f.Sync() }}
}

// NewFile opens (creating if needed) a file-backed checkpointer rooted at dir.
func NewFile(dir string) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: create dir: %w", err)
	}
	return &File{dir: dir, known: map[string]map[string]bool{}, ops: defaultFileOps()}, nil
}

// path resolves a thread id to its journal inside the store directory.
func (f *File) path(threadID string) (string, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return "", err
	}
	return filepath.Join(f.dir, threadID+".jsonl"), nil
}

func (f *File) lockPath(threadID string) (string, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return "", err
	}
	return filepath.Join(f.dir, threadID+".lock"), nil
}

// checkpointRecord is the legacy on-disk shape of a checkpoint line. It stays
// readable so pre-durable histories retain their resume and time-travel data.
type checkpointRecord struct {
	*Checkpoint
	Files map[string]string `json:"files,omitempty"`
}

type blobRecord struct {
	Blob string `json:"blob"`
	Data []byte `json:"data"`
}

const durableFormatVersion = 1

const (
	recordInboxDeliver = "inbox_deliver"
	recordClaim        = "claim"
	recordReleaseClaim = "release_claim"
	recordDecision     = "decision"
	recordTool         = "tool_execution"
	recordCommit       = "commit"
)

// journalRecord is a versioned durable record. Checkpoint and ack fields occur
// together only in recordCommit, which gives them one append-and-Sync boundary.
type journalRecord struct {
	Version int    `json:"version"`
	Kind    string `json:"kind"`

	Checkpoint     *checkpointRecord      `json:"checkpoint,omitempty"`
	AckMessageIDs  []string               `json:"ack_message_ids,omitempty"`
	Inbox          *InboxMessage          `json:"inbox,omitempty"`
	Claim          *Claim                 `json:"claim,omitempty"`
	ReleaseClaim   bool                   `json:"release_claim,omitempty"`
	Decision       *core.ApprovalDecision `json:"decision,omitempty"`
	ToolExecution  *ToolExecution         `json:"tool_execution,omitempty"`
	ToolExecutions []ToolExecution        `json:"tool_executions,omitempty"`
}

type fileState struct {
	memoryDurableState
	checkpoints   []*checkpointRecord
	checkpointIDs map[string]*checkpointRecord
	blobs         map[string][]byte
}

func newFileState() *fileState {
	return &fileState{
		memoryDurableState: memoryDurableState{
			inbox:      map[string]InboxMessage{},
			decisions:  map[string]core.ApprovalDecision{},
			executions: map[string]ToolExecution{},
		},
		checkpointIDs: map[string]*checkpointRecord{},
		blobs:         map[string][]byte{},
	}
}

// Save uses the same durable commit path as inbox acknowledgement. It does not
// bypass a live resume claim: callers resuming a claimed thread must carry the
// fencing token through Commit.
func (f *File) Save(ctx context.Context, cp *Checkpoint) error {
	if cp == nil || cp.ThreadID == "" {
		return fmt.Errorf("checkpoint: Save requires a non-nil checkpoint with ThreadID")
	}
	_, err := f.Commit(ctx, CommitRequest{Checkpoint: cp})
	return err
}

func (f *File) Deliver(ctx context.Context, threadID string, msg InboxMessage) (DeliveryReceipt, error) {
	if threadID == "" || msg.ID == "" {
		return DeliveryReceipt{}, fmt.Errorf("checkpoint: durable delivery requires thread ID and message ID")
	}
	var receipt DeliveryReceipt
	err := f.withThread(ctx, threadID, func(st *fileState) error {
		if old, ok := st.inbox[msg.ID]; ok {
			same, err := sameInboxMessage(old, msg)
			if err != nil {
				return err
			}
			if !same {
				return fmt.Errorf("checkpoint: inbox message ID %q conflicts with existing payload", msg.ID)
			}
			receipt = DeliveryReceipt{ID: msg.ID, Duplicate: true}
			return nil
		}
		rec := journalRecord{Version: durableFormatVersion, Kind: recordInboxDeliver, Inbox: &msg}
		if err := f.appendRecordLocked(threadID, st, nil, rec); err != nil {
			return err
		}
		applyJournalRecord(st, rec)
		receipt = DeliveryReceipt{ID: msg.ID}
		return nil
	})
	return receipt, err
}

func (f *File) Inbox(ctx context.Context, threadID string) ([]InboxMessage, error) {
	var out []InboxMessage
	err := f.withThread(ctx, threadID, func(st *fileState) error {
		out = make([]InboxMessage, 0, len(st.inboxOrder))
		for _, id := range st.inboxOrder {
			if msg, ok := st.inbox[id]; ok {
				out = append(out, msg)
			}
		}
		return nil
	})
	return out, err
}

func (f *File) Commit(ctx context.Context, req CommitRequest) (CommitReceipt, error) {
	if req.Checkpoint == nil || req.Checkpoint.ThreadID == "" {
		return CommitReceipt{}, fmt.Errorf("checkpoint: durable commit requires a checkpoint with ThreadID")
	}
	var receipt CommitReceipt
	err := f.withThread(ctx, req.Checkpoint.ThreadID, func(st *fileState) error {
		got, err := f.commitLocked(req.Checkpoint.ThreadID, st, req)
		if err != nil {
			return err
		}
		receipt = got
		return nil
	})
	return receipt, err
}

func (f *File) commitLocked(threadID string, st *fileState, req CommitRequest) (CommitReceipt, error) {
	if req.Checkpoint.ID != "" {
		if old, ok := st.checkpointIDs[req.Checkpoint.ID]; ok {
			// A failed Sync leaves the caller without an acknowledgement even
			// though the append may have reached the journal. Confirm the existing
			// record with a fresh Sync before treating this idempotent retry as a
			// success.
			if err := f.syncJournalLocked(threadID); err != nil {
				return CommitReceipt{}, err
			}
			f.setKnownLocked(threadID, st.blobs)
			req.Checkpoint.Revision = old.Revision
			return CommitReceipt{Revision: old.Revision, Claim: cloneClaim(st.claim)}, nil
		}
	}
	for _, ex := range req.ToolExecutions {
		if ex.InvocationID == "" {
			return CommitReceipt{}, fmt.Errorf("checkpoint: tool execution requires invocation ID")
		}
	}
	if err := validateCommitClaim(&st.memoryDurableState, req.Claim, time.Now()); err != nil {
		return CommitReceipt{}, err
	}

	stored := *req.Checkpoint
	stored.Revision = st.revision + 1
	checkpoint, blobRecords, newHashes, err := buildCheckpointRecord(&stored, st.blobs)
	if err != nil {
		return CommitReceipt{}, err
	}
	claim := renewedClaim(&st.memoryDurableState, req, time.Now())
	rec := journalRecord{
		Version:        durableFormatVersion,
		Kind:           recordCommit,
		Checkpoint:     checkpoint,
		AckMessageIDs:  append([]string(nil), req.AckMessageIDs...),
		Claim:          claim,
		ReleaseClaim:   req.ReleaseClaim && req.Claim != nil,
		ToolExecutions: cloneExecutions(req.ToolExecutions),
	}
	if err := f.appendRecordLocked(threadID, st, blobRecords, rec); err != nil {
		return CommitReceipt{}, err
	}
	applyJournalRecord(st, rec)
	for h, data := range newHashes {
		st.blobs[h] = data
	}
	f.setKnownLocked(threadID, st.blobs)
	req.Checkpoint.Revision = stored.Revision
	if rec.ReleaseClaim {
		claim = nil
	}
	return CommitReceipt{Revision: stored.Revision, Claim: claim}, nil
}

func (f *File) Claim(ctx context.Context, req ClaimRequest) (Claim, error) {
	if req.ThreadID == "" || req.PauseID == "" || req.Owner == "" {
		return Claim{}, fmt.Errorf("checkpoint: claim requires thread ID, pause ID and owner")
	}
	var claim Claim
	err := f.withThread(ctx, req.ThreadID, func(st *fileState) error {
		cp := latestFileCheckpoint(st)
		if cp == nil || cp.Revision != req.Revision || EffectivePauseID(cp.Checkpoint) != req.PauseID {
			return ErrClaimConflict
		}
		now := time.Now()
		if st.claim != nil && !claimExpired(st.claim, now) {
			if st.claim.Owner != req.Owner || st.claim.Revision != req.Revision || st.claim.PauseID != req.PauseID {
				return ErrClaimConflict
			}
			claim = *st.claim
			claim.ExpiresAt = now.Add(normalizeLease(req.Lease))
		} else {
			st.fencing++
			claim = Claim{
				ThreadID:     req.ThreadID,
				Revision:     req.Revision,
				PauseID:      req.PauseID,
				Owner:        req.Owner,
				FencingToken: st.fencing,
				ExpiresAt:    now.Add(normalizeLease(req.Lease)),
			}
		}
		rec := journalRecord{Version: durableFormatVersion, Kind: recordClaim, Claim: &claim}
		if err := f.appendRecordLocked(req.ThreadID, st, nil, rec); err != nil {
			return err
		}
		applyJournalRecord(st, rec)
		return nil
	})
	return claim, err
}

func (f *File) ReleaseClaim(ctx context.Context, claim Claim) error {
	if claim.ThreadID == "" {
		return fmt.Errorf("checkpoint: release claim requires thread ID")
	}
	return f.withThread(ctx, claim.ThreadID, func(st *fileState) error {
		if st.claim == nil {
			return nil
		}
		if !sameClaim(*st.claim, claim) {
			return ErrStaleFencingToken
		}
		rec := journalRecord{Version: durableFormatVersion, Kind: recordReleaseClaim, Claim: &claim, ReleaseClaim: true}
		if err := f.appendRecordLocked(claim.ThreadID, st, nil, rec); err != nil {
			return err
		}
		applyJournalRecord(st, rec)
		return nil
	})
}

func (f *File) RecordDecision(ctx context.Context, threadID string, decision core.ApprovalDecision) (DecisionReceipt, error) {
	if threadID == "" || decision.DecisionID == "" || decision.PauseID == "" || decision.Digest == "" {
		return DecisionReceipt{}, fmt.Errorf("checkpoint: structured decision requires thread ID, decision ID, pause ID and digest")
	}
	var receipt DecisionReceipt
	err := f.withThread(ctx, threadID, func(st *fileState) error {
		if old, ok := st.decisions[decision.DecisionID]; ok {
			if old.Digest != decision.Digest {
				return ErrDecisionConflict
			}
			receipt = DecisionReceipt{Decision: old, Duplicate: true}
			return nil
		}
		rec := journalRecord{Version: durableFormatVersion, Kind: recordDecision, Decision: &decision}
		if err := f.appendRecordLocked(threadID, st, nil, rec); err != nil {
			return err
		}
		applyJournalRecord(st, rec)
		receipt = DecisionReceipt{Decision: decision}
		return nil
	})
	return receipt, err
}

func (f *File) Decisions(ctx context.Context, threadID, pause string) ([]core.ApprovalDecision, error) {
	var out []core.ApprovalDecision
	err := f.withThread(ctx, threadID, func(st *fileState) error {
		out = make([]core.ApprovalDecision, 0, len(st.decisions))
		for _, d := range st.decisions {
			if d.PauseID == pause {
				out = append(out, d)
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].DecisionID < out[j].DecisionID })
		return nil
	})
	return out, err
}

func (f *File) ClaimTool(ctx context.Context, req ToolClaimRequest) (ToolExecution, error) {
	if req.ThreadID == "" || req.InvocationID == "" || req.CallID == "" || req.Tool == "" {
		return ToolExecution{}, fmt.Errorf("checkpoint: tool claim requires thread, invocation, call and tool IDs")
	}
	var result ToolExecution
	var outcomeErr error
	err := f.withThread(ctx, req.ThreadID, func(st *fileState) error {
		if err := validateCommitClaim(&st.memoryDurableState, req.Claim, time.Now()); err != nil {
			return err
		}
		if old, ok := st.executions[req.InvocationID]; ok {
			switch old.State {
			case ToolCompleted:
				result = cloneExecution(old)
				return nil
			case ToolUnknown:
				result, outcomeErr = cloneExecution(old), ErrToolOutcomeUnknown
				return nil
			case ToolClaimed:
				old.State, old.Result = ToolUnknown, nil
				result, outcomeErr = cloneExecution(old), ErrToolOutcomeUnknown
				rec := journalRecord{Version: durableFormatVersion, Kind: recordTool, ToolExecution: &old}
				if err := f.appendRecordLocked(req.ThreadID, st, nil, rec); err != nil {
					return err
				}
				applyJournalRecord(st, rec)
				return nil
			case ToolPending:
				old.Attempt++
				old.State = ToolClaimed
				if old.IdempotencyKey == "" {
					old.IdempotencyKey = req.IdempotencyKey
				}
				result = cloneExecution(old)
				rec := journalRecord{Version: durableFormatVersion, Kind: recordTool, ToolExecution: &old}
				if err := f.appendRecordLocked(req.ThreadID, st, nil, rec); err != nil {
					return err
				}
				applyJournalRecord(st, rec)
				return nil
			}
		}
		ex := ToolExecution{InvocationID: req.InvocationID, CallID: req.CallID, Tool: req.Tool, IdempotencyKey: req.IdempotencyKey, Attempt: 1, State: ToolClaimed}
		result = cloneExecution(ex)
		rec := journalRecord{Version: durableFormatVersion, Kind: recordTool, ToolExecution: &ex}
		if err := f.appendRecordLocked(req.ThreadID, st, nil, rec); err != nil {
			return err
		}
		applyJournalRecord(st, rec)
		return nil
	})
	if err != nil {
		return ToolExecution{}, err
	}
	return result, outcomeErr
}

func (f *File) ToolExecution(ctx context.Context, threadID, invocationID string) (ToolExecution, bool, error) {
	var result ToolExecution
	var ok bool
	err := f.withThread(ctx, threadID, func(st *fileState) error {
		result, ok = st.executions[invocationID]
		result = cloneExecution(result)
		return nil
	})
	return result, ok, err
}

// readAll returns a thread's snapshots oldest first. A truncated final record
// is removed while holding the same lock used by writers; malformed middle
// records fail loudly instead of silently skipping history.
func (f *File) readAll(ctx context.Context, threadID string) ([]*Checkpoint, error) {
	var out []*Checkpoint
	err := f.withThread(ctx, threadID, func(st *fileState) error {
		if err := rebuild(st.checkpoints, st.blobs); err != nil {
			return err
		}
		out = make([]*Checkpoint, 0, len(st.checkpoints))
		for _, rec := range st.checkpoints {
			out = append(out, rec.Checkpoint)
		}
		f.setKnownLocked(threadID, st.blobs)
		return nil
	})
	return out, err
}

func (f *File) Load(ctx context.Context, threadID, checkpointID string) (*Checkpoint, error) {
	all, err := f.readAll(ctx, threadID)
	if err != nil {
		return nil, err
	}
	for _, cp := range all {
		if cp.ID == checkpointID {
			return cp, nil
		}
	}
	return nil, fmt.Errorf("checkpoint: %s/%s not found", threadID, checkpointID)
}

func (f *File) Latest(ctx context.Context, threadID string) (*Checkpoint, error) {
	all, err := f.readAll(ctx, threadID)
	if err != nil || len(all) == 0 {
		return nil, err
	}
	return all[len(all)-1], nil
}

func (f *File) History(ctx context.Context, threadID string) ([]*Checkpoint, error) {
	return f.readAll(ctx, threadID)
}

// withThread establishes the only lock order in File: process-local mutex,
// then a cross-process lock file. The critical section is intentionally small
// and includes both scan/recovery and append, so two File instances cannot
// both observe an unclaimed pause and then claim it.
func (f *File) withThread(ctx context.Context, threadID string, fn func(*fileState) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := f.path(threadID); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	release, err := f.acquireProcessLock(ctx, threadID)
	if err != nil {
		return err
	}
	defer release()
	st, tailOffset, err := f.scanLocked(threadID)
	if err != nil {
		return err
	}
	if tailOffset >= 0 {
		fp, err := f.path(threadID)
		if err != nil {
			return err
		}
		if err := os.Truncate(fp, tailOffset); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("checkpoint: repair truncated tail: %w", err)
		}
	}
	return fn(st)
}

// acquireProcessLock uses O_EXCL rather than flock so the File backend works
// on Windows too. A lock can only be stolen after a generous operation TTL;
// ordinary claims have their own lease and fencing mechanism.
func (f *File) acquireProcessLock(ctx context.Context, threadID string) (func(), error) {
	path, err := f.lockPath(threadID)
	if err != nil {
		return nil, err
	}
	for {
		fh, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = fh.WriteString(fmt.Sprintf("%d", time.Now().UnixNano()))
			_ = fh.Close()
			return func() { _ = os.Remove(path) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if info, statErr := os.Stat(path); statErr == nil && time.Since(info.ModTime()) > 2*time.Minute {
			if removeErr := os.Remove(path); removeErr == nil || os.IsNotExist(removeErr) {
				continue
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// scanLocked reads the complete journal. It accepts only a missing final
// newline as a recoverable tail; a malformed record that ended with a newline
// is a middle-record corruption and is reported.
func (f *File) scanLocked(threadID string) (*fileState, int64, error) {
	fp, err := f.path(threadID)
	if err != nil {
		return nil, -1, err
	}
	fh, err := os.Open(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return newFileState(), -1, nil
		}
		return nil, -1, err
	}
	defer fh.Close()

	st := newFileState()
	r := bufio.NewReader(fh)
	var offset int64
	for {
		lineStart := offset
		line, readErr := r.ReadBytes('\n')
		if len(line) > 0 {
			if readErr == io.EOF {
				return st, lineStart, nil
			}
			offset += int64(len(line))
			line = line[:len(line)-1]
			if len(line) == 0 {
				if readErr != nil {
					return nil, -1, readErr
				}
				continue
			}
			if err := decodeLine(st, threadID, line); err != nil {
				return nil, -1, err
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return st, -1, nil
			}
			return nil, -1, readErr
		}
	}
}

func decodeLine(st *fileState, threadID string, line []byte) error {
	var probe struct {
		Blob    string `json:"blob"`
		ID      string `json:"id"`
		Kind    string `json:"kind"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		return fmt.Errorf("checkpoint: corrupt line in %s: %w", threadID, err)
	}
	if probe.Blob != "" {
		var blob blobRecord
		if err := json.Unmarshal(line, &blob); err != nil {
			return fmt.Errorf("checkpoint: corrupt blob in %s: %w", threadID, err)
		}
		st.blobs[blob.Blob] = blob.Data
		return nil
	}
	if probe.Kind != "" {
		if probe.Version != durableFormatVersion {
			return fmt.Errorf("checkpoint: unsupported durable record version %d in %s", probe.Version, threadID)
		}
		var rec journalRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("checkpoint: corrupt durable record in %s: %w", threadID, err)
		}
		if err := validateJournalRecord(rec); err != nil {
			return fmt.Errorf("checkpoint: corrupt durable record in %s: %w", threadID, err)
		}
		if rec.Claim != nil && rec.Claim.ThreadID != threadID {
			return fmt.Errorf("checkpoint: durable claim thread mismatch in %s", threadID)
		}
		if rec.Checkpoint != nil && rec.Checkpoint.Checkpoint != nil && rec.Checkpoint.ThreadID != threadID {
			return fmt.Errorf("checkpoint: durable checkpoint thread mismatch in %s", threadID)
		}
		applyJournalRecord(st, rec)
		return nil
	}
	var legacy checkpointRecord
	legacy.Checkpoint = &Checkpoint{}
	if err := json.Unmarshal(line, &legacy); err != nil {
		return fmt.Errorf("checkpoint: corrupt line in %s: %w", threadID, err)
	}
	if legacy.ID == "" || legacy.ThreadID == "" {
		return fmt.Errorf("checkpoint: unrecognized record in %s", threadID)
	}
	if legacy.ThreadID != threadID {
		return nil // old sanitized files could contain another thread's legacy line
	}
	applyCheckpoint(st, &legacy)
	return nil
}

func validateJournalRecord(rec journalRecord) error {
	switch rec.Kind {
	case recordInboxDeliver:
		if rec.Inbox == nil || rec.Inbox.ID == "" {
			return errors.New("invalid inbox delivery")
		}
	case recordClaim:
		if rec.Claim == nil || rec.Claim.ThreadID == "" {
			return errors.New("invalid claim")
		}
	case recordReleaseClaim:
		if rec.Claim == nil {
			return errors.New("invalid claim release")
		}
	case recordDecision:
		if rec.Decision == nil || rec.Decision.DecisionID == "" || rec.Decision.Digest == "" {
			return errors.New("invalid decision")
		}
	case recordTool:
		if rec.ToolExecution == nil || rec.ToolExecution.InvocationID == "" {
			return errors.New("invalid tool execution")
		}
	case recordCommit:
		if rec.Checkpoint == nil || rec.Checkpoint.Checkpoint == nil || rec.Checkpoint.ID == "" {
			return errors.New("invalid commit checkpoint")
		}
		for _, ex := range rec.ToolExecutions {
			if ex.InvocationID == "" {
				return errors.New("invalid committed tool execution")
			}
		}
	default:
		return fmt.Errorf("unknown durable record kind %q", rec.Kind)
	}
	return nil
}

func applyJournalRecord(st *fileState, rec journalRecord) {
	switch rec.Kind {
	case recordInboxDeliver:
		if _, exists := st.inbox[rec.Inbox.ID]; !exists {
			st.inbox[rec.Inbox.ID] = *rec.Inbox
			st.inboxOrder = append(st.inboxOrder, rec.Inbox.ID)
		}
	case recordClaim:
		c := *rec.Claim
		st.claim = &c
		if c.FencingToken > st.fencing {
			st.fencing = c.FencingToken
		}
	case recordReleaseClaim:
		if st.claim != nil && sameClaim(*st.claim, *rec.Claim) {
			st.claim = nil
		}
	case recordDecision:
		if _, exists := st.decisions[rec.Decision.DecisionID]; !exists {
			st.decisions[rec.Decision.DecisionID] = *rec.Decision
		}
	case recordTool:
		st.executions[rec.ToolExecution.InvocationID] = cloneExecution(*rec.ToolExecution)
	case recordCommit:
		applyCheckpoint(st, rec.Checkpoint)
		for _, id := range rec.AckMessageIDs {
			delete(st.inbox, id)
		}
		for _, ex := range rec.ToolExecutions {
			st.executions[ex.InvocationID] = cloneExecution(ex)
		}
		if rec.Claim != nil {
			c := *rec.Claim
			st.claim = &c
			if c.FencingToken > st.fencing {
				st.fencing = c.FencingToken
			}
		}
		if rec.ReleaseClaim {
			st.claim = nil
		}
	}
}

func applyCheckpoint(st *fileState, rec *checkpointRecord) {
	if rec == nil || rec.Checkpoint == nil {
		return
	}
	if rec.ID != "" {
		if _, exists := st.checkpointIDs[rec.ID]; exists {
			return
		}
	}
	if rec.Revision == 0 {
		rec.Revision = st.revision + 1
	}
	if rec.Revision > st.revision {
		st.revision = rec.Revision
	}
	st.checkpoints = append(st.checkpoints, rec)
	if rec.ID != "" {
		st.checkpointIDs[rec.ID] = rec
	}
}

func latestFileCheckpoint(st *fileState) *checkpointRecord {
	if len(st.checkpoints) == 0 {
		return nil
	}
	return st.checkpoints[len(st.checkpoints)-1]
}

func buildCheckpointRecord(cp *Checkpoint, known map[string][]byte) (*checkpointRecord, []blobRecord, map[string][]byte, error) {
	var blobs []blobRecord
	added := map[string][]byte{}
	var index map[string]string
	if ss, ok := cp.State.Files.(core.Snapshottable); ok {
		if snap := ss.Snapshot(); len(snap) > 0 {
			index = make(map[string]string, len(snap))
			for path, data := range snap {
				h := hashOf(data)
				index[path] = h
				if _, exists := known[h]; !exists {
					if _, staged := added[h]; !staged {
						copyData := append([]byte(nil), data...)
						added[h] = copyData
						blobs = append(blobs, blobRecord{Blob: h, Data: copyData})
					}
				}
			}
		}
	}
	return &checkpointRecord{Checkpoint: cp, Files: index}, blobs, added, nil
}

func (f *File) appendRecordLocked(threadID string, st *fileState, blobs []blobRecord, rec journalRecord) error {
	fp, err := f.path(threadID)
	if err != nil {
		return err
	}
	fh, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	for _, blob := range blobs {
		line, err := json.Marshal(blob)
		if err != nil {
			return fmt.Errorf("checkpoint: marshal blob: %w", err)
		}
		if err := f.writeLine(fh, line); err != nil {
			return err
		}
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal durable record: %w", err)
	}
	if err := f.writeLine(fh, line); err != nil {
		return err
	}
	if err := f.ops.sync(fh); err != nil {
		return fmt.Errorf("checkpoint: sync: %w", err)
	}
	return nil
}

// syncJournalLocked confirms data already present in a journal. It is used by
// idempotent Commit retries after a prior Sync error: the first append may have
// completed, but a caller must not receive success until a later Sync succeeds.
func (f *File) syncJournalLocked(threadID string) error {
	fp, err := f.path(threadID)
	if err != nil {
		return err
	}
	fh, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	if err := f.ops.sync(fh); err != nil {
		return fmt.Errorf("checkpoint: sync: %w", err)
	}
	return nil
}

func (f *File) writeLine(fh *os.File, line []byte) error {
	b := append(append([]byte(nil), line...), '\n')
	for len(b) > 0 {
		n, err := f.ops.write(fh, b)
		if n > 0 {
			b = b[n:]
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

func rebuild(cps []*checkpointRecord, blobs map[string][]byte) error {
	for _, rec := range cps {
		if len(rec.Files) == 0 {
			continue
		}
		snap := make(map[string][]byte, len(rec.Files))
		for path, h := range rec.Files {
			data, ok := blobs[h]
			if !ok {
				return fmt.Errorf("checkpoint: %s references missing blob %s", rec.ThreadID, h)
			}
			snap[path] = append([]byte(nil), data...)
		}
		rec.FileSnapshot = snap
	}
	return nil
}

func (f *File) setKnownLocked(threadID string, blobs map[string][]byte) {
	known := make(map[string]bool, len(blobs))
	for h := range blobs {
		known[h] = true
	}
	f.known[threadID] = known
}

func cloneExecutions(in []ToolExecution) []ToolExecution {
	if len(in) == 0 {
		return nil
	}
	out := make([]ToolExecution, len(in))
	for i := range in {
		out[i] = cloneExecution(in[i])
	}
	return out
}

func hashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var _ Checkpointer = (*File)(nil)
var _ Durable = (*File)(nil)
