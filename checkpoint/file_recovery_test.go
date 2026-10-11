package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

func fileRecoveryCheckpoint(thread, id string) *Checkpoint {
	files := vfs.NewInState()
	if err := files.Write("note.txt", []byte("durable")); err != nil {
		panic(err)
	}
	return &Checkpoint{ID: id, ThreadID: thread, State: core.State{Files: files}}
}

func TestFileTailRecoveryAndBlobRetry(t *testing.T) {
	ctx := context.Background()
	normal := defaultFileOps()

	// A short write leaves only a partial blob line. The retry must repair that
	// tail and write both blob and checkpoint, rather than trusting a stale cache.
	f, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	short := false
	f.ops = fileOps{
		write: func(fh *os.File, b []byte) (int, error) {
			if !short {
				short = true
				return 1, io.ErrShortWrite
			}
			return normal.write(fh, b)
		},
		sync: normal.sync,
	}
	cp := fileRecoveryCheckpoint("short", "cp-short")
	if err := f.Save(ctx, cp); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error = %v, want io.ErrShortWrite", err)
	}
	f.ops = normal
	if err := f.Save(ctx, cp); err != nil {
		t.Fatalf("retry after short write: %v", err)
	}
	got, err := f.Latest(ctx, "short")
	if err != nil || got == nil || string(got.FileSnapshot["note.txt"]) != "durable" {
		t.Fatalf("retry did not restore blob snapshot: checkpoint=%+v err=%v", got, err)
	}

	// Sync can fail after all bytes reach the journal. A retry of the same
	// checkpoint must perform another Sync before reporting success.
	syncStore, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	syncs := 0
	syncStore.ops = fileOps{
		write: normal.write,
		sync: func(fh *os.File) error {
			syncs++
			if syncs == 1 {
				return errors.New("injected sync failure")
			}
			return normal.sync(fh)
		},
	}
	syncCP := fileRecoveryCheckpoint("sync", "cp-sync")
	if err := syncStore.Save(ctx, syncCP); err == nil || !strings.Contains(err.Error(), "injected sync failure") {
		t.Fatalf("first sync failure = %v", err)
	}
	if err := syncStore.Save(ctx, syncCP); err != nil {
		t.Fatalf("sync retry failed: %v", err)
	}
	if syncs != 2 {
		t.Fatalf("sync retry calls = %d, want 2", syncs)
	}

	// An incomplete final record is recovered under the writer lock, leaving the
	// last complete checkpoint visible. A complete corrupt line is not recoverable.
	tailStore, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := tailStore.Save(ctx, fileRecoveryCheckpoint("tail", "cp-tail")); err != nil {
		t.Fatal(err)
	}
	tailPath := filepath.Join(tailStore.dir, "tail.jsonl")
	completeJournal := mustReadFile(t, tailPath)
	if fh, err := os.OpenFile(tailPath, os.O_APPEND|os.O_WRONLY, 0o644); err != nil {
		t.Fatal(err)
	} else {
		if _, err := fh.WriteString(`{"version":1`); err != nil {
			_ = fh.Close()
			t.Fatal(err)
		}
		_ = fh.Close()
	}
	if got, err := tailStore.Latest(ctx, "tail"); err != nil || got == nil || got.ID != "cp-tail" {
		t.Fatalf("tail recovery checkpoint=%+v err=%v", got, err)
	}
	data, err := os.ReadFile(tailPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(completeJournal) {
		t.Fatalf("recovered journal = %q, want original complete journal %q", data, completeJournal)
	}

	middleStore, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := middleStore.Save(ctx, fileRecoveryCheckpoint("middle", "cp-middle")); err != nil {
		t.Fatal(err)
	}
	middlePath := filepath.Join(middleStore.dir, "middle.jsonl")
	if err := os.WriteFile(middlePath, append(mustReadFile(t, middlePath), []byte("not-json\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := middleStore.Latest(ctx, "middle"); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("middle corruption error = %v", err)
	}
}

func TestSnapshotConsistencyFileTwoInstances(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, time.October, 12, 16, 0, 0, 0, time.UTC)
	stores := make([]*File, 2)
	for i := range stores {
		store, err := NewFile(dir)
		if err != nil {
			t.Fatal(err)
		}
		store.now = func() time.Time { return now }
		stores[i] = store
	}

	type result struct {
		claim Claim
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, len(stores))
	var wg sync.WaitGroup
	for i, store := range stores {
		wg.Add(1)
		go func(owner string, store *File) {
			defer wg.Done()
			<-start
			got, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: owner, Lease: time.Minute})
			results <- result{claim: got.Claim, err: err}
		}(string(rune('a'+i)), store)
	}
	close(start)
	wg.Wait()
	close(results)

	var granted, conflicts int
	for got := range results {
		switch {
		case got.err == nil:
			granted++
			if got.claim.FencingToken != 1 {
				t.Fatalf("winning fencing token = %d, want 1", got.claim.FencingToken)
			}
		case errors.Is(got.err, ErrClaimConflict):
			conflicts++
		default:
			t.Fatalf("BeginRun error = %v", got.err)
		}
	}
	if granted != 1 || conflicts != 1 {
		t.Fatalf("grants=%d conflicts=%d, want 1/1", granted, conflicts)
	}
}

func TestSnapshotConsistencyFileRestartClaim(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, time.October, 12, 17, 0, 0, 0, time.UTC)
	open := func() *File {
		store, err := NewFile(dir)
		if err != nil {
			t.Fatal(err)
		}
		store.now = func() time.Time { return now }
		return store
	}

	firstStore := open()
	head := &Checkpoint{ID: "head", ThreadID: "thread", State: core.State{KV: map[string]any{"value": "stored"}}}
	if err := firstStore.Save(ctx, head); err != nil {
		t.Fatal(err)
	}
	ownerA, err := firstStore.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	reopened := open()
	gotHead, err := reopened.Latest(ctx, "thread")
	if err != nil || gotHead == nil || gotHead.ID != head.ID || gotHead.Revision != head.Revision {
		t.Fatalf("reopened head=%+v err=%v", gotHead, err)
	}
	if _, err := reopened.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute}); !errors.Is(err, ErrClaimConflict) {
		t.Fatalf("live claim after restart error = %v, want ErrClaimConflict", err)
	}

	now = ownerA.Claim.ExpiresAt
	ownerB, err := open().BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if ownerB.Claim.FencingToken <= ownerA.Claim.FencingToken {
		t.Fatalf("restart fencing = %d, want greater than %d", ownerB.Claim.FencingToken, ownerA.Claim.FencingToken)
	}

	now = now.Add(10 * time.Second)
	renewed, err := open().RenewClaim(ctx, ownerB.Claim, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.FencingToken != ownerB.Claim.FencingToken || !renewed.ExpiresAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("renewed claim = %+v", renewed)
	}
}

func TestSnapshotConsistencyFileClaimWriteFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, time.October, 12, 17, 30, 0, 0, time.UTC)
	store := newFileRecoveryStore(t, dir, now)
	base := seedFileRecoveryThread(t, store, "thread", "message")
	claimed := fileRecoveryExecution(ToolClaimed)
	seed := &Checkpoint{ID: "seed", ThreadID: "thread", State: core.State{KV: map[string]any{"value": "seed"}}}
	if _, err := store.Commit(ctx, CommitRequest{
		Checkpoint: seed, ExpectedRevision: revisionPtr(base.Revision), ToolExecutions: []ToolExecution{claimed},
	}); err != nil {
		t.Fatal(err)
	}

	injectedErr := errors.New("injected claim write failure")
	store.ops.write = func(*os.File, []byte) (int, error) { return 0, injectedErr }
	result, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if !errors.Is(err, injectedErr) {
		t.Fatalf("BeginRun error = %v, want injected write failure", err)
	}
	if result.Claim.FencingToken != 0 || result.Checkpoint != nil {
		t.Fatalf("failed BeginRun returned state: %+v", result)
	}

	reopened := newFileRecoveryStore(t, dir, now)
	assertFileRecoveryContents(t, reopened, "thread", "seed", 2, []string{"message"}, claimed)
	if err := reopened.withThread(ctx, "thread", func(st *fileState) error {
		if st.claim != nil || st.fencing != 0 {
			t.Fatalf("failed claim changed owner state: claim=%+v fencing=%d", st.claim, st.fencing)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	winner, err := reopened.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if winner.Claim.FencingToken != 1 || winner.Checkpoint == nil || winner.Checkpoint.ID != "seed" {
		t.Fatalf("claim after write failure = %+v", winner)
	}
}

func TestSnapshotConsistencyFileRenewWriteFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, time.October, 12, 17, 40, 0, 0, time.UTC)
	store := newFileRecoveryStore(t, dir, now)
	base := seedFileRecoveryThread(t, store, "thread", "message")
	started, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimTool(ctx, ToolClaimRequest{
		ThreadID: "thread", InvocationID: "invocation", CallID: "call", Tool: "tool", Claim: &started.Claim,
	})
	if err != nil {
		t.Fatal(err)
	}

	injectedErr := errors.New("injected renew write failure")
	store.ops.write = func(*os.File, []byte) (int, error) { return 0, injectedErr }
	if _, err := store.RenewClaim(ctx, started.Claim, 2*time.Minute); !errors.Is(err, injectedErr) {
		t.Fatalf("RenewClaim error = %v, want injected write failure", err)
	}

	reopened := newFileRecoveryStore(t, dir, now)
	assertFileRecoveryContents(t, reopened, "thread", base.ID, base.Revision, []string{"message"}, claimed)
	if err := reopened.withThread(ctx, "thread", func(st *fileState) error {
		if st.claim == nil || !sameClaim(*st.claim, started.Claim) || !st.claim.ExpiresAt.Equal(started.Claim.ExpiresAt) {
			t.Fatalf("claim after failed renewal = %+v, want %+v", st.claim, started.Claim)
		}
		if st.fencing != started.Claim.FencingToken {
			t.Fatalf("fencing after failed renewal = %d, want %d", st.fencing, started.Claim.FencingToken)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute}); !errors.Is(err, ErrClaimConflict) {
		t.Fatalf("competing owner after failed renewal error = %v, want ErrClaimConflict", err)
	}
	renewed, err := reopened.RenewClaim(ctx, started.Claim, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.FencingToken != started.Claim.FencingToken || !renewed.ExpiresAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("renewed claim = %+v", renewed)
	}
}

func TestSnapshotConsistencyFileCommitShortWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, time.October, 12, 17, 50, 0, 0, time.UTC)
	normal := defaultFileOps()
	store := newFileRecoveryStore(t, dir, now)
	base := seedFileRecoveryThread(t, store, "thread", "message")
	started, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimTool(ctx, ToolClaimRequest{
		ThreadID: "thread", InvocationID: "invocation", CallID: "call", Tool: "tool", Claim: &started.Claim,
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := fileRecoveryExecution(ToolCompleted)
	completed.Result = coordinatorMessagePtr(core.AssistantText("done"))
	target := &Checkpoint{
		ID: "short-commit", ThreadID: "thread", State: core.State{KV: map[string]any{"value": "committed"}},
		FileSnapshot: map[string][]byte{"result.txt": []byte("durable result")},
	}
	shorted := false
	store.ops.write = func(fh *os.File, data []byte) (int, error) {
		if !shorted && bytes.Contains(data, []byte(`"kind":"commit"`)) && bytes.Contains(data, []byte(target.ID)) {
			shorted = true
			n, writeErr := normal.write(fh, data[:len(data)/2])
			if writeErr != nil {
				return n, writeErr
			}
			return n, io.ErrShortWrite
		}
		return normal.write(fh, data)
	}
	_, err = store.Commit(ctx, CommitRequest{
		Checkpoint: target, ExpectedRevision: revisionPtr(base.Revision), Claim: &started.Claim,
		AckMessageIDs: []string{"message"}, ToolExecutions: []ToolExecution{completed},
	})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Commit error = %v, want io.ErrShortWrite", err)
	}
	if target.Revision != 0 || !shorted {
		t.Fatalf("failed Commit revision=%d shorted=%v", target.Revision, shorted)
	}

	reopened := newFileRecoveryStore(t, dir, now)
	assertFileRecoveryContents(t, reopened, "thread", base.ID, base.Revision, []string{"message"}, claimed)
	if err := reopened.withThread(ctx, "thread", func(st *fileState) error {
		if st.claim == nil || !sameClaim(*st.claim, started.Claim) || st.fencing != started.Claim.FencingToken {
			t.Fatalf("claim after short commit = %+v fencing=%d", st.claim, st.fencing)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	retry := &Checkpoint{
		ID: target.ID, ThreadID: target.ThreadID, State: core.State{KV: map[string]any{"value": "committed"}},
		FileSnapshot: map[string][]byte{"result.txt": []byte("durable result")},
	}
	receipt, err := reopened.Commit(ctx, CommitRequest{
		Checkpoint: retry, ExpectedRevision: revisionPtr(base.Revision), Claim: &started.Claim,
		AckMessageIDs: []string{"message"}, ToolExecutions: []ToolExecution{completed},
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Revision != 2 || retry.Revision != 2 {
		t.Fatalf("retry receipt=%+v checkpoint revision=%d", receipt, retry.Revision)
	}
	if got := journalCommitIDCount(t, filepath.Join(dir, "thread.jsonl"), target.ID); got != 1 {
		t.Fatalf("complete commits for %q = %d, want 1", target.ID, got)
	}
}

func TestSnapshotConsistencyFileClaimSyncFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, time.October, 12, 18, 0, 0, 0, time.UTC)
	normal := defaultFileOps()
	store := newFileRecoveryStore(t, dir, now)
	base := seedFileRecoveryThread(t, store, "thread", "message")
	claimed := fileRecoveryExecution(ToolClaimed)
	seed := &Checkpoint{ID: "claim-sync-seed", ThreadID: "thread", State: core.State{KV: map[string]any{"value": "seed"}}}
	if _, err := store.Commit(ctx, CommitRequest{
		Checkpoint: seed, ExpectedRevision: revisionPtr(base.Revision), ToolExecutions: []ToolExecution{claimed},
	}); err != nil {
		t.Fatal(err)
	}
	store.ops = fileOps{
		write: normal.write,
		sync:  func(*os.File) error { return errors.New("injected claim sync failure") },
	}

	result, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "injected claim sync failure") {
		t.Fatalf("BeginRun error = %v", err)
	}
	if result.Claim.FencingToken != 0 {
		t.Fatalf("failed BeginRun returned grant: %+v", result.Claim)
	}
	if got := journalKindCount(t, filepath.Join(dir, "thread.jsonl"), recordClaim); got != 1 {
		t.Fatalf("claim records after failed Sync = %d, want 1", got)
	}

	reopened := newFileRecoveryStore(t, dir, now)
	assertFileRecoveryContents(t, reopened, "thread", seed.ID, seed.Revision, []string{"message"}, claimed)
	if err := reopened.withThread(ctx, "thread", func(st *fileState) error {
		if st.claim == nil || st.claim.Owner != "owner-a" || st.claim.FencingToken != 1 || st.fencing != 1 {
			t.Fatalf("claim after failed Sync = %+v fencing=%d", st.claim, st.fencing)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute}); !errors.Is(err, ErrClaimConflict) {
		t.Fatalf("competing owner after failed Sync error = %v, want ErrClaimConflict", err)
	}
	confirmed, err := reopened.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.Claim.FencingToken != 1 || confirmed.Checkpoint == nil || confirmed.Checkpoint.ID != seed.ID {
		t.Fatalf("confirmed claim = %+v", confirmed.Claim)
	}
}

func TestSnapshotConsistencyFileCommitSyncRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, time.October, 12, 19, 0, 0, 0, time.UTC)
	normal := defaultFileOps()
	store := newFileRecoveryStore(t, dir, now)
	base := &Checkpoint{ID: "base", ThreadID: "thread", State: core.State{KV: map[string]any{"value": "base"}}}
	if err := store.Save(ctx, base); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"message-a", "message-b"} {
		if _, err := store.Deliver(ctx, "thread", InboxMessage{ID: id, Message: core.UserText(id)}); err != nil {
			t.Fatal(err)
		}
	}
	started, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	executions := []ToolExecution{
		{InvocationID: "invocation-a", CallID: "call-a", Tool: "tool", Attempt: 1, State: ToolCompleted, Result: coordinatorMessagePtr(core.AssistantText("a"))},
		{InvocationID: "invocation-b", CallID: "call-b", Tool: "tool", Attempt: 1, State: ToolCompleted, Result: coordinatorMessagePtr(core.AssistantText("b"))},
	}
	for _, execution := range executions {
		claimed, err := store.ClaimTool(ctx, ToolClaimRequest{
			ThreadID: "thread", InvocationID: execution.InvocationID, CallID: execution.CallID, Tool: execution.Tool, Claim: &started.Claim,
		})
		if err != nil || claimed.State != ToolClaimed {
			t.Fatalf("ClaimTool %s = %+v err=%v", execution.InvocationID, claimed, err)
		}
	}
	checkpointRequest := func(reverse bool) *Checkpoint {
		files := map[string][]byte{}
		if reverse {
			files["b.txt"] = []byte("b")
			files["a.txt"] = []byte("a")
		} else {
			files["a.txt"] = []byte("a")
			files["b.txt"] = []byte("b")
		}
		return &Checkpoint{ID: "checkpoint", ThreadID: "thread", State: core.State{KV: map[string]any{"value": "same"}}, FileSnapshot: files}
	}
	syncs := 0
	store.ops = fileOps{
		write: normal.write,
		sync: func(fh *os.File) error {
			syncs++
			return errors.New("injected commit sync failure")
		},
	}
	firstCheckpoint := checkpointRequest(false)
	_, err = store.Commit(ctx, CommitRequest{
		Checkpoint: firstCheckpoint, ExpectedRevision: revisionPtr(base.Revision),
		AckMessageIDs: []string{"message-a", "message-b"}, Claim: &started.Claim, Lease: 2 * time.Minute,
		ToolExecutions: executions,
	})
	if err == nil || !strings.Contains(err.Error(), "injected commit sync failure") {
		t.Fatalf("first Commit error = %v", err)
	}
	if firstCheckpoint.Revision != 0 {
		t.Fatalf("failed Commit changed caller revision to %d", firstCheckpoint.Revision)
	}
	if got := journalCommitIDCount(t, filepath.Join(dir, "thread.jsonl"), "checkpoint"); got != 1 {
		t.Fatalf("checkpoint commit records after failed Sync = %d, want 1", got)
	}
	if digest := journalCommitDigest(t, filepath.Join(dir, "thread.jsonl"), "checkpoint"); digest == "" {
		t.Fatal("commit record did not persist request_digest")
	}

	retryStore := newFileRecoveryStore(t, dir, now)
	assertFileRecoveryContents(t, retryStore, "thread", "checkpoint", 2, nil, executions[0], executions[1])
	if err := retryStore.withThread(ctx, "thread", func(st *fileState) error {
		if st.claim == nil || !sameClaim(*st.claim, started.Claim) || st.fencing != started.Claim.FencingToken {
			t.Fatalf("claim after failed commit Sync = %+v fencing=%d", st.claim, st.fencing)
		}
		if !st.claim.ExpiresAt.Equal(now.Add(2 * time.Minute)) {
			t.Fatalf("claim expiry after failed commit Sync = %v", st.claim.ExpiresAt)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	retryStore.ops = fileOps{
		write: normal.write,
		sync: func(fh *os.File) error {
			syncs++
			return normal.sync(fh)
		},
	}
	ignoredExpected := uint64(999)
	retryCheckpoint := checkpointRequest(true)
	retryCheckpoint.Revision = 42
	receipt, err := retryStore.Commit(ctx, CommitRequest{
		Checkpoint: retryCheckpoint, ExpectedRevision: &ignoredExpected,
		AckMessageIDs: []string{"message-b", "message-a", "message-a"}, Claim: &started.Claim, Lease: time.Hour,
		ToolExecutions: []ToolExecution{executions[1], executions[0]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if syncs != 2 || receipt.Revision != 2 || retryCheckpoint.Revision != 2 || receipt.Claim == nil {
		t.Fatalf("retry receipt=%+v checkpoint revision=%d syncs=%d", receipt, retryCheckpoint.Revision, syncs)
	}
	if got := journalCommitIDCount(t, filepath.Join(dir, "thread.jsonl"), "checkpoint"); got != 1 {
		t.Fatalf("retry appended %d commit records, want 1", got)
	}
	history, err := retryStore.History(ctx, "thread")
	if err != nil || len(history) != 2 || history[0].ID != "base" || history[1].ID != "checkpoint" {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	inbox, err := retryStore.Inbox(ctx, "thread")
	if err != nil || len(inbox) != 0 {
		t.Fatalf("inbox=%+v err=%v", inbox, err)
	}
	for _, execution := range executions {
		got, ok, err := retryStore.ToolExecution(ctx, "thread", execution.InvocationID)
		if err != nil || !ok || got.State != ToolCompleted || got.Result == nil || got.Result.Text() != execution.Result.Text() {
			t.Fatalf("tool execution %s=%+v ok=%v err=%v", execution.InvocationID, got, ok, err)
		}
	}
	conflict := checkpointRequest(false)
	conflict.FileSnapshot["a.txt"] = []byte("different")
	if _, err := retryStore.Commit(ctx, CommitRequest{
		Checkpoint: conflict, Claim: &started.Claim,
		AckMessageIDs: []string{"message-a", "message-b"}, ToolExecutions: executions,
	}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("conflicting retry error = %v, want ErrCheckpointConflict", err)
	}
}

func TestSnapshotConsistencyFileLegacyCommitDigest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Date(2026, time.October, 12, 20, 0, 0, 0, time.UTC)
	store, err := NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	started, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	execution := ToolExecution{
		InvocationID: "invocation", CallID: "call", Tool: "tool", Attempt: 1, State: ToolCompleted,
		Result: coordinatorMessagePtr(core.AssistantText("done")),
	}
	requestCheckpoint := func(value string) *Checkpoint {
		return &Checkpoint{
			ID: "legacy-commit", ThreadID: "thread", State: core.State{KV: map[string]any{"value": value}},
			FileSnapshot: map[string][]byte{"note.txt": []byte("snapshot")},
		}
	}
	if err := store.withThread(ctx, "thread", func(st *fileState) error {
		stored, err := freezeCheckpoint(requestCheckpoint("same"))
		if err != nil {
			return err
		}
		stored.Revision = st.revision + 1
		checkpoint, blobs, _, err := buildCheckpointRecord(stored, st.blobs)
		if err != nil {
			return err
		}
		rec := journalRecord{
			Version: durableFormatVersion, Kind: recordCommit, Checkpoint: checkpoint,
			AckMessageIDs: []string{"message"}, Claim: &started.Claim, ToolExecutions: []ToolExecution{execution},
		}
		return store.appendRecordLocked("thread", st, blobs, rec)
	}); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	reopened.now = func() time.Time { return now }
	retry := requestCheckpoint("same")
	receipt, err := reopened.Commit(ctx, CommitRequest{
		Checkpoint: retry, AckMessageIDs: []string{"message"}, Claim: &started.Claim, ToolExecutions: []ToolExecution{execution},
	})
	if err != nil || receipt.Revision != 1 || retry.Revision != 1 {
		t.Fatalf("legacy equivalent retry receipt=%+v revision=%d err=%v", receipt, retry.Revision, err)
	}
	if _, err := reopened.Commit(ctx, CommitRequest{
		Checkpoint: requestCheckpoint("different"), AckMessageIDs: []string{"message"}, Claim: &started.Claim,
		ToolExecutions: []ToolExecution{execution},
	}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("legacy conflicting retry error = %v, want ErrCheckpointConflict", err)
	}
}

func newFileRecoveryStore(t *testing.T, dir string, now time.Time) *File {
	t.Helper()
	store, err := NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	return store
}

func seedFileRecoveryThread(t *testing.T, store *File, threadID, messageID string) *Checkpoint {
	t.Helper()
	ctx := context.Background()
	cp := &Checkpoint{ID: "base", ThreadID: threadID, State: core.State{KV: map[string]any{"value": "base"}}}
	if err := store.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Deliver(ctx, threadID, InboxMessage{ID: messageID, Message: core.UserText(messageID)}); err != nil {
		t.Fatal(err)
	}
	return cp
}

func fileRecoveryExecution(state ToolExecutionState) ToolExecution {
	return ToolExecution{InvocationID: "invocation", CallID: "call", Tool: "tool", Attempt: 1, State: state}
}

func revisionPtr(revision uint64) *uint64 { return &revision }

func assertFileRecoveryContents(
	t *testing.T,
	store *File,
	threadID, checkpointID string,
	revision uint64,
	inboxIDs []string,
	executions ...ToolExecution,
) {
	t.Helper()
	ctx := context.Background()
	head, err := store.Latest(ctx, threadID)
	if err != nil || head == nil || head.ID != checkpointID || head.Revision != revision {
		t.Fatalf("head=%+v err=%v, want id=%q revision=%d", head, err, checkpointID, revision)
	}
	inbox, err := store.Inbox(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != len(inboxIDs) {
		t.Fatalf("inbox=%+v, want IDs %v", inbox, inboxIDs)
	}
	for i, id := range inboxIDs {
		if inbox[i].ID != id {
			t.Fatalf("inbox=%+v, want IDs %v", inbox, inboxIDs)
		}
	}
	for _, want := range executions {
		got, ok, err := store.ToolExecution(ctx, threadID, want.InvocationID)
		if err != nil || !ok {
			t.Fatalf("tool execution %q=%+v ok=%v err=%v", want.InvocationID, got, ok, err)
		}
		if got.InvocationID != want.InvocationID || got.CallID != want.CallID || got.Tool != want.Tool ||
			got.Attempt != want.Attempt || got.State != want.State {
			t.Fatalf("tool execution %q=%+v, want %+v", want.InvocationID, got, want)
		}
		if want.Result == nil {
			if got.Result != nil {
				t.Fatalf("tool execution %q result=%+v, want nil", want.InvocationID, got.Result)
			}
		} else if got.Result == nil || got.Result.Text() != want.Result.Text() {
			t.Fatalf("tool execution %q result=%+v, want %+v", want.InvocationID, got.Result, want.Result)
		}
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func journalKindCount(t *testing.T, path, kind string) int {
	t.Helper()
	data := mustReadFile(t, path)
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var probe struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(line), &probe); err != nil {
			t.Fatal(err)
		}
		if probe.Kind == kind {
			count++
		}
	}
	return count
}

func journalCommitIDCount(t *testing.T, path, checkpointID string) int {
	t.Helper()
	data := mustReadFile(t, path)
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var rec journalRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Kind == recordCommit && rec.Checkpoint != nil && rec.Checkpoint.ID == checkpointID {
			count++
		}
	}
	return count
}

func journalCommitDigest(t *testing.T, path, checkpointID string) string {
	t.Helper()
	data := mustReadFile(t, path)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var rec journalRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Kind == recordCommit && rec.Checkpoint != nil && rec.Checkpoint.ID == checkpointID {
			return rec.RequestDigest
		}
	}
	return ""
}
