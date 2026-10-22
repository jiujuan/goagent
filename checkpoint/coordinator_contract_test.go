package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
)

func TestSnapshotConsistencyBeginRunEmpty(t *testing.T) {
	eachCoordinatorBackend(t, testSnapshotConsistencyBeginRunEmpty)
}

func testSnapshotConsistencyBeginRunEmpty(t *testing.T, backend coordinatorTestBackend) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 12, 9, 0, 0, 0, time.UTC)
	store := backend.open(t, &now)

	first, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if first.Checkpoint != nil {
		t.Fatalf("empty thread checkpoint = %+v, want nil", first.Checkpoint)
	}
	if first.Claim.ThreadID != "thread" || first.Claim.Owner != "owner-a" || first.Claim.Revision != 0 || first.Claim.PauseID != "thread-root" {
		t.Fatalf("empty thread claim = %+v", first.Claim)
	}
	if first.Claim.FencingToken == 0 || !first.Claim.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("empty thread lease = %+v", first.Claim)
	}

	retry, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if retry.Claim.FencingToken != first.Claim.FencingToken {
		t.Fatalf("same-owner BeginRun advanced fencing: first=%d retry=%d", first.Claim.FencingToken, retry.Claim.FencingToken)
	}
	if _, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute}); !errors.Is(err, ErrClaimConflict) {
		t.Fatalf("second owner error = %v, want ErrClaimConflict", err)
	}
}

func TestSnapshotConsistencyBeginRunHead(t *testing.T) {
	eachCoordinatorBackend(t, testSnapshotConsistencyBeginRunHead)
}

func testSnapshotConsistencyBeginRunHead(t *testing.T, backend coordinatorTestBackend) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 12, 10, 0, 0, 0, time.UTC)
	store := backend.open(t, &now)
	head := &Checkpoint{
		ID: "head", ThreadID: "thread", Step: 3,
		State:        core.State{KV: map[string]any{"nested": map[string]any{"value": "stored"}}},
		FileSnapshot: map[string][]byte{"note.txt": []byte("stored")},
	}
	if err := store.Save(ctx, head); err != nil {
		t.Fatal(err)
	}

	got, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if got.Checkpoint == nil || got.Checkpoint.Revision != got.Claim.Revision || got.Checkpoint.Revision != head.Revision {
		t.Fatalf("BeginRun result = %+v, head revision=%d", got, head.Revision)
	}
	if got.Claim.PauseID != EffectivePauseID(head) {
		t.Fatalf("claim pause ID = %q, want %q", got.Claim.PauseID, EffectivePauseID(head))
	}
	got.Checkpoint.State.KV["nested"].(map[string]any)["value"] = "changed"
	got.Checkpoint.FileSnapshot["note.txt"][0] = 'X'
	latest, err := store.Latest(ctx, "thread")
	if err != nil {
		t.Fatal(err)
	}
	if latest.State.KV["nested"].(map[string]any)["value"] != "stored" || string(latest.FileSnapshot["note.txt"]) != "stored" {
		t.Fatalf("mutating BeginRun result changed stored head: %+v", latest)
	}

	if backend.name == "memory" {
		t.Run("clone failure leaves coordination unchanged", func(t *testing.T) {
			broken := newCoordinatorTestMemory(&now)
			broken.byThread["broken"] = []*Checkpoint{{
				ID: "broken-head", ThreadID: "broken", Revision: 1,
				State: core.State{KV: map[string]any{"bad": make(chan int)}},
			}}
			if _, err := broken.BeginRun(ctx, BeginRunRequest{ThreadID: "broken", Owner: "owner", Lease: time.Minute}); !errors.Is(err, core.ErrStateNotCloneable) {
				t.Fatalf("BeginRun clone error = %v, want ErrStateNotCloneable", err)
			}
			st := broken.durable["broken"]
			if st == nil || st.claim != nil || st.fencing != 0 {
				t.Fatalf("clone failure changed coordination: %+v", st)
			}
		})
	}
}

func TestSnapshotConsistencyBeginRunPaused(t *testing.T) {
	eachCoordinatorBackend(t, testSnapshotConsistencyBeginRunPaused)
}

func testSnapshotConsistencyBeginRunPaused(t *testing.T, backend coordinatorTestBackend) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 12, 11, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name string
		edit func(*Checkpoint)
	}{
		{name: "pending", edit: func(cp *Checkpoint) { cp.Pending = &PendingHITL{Step: 1} }},
		{name: "pause", edit: func(cp *Checkpoint) {
			cp.Pause = &Pause{ID: "pause", Phase: "after_model", Recovery: RecoveryReplayModel}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := backend.open(t, &now)
			paused := &Checkpoint{ID: "paused", ThreadID: "thread", State: core.State{}}
			tc.edit(paused)
			if err := store.Save(ctx, paused); err != nil {
				t.Fatal(err)
			}
			if _, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "ordinary", Lease: time.Minute}); !errors.Is(err, ErrResumeRequired) {
				t.Fatalf("BeginRun error = %v, want ErrResumeRequired", err)
			}
			resume, err := store.Claim(ctx, ClaimRequest{
				ThreadID: "thread", Revision: paused.Revision, PauseID: EffectivePauseID(paused), Owner: "resume", Lease: time.Minute,
			})
			if err != nil {
				t.Fatalf("resume Claim: %v", err)
			}
			if resume.Owner != "resume" || resume.PauseID != EffectivePauseID(paused) {
				t.Fatalf("resume claim = %+v", resume)
			}
			if _, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "ordinary", Lease: time.Minute}); !errors.Is(err, ErrClaimConflict) {
				t.Fatalf("BeginRun with live resume owner error = %v, want ErrClaimConflict", err)
			}
		})
	}
}

func TestSnapshotConsistencyRenewClaim(t *testing.T) {
	eachCoordinatorBackend(t, testSnapshotConsistencyRenewClaim)
}

func testSnapshotConsistencyRenewClaim(t *testing.T, backend coordinatorTestBackend) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC)
	store := backend.open(t, &now)
	started, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	original := started.Claim

	now = now.Add(10 * time.Second)
	renewed, err := store.RenewClaim(ctx, original, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.FencingToken != original.FencingToken || renewed.Revision != original.Revision || renewed.PauseID != original.PauseID {
		t.Fatalf("renewal changed claim identity: original=%+v renewed=%+v", original, renewed)
	}
	if !renewed.ExpiresAt.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("renewed expiry = %v", renewed.ExpiresAt)
	}
	if latest, err := store.Latest(ctx, "thread"); err != nil || latest != nil {
		t.Fatalf("renewal changed head: latest=%+v err=%v", latest, err)
	}

	for name, mutate := range map[string]func(*Claim){
		"owner":    func(c *Claim) { c.Owner = "other" },
		"revision": func(c *Claim) { c.Revision++ },
		"pause":    func(c *Claim) { c.PauseID = "other" },
		"fencing":  func(c *Claim) { c.FencingToken++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := renewed
			mutate(&bad)
			if _, err := store.RenewClaim(ctx, bad, time.Hour); !errors.Is(err, ErrStaleFencingToken) {
				t.Fatalf("RenewClaim error = %v, want ErrStaleFencingToken", err)
			}
		})
	}

	now = renewed.ExpiresAt
	if _, err := store.RenewClaim(ctx, renewed, time.Minute); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("expired RenewClaim error = %v, want ErrStaleFencingToken", err)
	}
	next, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if next.Claim.FencingToken <= renewed.FencingToken {
		t.Fatalf("takeover fencing = %d, want greater than %d", next.Claim.FencingToken, renewed.FencingToken)
	}
	if _, err := store.RenewClaim(ctx, renewed, time.Minute); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("old owner renewal error = %v, want ErrStaleFencingToken", err)
	}
}

func TestSnapshotConsistencyExpectedRevision(t *testing.T) {
	eachCoordinatorBackend(t, testSnapshotConsistencyExpectedRevision)
}

func testSnapshotConsistencyExpectedRevision(t *testing.T, backend coordinatorTestBackend) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 12, 13, 0, 0, 0, time.UTC)
	store := backend.open(t, &now)
	base := &Checkpoint{ID: "base", ThreadID: "thread", State: core.State{}}
	if err := store.Save(ctx, base); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Deliver(ctx, "thread", InboxMessage{ID: "message", Message: core.UserText("keep")}); err != nil {
		t.Fatal(err)
	}
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

	wrong := uint64(0)
	failed := &Checkpoint{ID: "failed", ThreadID: "thread", State: core.State{KV: map[string]any{"value": "new"}}}
	_, err = store.Commit(ctx, CommitRequest{
		Checkpoint: failed, ExpectedRevision: &wrong, AckMessageIDs: []string{"message"}, Claim: &started.Claim,
		Lease: time.Hour, ReleaseClaim: true,
		ToolExecutions: []ToolExecution{{
			InvocationID: "invocation", CallID: "call", Tool: "tool", Attempt: 1, State: ToolCompleted,
			Result: coordinatorMessagePtr(core.AssistantText("done")),
		}},
	})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("Commit error = %v, want ErrRevisionConflict", err)
	}
	if failed.Revision != 0 {
		t.Fatalf("failed CAS changed caller revision to %d", failed.Revision)
	}
	latest, _ := store.Latest(ctx, "thread")
	if latest == nil || latest.ID != "base" || latest.Revision != base.Revision {
		t.Fatalf("failed CAS changed head: %+v", latest)
	}
	inbox, _ := store.Inbox(ctx, "thread")
	if len(inbox) != 1 || inbox[0].ID != "message" {
		t.Fatalf("failed CAS acknowledged inbox: %+v", inbox)
	}
	execution, ok, err := store.ToolExecution(ctx, "thread", "invocation")
	if err != nil || !ok || execution.State != claimed.State || execution.Result != nil {
		t.Fatalf("failed CAS changed tool execution: execution=%+v ok=%v err=%v", execution, ok, err)
	}
	current := coordinatorCurrentClaim(store, "thread")
	if current == nil || !current.ExpiresAt.Equal(started.Claim.ExpiresAt) {
		t.Fatalf("failed CAS renewed or released claim: %+v", current)
	}

	empty := backend.open(t, &now)
	zero := uint64(0)
	root := &Checkpoint{ID: "root", ThreadID: "empty", State: core.State{}}
	if _, err := empty.Commit(ctx, CommitRequest{Checkpoint: root, ExpectedRevision: &zero}); err != nil {
		t.Fatalf("pointer-to-zero CAS on empty thread: %v", err)
	}
	if _, err := empty.Commit(ctx, CommitRequest{
		Checkpoint: &Checkpoint{ID: "second", ThreadID: "empty", State: core.State{}}, ExpectedRevision: &zero,
	}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("pointer-to-zero CAS with existing head error = %v, want ErrRevisionConflict", err)
	}
}

func TestSnapshotConsistencyCommitRetry(t *testing.T) {
	eachCoordinatorBackend(t, testSnapshotConsistencyCommitRetry)
}

func testSnapshotConsistencyCommitRetry(t *testing.T, backend coordinatorTestBackend) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 12, 14, 0, 0, 0, time.UTC)
	store := backend.open(t, &now)
	for _, id := range []string{"message-a", "message-b"} {
		if _, err := store.Deliver(ctx, "thread", InboxMessage{ID: id, Message: core.UserText(id)}); err != nil {
			t.Fatal(err)
		}
	}
	started, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	checkpointRequest := func(value string) *Checkpoint {
		return &Checkpoint{
			ID: "checkpoint", ThreadID: "thread", State: core.State{KV: map[string]any{"value": value}},
			FileSnapshot: map[string][]byte{"note.txt": []byte("snapshot")},
		}
	}
	executions := []ToolExecution{
		{InvocationID: "invocation-a", CallID: "call-a", Tool: "tool", Attempt: 1, State: ToolCompleted, Result: coordinatorMessagePtr(core.AssistantText("a"))},
		{InvocationID: "invocation-b", CallID: "call-b", Tool: "tool", Attempt: 1, State: ToolCompleted, Result: coordinatorMessagePtr(core.AssistantText("b"))},
	}
	first, err := store.Commit(ctx, CommitRequest{
		Checkpoint: checkpointRequest("same"), ExpectedRevision: &zero,
		AckMessageIDs: []string{"message-a", "message-b"}, Claim: &started.Claim, Lease: 2 * time.Minute,
		ToolExecutions: executions,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimAfterFirst := coordinatorCurrentClaim(store, "thread")
	if first.Revision != 1 || claimAfterFirst == nil {
		t.Fatalf("first commit receipt=%+v claim=%+v", first, claimAfterFirst)
	}

	now = now.Add(10 * time.Second)
	ignoredExpected := uint64(999)
	retryCheckpoint := checkpointRequest("same")
	retryCheckpoint.Revision = 42
	retry, err := store.Commit(ctx, CommitRequest{
		Checkpoint: retryCheckpoint, ExpectedRevision: &ignoredExpected,
		AckMessageIDs: []string{"message-b", "message-a", "message-a"}, Claim: &started.Claim,
		Lease: time.Hour, ReleaseClaim: true,
		ToolExecutions: []ToolExecution{executions[1], executions[0]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if retry.Revision != first.Revision || retryCheckpoint.Revision != first.Revision {
		t.Fatalf("retry revision = receipt %d checkpoint %d, want %d", retry.Revision, retryCheckpoint.Revision, first.Revision)
	}
	claimAfterRetry := coordinatorCurrentClaim(store, "thread")
	if claimAfterRetry == nil || !claimAfterRetry.ExpiresAt.Equal(claimAfterFirst.ExpiresAt) {
		t.Fatalf("retry renewed or released claim: before=%+v after=%+v", claimAfterFirst, claimAfterRetry)
	}
	history, _ := store.History(ctx, "thread")
	if len(history) != 1 {
		t.Fatalf("retry appended history: %d checkpoints", len(history))
	}

	if _, err := store.Commit(ctx, CommitRequest{
		Checkpoint: checkpointRequest("different"), Claim: &started.Claim,
		AckMessageIDs: []string{"message-a", "message-b"}, ToolExecutions: executions,
	}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("different content retry error = %v, want ErrCheckpointConflict", err)
	}
	conflicts := []struct {
		name string
		req  CommitRequest
	}{
		{
			name: "files",
			req: CommitRequest{
				Checkpoint: func() *Checkpoint {
					cp := checkpointRequest("same")
					cp.FileSnapshot["note.txt"] = []byte("different")
					return cp
				}(),
				Claim: &started.Claim, AckMessageIDs: []string{"message-a", "message-b"}, ToolExecutions: executions,
			},
		},
		{
			name: "ack set",
			req: CommitRequest{
				Checkpoint: checkpointRequest("same"), Claim: &started.Claim,
				AckMessageIDs: []string{"message-a"}, ToolExecutions: executions,
			},
		},
		{
			name: "tool executions",
			req: CommitRequest{
				Checkpoint: checkpointRequest("same"), Claim: &started.Claim,
				AckMessageIDs: []string{"message-a", "message-b"},
				ToolExecutions: []ToolExecution{executions[0], {
					InvocationID: "invocation-b", CallID: "call-b", Tool: "tool", Attempt: 1, State: ToolCompleted,
					Result: coordinatorMessagePtr(core.AssistantText("different")),
				}},
			},
		},
		{
			name: "claim identity",
			req: CommitRequest{
				Checkpoint: checkpointRequest("same"),
				Claim: func() *Claim {
					claim := started.Claim
					claim.Owner = "different"
					return &claim
				}(),
				AckMessageIDs: []string{"message-a", "message-b"}, ToolExecutions: executions,
			},
		},
	}
	for _, tc := range conflicts {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.Commit(ctx, tc.req); !errors.Is(err, ErrCheckpointConflict) {
				t.Fatalf("Commit error = %v, want ErrCheckpointConflict", err)
			}
		})
	}

	now = claimAfterRetry.ExpiresAt
	ownerB, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	leakCheck, err := store.Commit(ctx, CommitRequest{
		Checkpoint: checkpointRequest("same"), Claim: &started.Claim,
		AckMessageIDs: []string{"message-b", "message-a"}, ToolExecutions: []ToolExecution{executions[1], executions[0]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if leakCheck.Claim != nil {
		t.Fatalf("retry leaked another owner's claim: %+v", leakCheck.Claim)
	}
	current := coordinatorCurrentClaim(store, "thread")
	if current == nil || !sameClaim(*current, ownerB.Claim) {
		t.Fatalf("retry changed new owner claim: got=%+v want=%+v", current, ownerB.Claim)
	}

	t.Run("duplicate invocation follows last visible result", func(t *testing.T) {
		duplicateStore := backend.open(t, &now)
		started, err := duplicateStore.BeginRun(ctx, BeginRunRequest{ThreadID: "duplicate", Owner: "owner", Lease: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		firstResult := ToolExecution{
			InvocationID: "invocation", CallID: "call", Tool: "tool", Attempt: 1, State: ToolCompleted,
			Result: coordinatorMessagePtr(core.AssistantText("first")),
		}
		lastResult := firstResult
		lastResult.Result = coordinatorMessagePtr(core.AssistantText("last"))
		zero := uint64(0)
		requestCheckpoint := func() *Checkpoint {
			return &Checkpoint{ID: "duplicate-checkpoint", ThreadID: "duplicate", State: core.State{}}
		}
		if _, err := duplicateStore.Commit(ctx, CommitRequest{
			Checkpoint: requestCheckpoint(), ExpectedRevision: &zero, Claim: &started.Claim,
			ToolExecutions: []ToolExecution{firstResult, lastResult},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := duplicateStore.Commit(ctx, CommitRequest{
			Checkpoint: requestCheckpoint(), Claim: &started.Claim,
			ToolExecutions: []ToolExecution{lastResult, firstResult},
		}); !errors.Is(err, ErrCheckpointConflict) {
			t.Fatalf("reordered duplicate execution error = %v, want ErrCheckpointConflict", err)
		}
	})
}

func TestSnapshotConsistencyStaleWriter(t *testing.T) {
	eachCoordinatorBackend(t, testSnapshotConsistencyStaleWriter)
}

func testSnapshotConsistencyStaleWriter(t *testing.T, backend coordinatorTestBackend) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 12, 15, 0, 0, 0, time.UTC)
	store := backend.open(t, &now)
	ownerA, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-a", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	now = ownerA.Claim.ExpiresAt
	ownerB, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "owner-b", Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Save(ctx, &Checkpoint{ID: "stale-save", ThreadID: "thread", State: core.State{}}); !errors.Is(err, ErrClaimRequired) {
		t.Fatalf("stale Save error = %v, want ErrClaimRequired", err)
	}
	if _, err := store.Commit(ctx, CommitRequest{
		Checkpoint: &Checkpoint{ID: "stale-commit", ThreadID: "thread", State: core.State{}}, Claim: &ownerA.Claim,
	}); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale Commit error = %v, want ErrStaleFencingToken", err)
	}
	if _, err := store.ClaimTool(ctx, ToolClaimRequest{
		ThreadID: "thread", InvocationID: "stale-tool", CallID: "call", Tool: "tool", Claim: &ownerA.Claim,
	}); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale ClaimTool error = %v, want ErrStaleFencingToken", err)
	}
	if err := store.ReleaseClaim(ctx, ownerA.Claim); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale ReleaseClaim error = %v, want ErrStaleFencingToken", err)
	}
	if history, err := store.History(ctx, "thread"); err != nil || len(history) != 0 {
		t.Fatalf("stale writer changed history: history=%+v err=%v", history, err)
	}
	if _, ok, err := store.ToolExecution(ctx, "thread", "stale-tool"); err != nil || ok {
		t.Fatalf("stale writer changed tool ledger: ok=%v err=%v", ok, err)
	}
	current := coordinatorCurrentClaim(store, "thread")
	if current == nil || !sameClaim(*current, ownerB.Claim) {
		t.Fatalf("stale writer changed current claim: got=%+v want=%+v", current, ownerB.Claim)
	}
}

type coordinatorTestBackend struct {
	name string
	open func(*testing.T, *time.Time) coordinatorTestStore
}

type coordinatorTestStore struct {
	ThreadCoordinator
	currentClaim func(string) *Claim
}

func eachCoordinatorBackend(t *testing.T, test func(*testing.T, coordinatorTestBackend)) {
	t.Helper()
	backends := []coordinatorTestBackend{
		{
			name: "memory",
			open: func(t *testing.T, now *time.Time) coordinatorTestStore {
				store := newCoordinatorTestMemory(now)
				return coordinatorTestStore{
					ThreadCoordinator: store,
					currentClaim: func(threadID string) *Claim {
						store.mu.RLock()
						defer store.mu.RUnlock()
						st := store.durable[threadID]
						if st == nil {
							return nil
						}
						return cloneClaim(st.claim)
					},
				}
			},
		},
		{
			name: "file",
			open: func(t *testing.T, now *time.Time) coordinatorTestStore {
				store, err := NewFile(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				store.now = func() time.Time { return *now }
				return coordinatorTestStore{
					ThreadCoordinator: store,
					currentClaim: func(threadID string) *Claim {
						var claim *Claim
						if err := store.withThread(context.Background(), threadID, func(st *fileState) error {
							claim = cloneClaim(st.claim)
							return nil
						}); err != nil {
							t.Fatal(err)
						}
						return claim
					},
				}
			},
		},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) { test(t, backend) })
	}
}

func newCoordinatorTestMemory(now *time.Time) *Memory {
	store := NewMemory()
	store.now = func() time.Time { return *now }
	return store
}

func coordinatorCurrentClaim(store coordinatorTestStore, threadID string) *Claim {
	return store.currentClaim(threadID)
}

func coordinatorMessagePtr(message core.Message) *core.Message { return &message }

// TestCommitDigestOmitsInFlightWhenAbsent is the byte-compatibility guard for
// invariant 25: a commit that carries no in-flight batch hashes to the same
// payload it did before the field existed.
func TestCommitDigestOmitsInFlightWhenAbsent(t *testing.T) {
	absent, err := json.Marshal(commitDigestCheckpoint{ID: "cp", ThreadID: "thread", Step: 2})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(absent), `"in_flight"`) {
		t.Fatalf("digest payload carries an empty in-flight key: %s", absent)
	}

	present, err := json.Marshal(commitDigestCheckpoint{
		ID: "cp", ThreadID: "thread", Step: 2,
		InFlight: &commitDigestInFlight{Step: 2, Calls: []commitDigestToolCall{
			{ID: "call-1", Name: "search", Args: []byte(`{"query":"a"}`)},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(present), `"in_flight"`) || !strings.Contains(string(present), "call-1") {
		t.Fatalf("digest payload dropped the in-flight batch: %s", present)
	}
}

func TestSnapshotConsistencyBeginRunInFlight(t *testing.T) {
	eachCoordinatorBackend(t, testSnapshotConsistencyBeginRunInFlight)
}

// testSnapshotConsistencyBeginRunInFlight keeps an in-flight thread from taking
// an ordinary new run, while still letting a resume claim fence it — the same
// coordination a pause gets, reported without the paused wording.
func testSnapshotConsistencyBeginRunInFlight(t *testing.T, backend coordinatorTestBackend) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 12, 11, 30, 0, 0, time.UTC)
	store := backend.open(t, &now)

	head := inFlightStoredFixture("thread", "inflight-head")
	if err := store.Save(ctx, head); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Latest(ctx, "thread")
	if err != nil || stored == nil || stored.InFlight == nil {
		t.Fatalf("in-flight head = %+v err=%v", stored, err)
	}

	_, err = store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "ordinary", Lease: time.Minute})
	if !errors.Is(err, ErrResumeRequired) {
		t.Fatalf("BeginRun error = %v, want ErrResumeRequired", err)
	}
	if strings.Contains(err.Error(), "paused") {
		t.Fatalf("BeginRun error = %v, want wording that also fits an in-flight thread", err)
	}

	resume, err := store.Claim(ctx, ClaimRequest{
		ThreadID: "thread", Revision: stored.Revision, PauseID: EffectivePauseID(stored), Owner: "resume", Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("resume Claim: %v", err)
	}
	if resume.Owner != "resume" || resume.PauseID != EffectivePauseID(stored) {
		t.Fatalf("resume claim = %+v", resume)
	}
	if _, err := store.BeginRun(ctx, BeginRunRequest{ThreadID: "thread", Owner: "ordinary", Lease: time.Minute}); !errors.Is(err, ErrClaimConflict) {
		t.Fatalf("BeginRun with a live resume owner error = %v, want ErrClaimConflict", err)
	}
}
