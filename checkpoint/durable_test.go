package checkpoint_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

type durableBackend struct {
	store checkpoint.Durable
	check checkpoint.Checkpointer
}

func eachDurableBackend(t *testing.T, test func(t *testing.T, backend durableBackend)) {
	t.Helper()
	backends := []struct {
		name string
		open func(t *testing.T) durableBackend
	}{
		{
			name: "memory",
			open: func(*testing.T) durableBackend {
				store := checkpoint.NewMemory()
				return durableBackend{store: store, check: store}
			},
		},
		{
			name: "file",
			open: func(t *testing.T) durableBackend {
				store, err := checkpoint.NewFile(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				return durableBackend{store: store, check: store}
			},
		},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) { test(t, backend.open(t)) })
	}
}

func durableCheckpoint(id, thread string) *checkpoint.Checkpoint {
	return &checkpoint.Checkpoint{ID: id, ThreadID: thread, State: core.State{}}
}

func TestInboxHistoryAndAckCommitTogether(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		msg := checkpoint.InboxMessage{ID: "message-1", Message: core.UserText("durable instruction")}
		if _, err := backend.store.Deliver(ctx, "thread", msg); err != nil {
			t.Fatal(err)
		}

		// Validate every execution before making either side of the transition
		// visible. An invalid execution must leave both history and inbox intact.
		_, err := backend.store.Commit(ctx, checkpoint.CommitRequest{
			Checkpoint:    durableCheckpoint("cp-failed", "thread"),
			AckMessageIDs: []string{msg.ID},
			ToolExecutions: []checkpoint.ToolExecution{{
				State: checkpoint.ToolCompleted,
			}},
		})
		if err == nil {
			t.Fatal("Commit accepted an execution without invocation ID")
		}
		if latest, err := backend.check.Latest(ctx, "thread"); err != nil || latest != nil {
			t.Fatalf("failed Commit changed history: latest=%+v err=%v", latest, err)
		}
		inbox, err := backend.store.Inbox(ctx, "thread")
		if err != nil || len(inbox) != 1 || inbox[0].ID != msg.ID {
			t.Fatalf("failed Commit acknowledged inbox: inbox=%+v err=%v", inbox, err)
		}

		cp := durableCheckpoint("cp-ok", "thread")
		cp.State.Messages = []core.Message{msg.Message}
		if _, err := backend.store.Commit(ctx, checkpoint.CommitRequest{
			Checkpoint:    cp,
			AckMessageIDs: []string{msg.ID},
		}); err != nil {
			t.Fatal(err)
		}
		latest, err := backend.check.Latest(ctx, "thread")
		if err != nil || latest == nil || len(latest.State.Messages) != 1 || latest.State.Messages[0].Text() != "durable instruction" {
			t.Fatalf("committed history = %+v err=%v", latest, err)
		}
		inbox, err = backend.store.Inbox(ctx, "thread")
		if err != nil || len(inbox) != 0 {
			t.Fatalf("committed acknowledgement missing: inbox=%+v err=%v", inbox, err)
		}
	})
}

func TestExpiredOwnerCannotCommit(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		paused := durableCheckpoint("paused", "thread")
		paused.Pause = &checkpoint.Pause{ID: "pause", Phase: "after_model", Recovery: checkpoint.RecoveryReplayModel}
		if err := backend.check.Save(ctx, paused); err != nil {
			t.Fatal(err)
		}
		ownerA, err := backend.store.Claim(ctx, checkpoint.ClaimRequest{
			ThreadID: "thread", Revision: paused.Revision, PauseID: "pause", Owner: "owner-a", Lease: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
		ownerB, err := backend.store.Claim(ctx, checkpoint.ClaimRequest{
			ThreadID: "thread", Revision: paused.Revision, PauseID: "pause", Owner: "owner-b", Lease: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if ownerB.FencingToken <= ownerA.FencingToken {
			t.Fatalf("fencing did not advance: old=%d new=%d", ownerA.FencingToken, ownerB.FencingToken)
		}

		_, err = backend.store.Commit(ctx, checkpoint.CommitRequest{
			Checkpoint: durableCheckpoint("stale", "thread"), Claim: &ownerA,
		})
		if !errors.Is(err, checkpoint.ErrStaleFencingToken) {
			t.Fatalf("stale owner Commit error = %v, want ErrStaleFencingToken", err)
		}
		if _, err := backend.store.Commit(ctx, checkpoint.CommitRequest{
			Checkpoint: durableCheckpoint("fresh", "thread"), Claim: &ownerB, ReleaseClaim: true,
		}); err != nil {
			t.Fatalf("current owner failed to commit: %v", err)
		}
		latest, err := backend.check.Latest(ctx, "thread")
		if err != nil || latest == nil || latest.ID != "fresh" {
			t.Fatalf("latest = %+v err=%v, want fresh checkpoint", latest, err)
		}
	})
}

func TestDecisionIdempotencyAndConflict(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		decision := core.ApprovalDecision{
			DecisionID: "decision-1", PauseID: "pause", CallID: "call", Tool: "danger",
			ParameterDigest: "parameters", PolicyVersion: "policy-v1", Approved: true, Digest: "digest-1",
		}
		first, err := backend.store.RecordDecision(ctx, "thread", decision)
		if err != nil || first.Duplicate {
			t.Fatalf("first decision = %+v err=%v", first, err)
		}
		duplicate, err := backend.store.RecordDecision(ctx, "thread", decision)
		if err != nil || !duplicate.Duplicate || duplicate.Decision != decision {
			t.Fatalf("duplicate decision = %+v err=%v", duplicate, err)
		}
		conflict := decision
		conflict.Digest = "other-digest"
		if _, err := backend.store.RecordDecision(ctx, "thread", conflict); !errors.Is(err, checkpoint.ErrDecisionConflict) {
			t.Fatalf("conflicting decision error = %v, want ErrDecisionConflict", err)
		}
	})
}

func TestToolOutcomeUnknownAfterCrash(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		if err := backend.check.Save(ctx, durableCheckpoint("cp", "thread")); err != nil {
			t.Fatal(err)
		}
		req := checkpoint.ToolClaimRequest{
			ThreadID: "thread", InvocationID: "invocation", CallID: "call", Tool: "danger", IdempotencyKey: "idempotency",
		}
		claimed, err := backend.store.ClaimTool(ctx, req)
		if err != nil || claimed.State != checkpoint.ToolClaimed {
			t.Fatalf("first tool claim = %+v err=%v", claimed, err)
		}
		unknown, err := backend.store.ClaimTool(ctx, req)
		if !errors.Is(err, checkpoint.ErrToolOutcomeUnknown) || unknown.State != checkpoint.ToolUnknown {
			t.Fatalf("recovered tool claim = %+v err=%v, want unknown", unknown, err)
		}
	})
}
