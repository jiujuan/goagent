package checkpoint_test

import (
	"context"
	"errors"
	"strings"
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

func TestInboxPhaseRoundTripMemoryAndFile(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		if _, err := backend.store.Deliver(ctx, "thread", checkpoint.InboxMessage{
			ID:      "no-phase",
			Message: core.UserText("first"),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.store.Deliver(ctx, "thread", checkpoint.InboxMessage{
			ID:      "with-phase",
			Message: core.UserText("second"),
			Phase:   core.InputNextTurn,
		}); err != nil {
			t.Fatal(err)
		}

		inbox, err := backend.store.Inbox(ctx, "thread")
		if err != nil {
			t.Fatal(err)
		}
		if len(inbox) != 2 {
			t.Fatalf("inbox holds %d messages, want 2", len(inbox))
		}
		if inbox[0].ID != "no-phase" || inbox[0].Phase != core.InputBeforeModel {
			t.Fatalf("first record = %+v, want the empty phase stored as before_model", inbox[0])
		}
		if inbox[1].ID != "with-phase" || inbox[1].Phase != core.InputNextTurn {
			t.Fatalf("second record = %+v, want next_turn", inbox[1])
		}
	})
}

func TestInboxSameIDDifferentPhaseConflicts(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		msg := core.UserText("same words")
		if _, err := backend.store.Deliver(ctx, "thread", checkpoint.InboxMessage{
			ID: "dup", Message: msg, Phase: core.InputBeforeModel,
		}); err != nil {
			t.Fatal(err)
		}
		_, err := backend.store.Deliver(ctx, "thread", checkpoint.InboxMessage{
			ID: "dup", Message: msg, Phase: core.InputNextTurn,
		})
		if err == nil {
			t.Fatal("a phase change under the same ID must conflict, not redeliver")
		}
		if !strings.Contains(err.Error(), "dup") {
			t.Fatalf("conflict error = %v, want it to name the message ID", err)
		}
		inbox, err := backend.store.Inbox(ctx, "thread")
		if err != nil {
			t.Fatal(err)
		}
		if len(inbox) != 1 || inbox[0].Phase != core.InputBeforeModel {
			t.Fatalf("inbox = %+v, want the original record untouched", inbox)
		}
	})
}

func TestInboxSameIDSamePhaseIsDuplicate(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		msg := core.UserText("retry me")
		first, err := backend.store.Deliver(ctx, "thread", checkpoint.InboxMessage{
			ID: "same", Message: msg,
		})
		if err != nil {
			t.Fatal(err)
		}
		if first.ID != "same" || first.Duplicate || first.State != core.InputStateQueued {
			t.Fatalf("first receipt = %+v, want accepted and queued", first)
		}
		// An empty phase and an explicit before_model are the same instruction, so
		// a producer retry after an ambiguous failure stays a duplicate.
		again, err := backend.store.Deliver(ctx, "thread", checkpoint.InboxMessage{
			ID: "same", Message: msg, Phase: core.InputBeforeModel,
		})
		if err != nil {
			t.Fatalf("retry with an equivalent phase: %v", err)
		}
		if !again.Duplicate || again.State != core.InputStateQueued {
			t.Fatalf("retry receipt = %+v, want duplicate and queued", again)
		}
	})
}

func TestInboxUnknownPhaseRejected(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		_, err := backend.store.Deliver(ctx, "thread", checkpoint.InboxMessage{
			ID: "bad", Message: core.UserText("odd"), Phase: core.InputPhase("later"),
		})
		if !errors.Is(err, core.ErrUnknownInputPhase) {
			t.Fatalf("unknown phase error = %v, want ErrUnknownInputPhase", err)
		}
		inbox, err := backend.store.Inbox(ctx, "thread")
		if err != nil {
			t.Fatal(err)
		}
		if len(inbox) != 0 {
			t.Fatalf("inbox = %+v, want the refused record absent", inbox)
		}
	})
}

// TestFileJournalValidatesInFlightRecord covers the in-flight boundary on the
// append-only journal: a legal batch survives the replay that every read does,
// and a record that carries a batch alongside a pause never reaches the journal.
func TestFileJournalValidatesInFlightRecord(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		if err := backend.check.Save(ctx, inFlightCheckpoint("thread", "journal-inflight")); err != nil {
			t.Fatalf("save in-flight record: %v", err)
		}
		head, err := backend.check.Latest(ctx, "thread")
		if err != nil {
			t.Fatal(err)
		}
		if head.InFlight == nil || len(head.InFlight.Calls) != 1 || head.InFlight.Calls[0].ID != "call-1" {
			t.Fatalf("replayed in-flight batch = %+v", head.InFlight)
		}
		if string(head.InFlight.Calls[0].Args) != `{"query":"first"}` {
			t.Fatalf("replayed arguments = %s, want the recorded payload", head.InFlight.Calls[0].Args)
		}

		conflicting := inFlightCheckpoint("thread", "journal-conflict")
		conflicting.Pause = &checkpoint.Pause{
			ID: "pause-1", Phase: "before_tool", Recovery: checkpoint.RecoveryResumeTools,
		}
		if err := backend.check.Save(ctx, conflicting); !errors.Is(err, checkpoint.ErrInFlightConflict) {
			t.Fatalf("save conflicting record error = %v, want ErrInFlightConflict", err)
		}
		again, err := backend.check.Latest(ctx, "thread")
		if err != nil {
			t.Fatal(err)
		}
		if again.ID != "journal-inflight" {
			t.Fatalf("rejected record became visible: %+v", again)
		}
	})
}
