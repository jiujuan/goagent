package checkpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// inFlightCheckpoint builds the one shape of an in-flight batch the store
// accepts: the batch belongs to this step, the step snapshot that produced it is
// recorded, and the state history ends with the assistant message declaring it.
func inFlightCheckpoint(threadID, id string) *checkpoint.Checkpoint {
	call := core.ToolCall{ID: "call-1", Name: "search", Args: json.RawMessage(`{"query":"first"}`)}
	return &checkpoint.Checkpoint{
		ID:       id,
		ThreadID: threadID,
		Step:     2,
		State: core.State{Messages: []core.Message{
			core.UserText("find something"),
			{Role: core.RoleAssistant, Parts: []core.Part{call}},
		}},
		Snapshot: &checkpoint.StepSnapshot{
			Step:           2,
			Advertised:     []string{"search", "write"},
			ToolDigest:     "tool-digest",
			SettingsDigest: "settings-digest",
			Model:          "test-model",
		},
		InFlight: &checkpoint.InFlightBatch{Step: 2, Calls: []core.ToolCall{call}},
	}
}

// TestInFlightCoexistsWithNothingElse is the storage invariant: an in-flight
// batch is the only work state on its checkpoint, and a checkpoint that breaks
// that never becomes visible.
func TestInFlightCoexistsWithNothingElse(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		if err := backend.check.Save(ctx, inFlightCheckpoint("thread", "cp-inflight")); err != nil {
			t.Fatalf("save a legal in-flight checkpoint: %v", err)
		}
		head, err := backend.check.Latest(ctx, "thread")
		if err != nil {
			t.Fatal(err)
		}
		if head.InFlight == nil || head.InFlight.Step != 2 || len(head.InFlight.Calls) != 1 {
			t.Fatalf("in-flight batch lost in storage: %+v", head.InFlight)
		}

		for _, tc := range []struct {
			name string
			edit func(*checkpoint.Checkpoint)
		}{
			{name: "pending", edit: func(cp *checkpoint.Checkpoint) {
				cp.Pending = &checkpoint.PendingHITL{Step: 2}
			}},
			{name: "pause", edit: func(cp *checkpoint.Checkpoint) {
				cp.Pause = &checkpoint.Pause{ID: "p1", Phase: "before_tool", Recovery: checkpoint.RecoveryResumeTools}
			}},
			{name: "completion", edit: func(cp *checkpoint.Checkpoint) {
				cp.Completion = &checkpoint.Completion{RunID: "run-1", Kind: checkpoint.CompletionCompleted}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cp := inFlightCheckpoint("thread", "cp-conflict-"+tc.name)
				tc.edit(cp)
				err := backend.check.Save(ctx, cp)
				if !errors.Is(err, checkpoint.ErrInFlightConflict) {
					t.Fatalf("save with %s error = %v, want ErrInFlightConflict", tc.name, err)
				}
				got, err := backend.check.Latest(ctx, "thread")
				if err != nil {
					t.Fatal(err)
				}
				if got.ID != "cp-inflight" {
					t.Fatalf("rejected record became visible: %+v", got)
				}
			})
		}
	})
}

func TestInFlightRequiresSnapshot(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		for _, tc := range []struct {
			name string
			edit func(*checkpoint.Checkpoint)
		}{
			{name: "no snapshot", edit: func(cp *checkpoint.Checkpoint) { cp.Snapshot = nil }},
			{name: "snapshot step differs", edit: func(cp *checkpoint.Checkpoint) { cp.Snapshot.Step = 1 }},
			{name: "snapshot has no tool digest", edit: func(cp *checkpoint.Checkpoint) { cp.Snapshot.ToolDigest = "" }},
			{name: "batch step differs", edit: func(cp *checkpoint.Checkpoint) { cp.InFlight.Step = 1 }},
			{name: "no calls", edit: func(cp *checkpoint.Checkpoint) { cp.InFlight.Calls = nil }},
			{name: "call without ID", edit: func(cp *checkpoint.Checkpoint) { cp.InFlight.Calls[0].ID = "" }},
			{name: "call not advertised", edit: func(cp *checkpoint.Checkpoint) { cp.InFlight.Calls[0].Name = "delete" }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cp := inFlightCheckpoint("thread", "cp-invalid")
				tc.edit(cp)
				err := backend.check.Save(ctx, cp)
				if !errors.Is(err, checkpoint.ErrInFlightConflict) {
					t.Fatalf("save error = %v, want ErrInFlightConflict", err)
				}
			})
		}
	})
}

// TestInFlightCallIDsMayRepeat keeps the store from inventing a uniqueness rule:
// a model can emit the same call ID twice, and recovery matches calls in order.
func TestInFlightCallIDsMayRepeat(t *testing.T) {
	eachDurableBackend(t, func(t *testing.T, backend durableBackend) {
		ctx := context.Background()
		cp := inFlightCheckpoint("thread", "cp-repeat")
		cp.Snapshot.Advertised = []string{"search"}
		cp.InFlight.Calls = []core.ToolCall{
			{ID: "call-1", Name: "search", Args: json.RawMessage(`{"query":"first"}`)},
			{ID: "call-1", Name: "search", Args: json.RawMessage(`{"query":"second"}`)},
		}
		if err := backend.check.Save(ctx, cp); err != nil {
			t.Fatalf("save repeated call IDs: %v", err)
		}
		got, err := backend.check.Latest(ctx, "thread")
		if err != nil {
			t.Fatal(err)
		}
		if len(got.InFlight.Calls) != 2 || got.InFlight.Calls[1].ID != "call-1" {
			t.Fatalf("stored batch = %+v, want both calls in order", got.InFlight)
		}
		if string(got.InFlight.Calls[1].Args) != `{"query":"second"}` {
			t.Fatalf("second call args = %s, want the recorded arguments", got.InFlight.Calls[1].Args)
		}
	})
}

func TestRequiresResume(t *testing.T) {
	inFlight := inFlightCheckpoint("thread", "cp")
	if checkpoint.RequiresResume(nil) {
		t.Fatal("RequiresResume(nil) = true, want false")
	}
	stateOnly := &checkpoint.Checkpoint{ID: "cp", ThreadID: "thread", State: core.State{KV: map[string]any{"k": "v"}}}
	if checkpoint.RequiresResume(stateOnly) {
		t.Fatal("a plain checkpoint requires no resume")
	}
	pending := inFlightCheckpoint("thread", "cp-pending")
	pending.InFlight = nil
	pending.Pending = &checkpoint.PendingHITL{Step: 2}
	paused := inFlightCheckpoint("thread", "cp-pause")
	paused.InFlight = nil
	paused.Pause = &checkpoint.Pause{ID: "p", Phase: "after_model", Recovery: checkpoint.RecoveryReplayModel}
	for _, tc := range []struct {
		name string
		cp   *checkpoint.Checkpoint
	}{
		{"pending", pending},
		{"pause", paused},
		{"in flight", inFlight},
	} {
		if !checkpoint.RequiresResume(tc.cp) {
			t.Errorf("RequiresResume(%s) = false, want true", tc.name)
		}
	}
	completed := inFlightCheckpoint("thread", "cp-completed")
	completed.InFlight = nil
	completed.Completion = &checkpoint.Completion{RunID: "run-1", Kind: checkpoint.CompletionCompleted}
	if checkpoint.RequiresResume(completed) {
		t.Fatal("a terminal record needs no resume")
	}
}
