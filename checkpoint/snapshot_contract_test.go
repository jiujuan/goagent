package checkpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

func TestSnapshotConsistencyMemorySave(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	input := &checkpoint.Checkpoint{
		ID: "cp1", ThreadID: "thread1",
		State: core.State{KV: map[string]any{
			"nested": map[string]any{"value": "before"},
		}},
	}
	if err := store.Save(ctx, input); err != nil {
		t.Fatal(err)
	}
	input.State.KV["nested"].(map[string]any)["value"] = "changed"
	first, err := store.Latest(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	if first == nil {
		t.Fatal("missing checkpoint")
	}
	if got := first.State.KV["nested"].(map[string]any)["value"]; got != "before" {
		t.Fatalf("saved snapshot changed through input: %v", got)
	}
	first.State.KV["nested"].(map[string]any)["value"] = "changed-via-read"
	second, err := store.Latest(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	if second == nil {
		t.Fatal("missing checkpoint on second read")
	}
	if got := second.State.KV["nested"].(map[string]any)["value"]; got != "before" {
		t.Fatalf("saved snapshot changed through read: %v", got)
	}
}

func TestSnapshotConsistencyMemoryReads(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	cp := &checkpoint.Checkpoint{
		ID:       "cp1",
		ThreadID: "thread1",
		State: core.State{
			Messages: []core.Message{{
				Role: core.RoleUser,
				Parts: []core.Part{
					core.Image{MIME: "image/png", Data: []byte("image")},
					core.ToolCall{ID: "call1", Name: "lookup", Args: json.RawMessage("{\"key\":\"before\"}")},
				},
			}},
			KV: map[string]any{"nested": map[string]any{"value": "before"}},
		},
		FileSnapshot: map[string][]byte{"note.txt": []byte("file")},
	}
	if err := store.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(ctx, "thread1", "cp1")
	if err != nil {
		t.Fatal(err)
	}
	mutateCheckpointData(loaded)

	latest, err := store.Latest(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	assertCheckpointDataUnchanged(t, latest)
	mutateCheckpointData(latest)

	history, err := store.History(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history length = %d, want 1", len(history))
	}
	assertCheckpointDataUnchanged(t, history[0])
	mutateCheckpointData(history[0])

	again, err := store.History(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 {
		t.Fatalf("second history length = %d, want 1", len(again))
	}
	assertCheckpointDataUnchanged(t, again[0])
}

func TestSnapshotConsistencyPendingPause(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	cp := &checkpoint.Checkpoint{
		ID:       "paused",
		ThreadID: "thread1",
		State:    core.State{},
		Pending: &checkpoint.PendingHITL{
			Step: 3,
			Pending: []core.ToolCall{{
				ID: "call1", Name: "danger", Args: json.RawMessage("{\"approved\":false}"),
			}},
		},
		Pause: &checkpoint.Pause{
			ID: "pause1", Phase: "before_tool", Reason: "approval required", Recovery: checkpoint.RecoveryResumeTools,
		},
	}
	if err := store.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}

	cp.Pending.Step = 99
	cp.Pending.Pending[0].Args[0] = 'X'
	cp.Pause.Reason = "changed"
	first, err := store.Latest(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	assertPendingPauseUnchanged(t, first)

	first.Pending.Step = 100
	first.Pending.Pending[0].Args[0] = 'Y'
	first.Pause.Reason = "changed-via-read"
	second, err := store.Latest(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	assertPendingPauseUnchanged(t, second)
}

func TestSnapshotConsistencyCommitCloneFailureAtomic(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	base := &checkpoint.Checkpoint{ID: "base", ThreadID: "thread1", State: core.State{}}
	if err := store.Save(ctx, base); err != nil {
		t.Fatal(err)
	}
	msg := checkpoint.InboxMessage{ID: "message1", Message: core.UserText("keep me")}
	if _, err := store.Deliver(ctx, "thread1", msg); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimTool(ctx, checkpoint.ToolClaimRequest{
		ThreadID: "thread1", InvocationID: "invocation1", CallID: "call1", Tool: "danger",
	})
	if err != nil {
		t.Fatal(err)
	}
	if claimed.State != checkpoint.ToolClaimed {
		t.Fatalf("initial tool state = %q, want %q", claimed.State, checkpoint.ToolClaimed)
	}

	bad := &checkpoint.Checkpoint{
		ID:       "bad",
		ThreadID: "thread1",
		State:    core.State{KV: map[string]any{"not_cloneable": make(chan int)}},
	}
	_, err = store.Commit(ctx, checkpoint.CommitRequest{
		Checkpoint:    bad,
		AckMessageIDs: []string{msg.ID},
		ToolExecutions: []checkpoint.ToolExecution{{
			InvocationID: "invocation1",
			CallID:       "call1",
			Tool:         "danger",
			Attempt:      1,
			State:        checkpoint.ToolCompleted,
			Result:       messagePtr(core.AssistantText("completed")),
		}},
	})
	if !errors.Is(err, core.ErrStateNotCloneable) {
		t.Fatalf("Commit error = %v, want ErrStateNotCloneable", err)
	}
	if bad.Revision != 0 {
		t.Fatalf("failed Commit changed caller revision to %d", bad.Revision)
	}

	latest, err := store.Latest(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.ID != "base" || latest.Revision != 1 {
		t.Fatalf("failed Commit changed history: latest=%+v", latest)
	}
	history, err := store.History(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("failed Commit changed history length to %d", len(history))
	}
	inbox, err := store.Inbox(ctx, "thread1")
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 1 || inbox[0].ID != msg.ID {
		t.Fatalf("failed Commit acknowledged inbox: %+v", inbox)
	}
	execution, ok, err := store.ToolExecution(ctx, "thread1", "invocation1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || execution.State != checkpoint.ToolClaimed || execution.Result != nil {
		t.Fatalf("failed Commit changed tool execution: ok=%v execution=%+v", ok, execution)
	}

	next := &checkpoint.Checkpoint{ID: "next", ThreadID: "thread1", State: core.State{}}
	if err := store.Save(ctx, next); err != nil {
		t.Fatal(err)
	}
	if next.Revision != 2 {
		t.Fatalf("revision after failed Commit = %d, want 2", next.Revision)
	}
}

func TestSnapshotConsistencyToolResultOwnership(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	result := core.Message{
		Role: core.RoleTool,
		Parts: []core.Part{
			core.Image{MIME: "image/png", Data: []byte("image")},
			core.ToolResult{
				CallID: "call1",
				Name:   "lookup",
				Content: []core.Part{
					core.ToolCall{ID: "nested", Name: "inspect", Args: json.RawMessage("{\"value\":\"before\"}")},
				},
			},
		},
	}
	if _, err := store.Commit(ctx, checkpoint.CommitRequest{
		Checkpoint: &checkpoint.Checkpoint{ID: "cp1", ThreadID: "thread1", State: core.State{}},
		ToolExecutions: []checkpoint.ToolExecution{{
			InvocationID: "invocation1",
			CallID:       "call1",
			Tool:         "lookup",
			Attempt:      1,
			State:        checkpoint.ToolCompleted,
			Result:       &result,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	mutateToolResultMessage(&result)
	first, ok, err := store.ToolExecution(ctx, "thread1", "invocation1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || first.Result == nil {
		t.Fatalf("missing tool execution: ok=%v execution=%+v", ok, first)
	}
	assertToolResultMessageUnchanged(t, first.Result)

	mutateToolResultMessage(first.Result)
	second, ok, err := store.ToolExecution(ctx, "thread1", "invocation1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || second.Result == nil {
		t.Fatalf("missing tool execution on second read: ok=%v execution=%+v", ok, second)
	}
	assertToolResultMessageUnchanged(t, second.Result)
}

func mutateCheckpointData(cp *checkpoint.Checkpoint) {
	cp.State.KV["nested"].(map[string]any)["value"] = "changed"
	image := cp.State.Messages[0].Parts[0].(core.Image)
	image.Data[0] = 'X'
	call := cp.State.Messages[0].Parts[1].(core.ToolCall)
	call.Args[0] = 'X'
	cp.FileSnapshot["note.txt"][0] = 'X'
}

func assertCheckpointDataUnchanged(t *testing.T, cp *checkpoint.Checkpoint) {
	t.Helper()
	if cp == nil {
		t.Fatal("missing checkpoint")
	}
	if got := cp.State.KV["nested"].(map[string]any)["value"]; got != "before" {
		t.Fatalf("KV value = %v, want before", got)
	}
	if got := string(cp.State.Messages[0].Parts[0].(core.Image).Data); got != "image" {
		t.Fatalf("image data = %q, want image", got)
	}
	if got := string(cp.State.Messages[0].Parts[1].(core.ToolCall).Args); got != "{\"key\":\"before\"}" {
		t.Fatalf("tool args = %q", got)
	}
	if got := string(cp.FileSnapshot["note.txt"]); got != "file" {
		t.Fatalf("file snapshot = %q, want file", got)
	}
}

func assertPendingPauseUnchanged(t *testing.T, cp *checkpoint.Checkpoint) {
	t.Helper()
	if cp == nil || cp.Pending == nil || cp.Pause == nil {
		t.Fatalf("missing pending pause data: %+v", cp)
	}
	if cp.Pending.Step != 3 || len(cp.Pending.Pending) != 1 {
		t.Fatalf("pending = %+v", cp.Pending)
	}
	if got := string(cp.Pending.Pending[0].Args); got != "{\"approved\":false}" {
		t.Fatalf("pending args = %q", got)
	}
	if cp.Pause.Reason != "approval required" {
		t.Fatalf("pause reason = %q", cp.Pause.Reason)
	}
}

func messagePtr(message core.Message) *core.Message { return &message }

func mutateToolResultMessage(message *core.Message) {
	image := message.Parts[0].(core.Image)
	image.Data[0] = 'X'
	result := message.Parts[1].(core.ToolResult)
	call := result.Content[0].(core.ToolCall)
	call.Args[0] = 'X'
}

func assertToolResultMessageUnchanged(t *testing.T, message *core.Message) {
	t.Helper()
	if got := string(message.Parts[0].(core.Image).Data); got != "image" {
		t.Fatalf("tool result image data = %q, want image", got)
	}
	result := message.Parts[1].(core.ToolResult)
	if got := string(result.Content[0].(core.ToolCall).Args); got != "{\"value\":\"before\"}" {
		t.Fatalf("nested tool args = %q", got)
	}
}
