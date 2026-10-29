package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func internalRecTool(name string) tool.Tool {
	return tool.New(name, name, func(_ *tool.Context, _ struct{}) (string, error) {
		return "ok", nil
	})
}

func internalInFlightSnapshot(step int, tools ...tool.Tool) *checkpoint.StepSnapshot {
	names := make([]string, len(tools))
	for i, t := range tools {
		names[i] = t.Name()
	}
	return &checkpoint.StepSnapshot{
		Step:           step,
		Advertised:     names,
		ToolDigest:     toolDigest(tool.Schemas(tools)),
		SettingsDigest: "seeded-settings",
	}
}

func internalPair(calls []core.ToolCall) core.Message {
	parts := make([]core.Part, len(calls))
	for i, c := range calls {
		parts[i] = c
	}
	return core.Message{Role: core.RoleAssistant, Parts: parts}
}

func seedInternalInFlight(t *testing.T, store checkpoint.Durable, threadID string, step int, calls []core.ToolCall, tools ...tool.Tool) *checkpoint.Checkpoint {
	t.Helper()
	cp := &checkpoint.Checkpoint{
		ID:       core.NewID("seed"),
		ThreadID: threadID,
		Step:     step,
		State: core.State{Messages: []core.Message{
			core.UserText("go"),
			internalPair(calls),
		}},
		InFlight: &checkpoint.InFlightBatch{Step: step, Calls: calls},
		Snapshot: internalInFlightSnapshot(step, tools...),
	}
	if _, err := store.Commit(context.Background(), checkpoint.CommitRequest{Checkpoint: cp}); err != nil {
		t.Fatalf("seed in-flight head: %v", err)
	}
	head, err := store.Latest(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func TestResumeRegistersInFlightBatch(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	rec := internalRecTool("rec")
	calls := []core.ToolCall{{ID: "c1", Name: "rec", Args: json.RawMessage(`{}`)}}
	seedInternalInFlight(t, store, "inflight-registered", 0, calls, rec)

	a, err := New(
		WithModel(mock.New("internal-resume", func(*llm.Request) *llm.Response { return mock.Text("done") })),
		WithTools(rec),
		WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, "inflight-registered")
	if err != nil {
		t.Fatal(err)
	}
	ib := run.rc.inFlight
	if ib == nil {
		t.Fatal("resumed run has no in-flight batch")
	}
	if ib.step != 0 || len(ib.calls) != 1 || ib.calls[0].ID != "c1" {
		t.Fatalf("registered batch = %+v, want step 0 with call c1", ib)
	}
	if ib.snapshot == nil || ib.final.Role != core.RoleAssistant {
		t.Fatalf("registered batch snapshot/final = %+v/%+v", ib.snapshot, ib.final)
	}
	if !run.rc.continuedTurn {
		t.Fatal("in-flight resume did not continue the turn")
	}
	if run.rc.replayBaseline != nil {
		t.Fatalf("in-flight resume set a replay baseline: %+v", run.rc.replayBaseline)
	}
	if run.preparation == nil || run.preparation.claim == nil {
		t.Fatal("in-flight resume established no claim")
	}
}

func TestResumeInFlightRequiresAdvertised(t *testing.T) {
	store := checkpoint.NewMemory()
	a, err := New(
		WithModel(mock.New("internal-preflight", func(*llm.Request) *llm.Response { return mock.Text("done") })),
		WithTools(internalRecTool("rec")),
		WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	cp := &checkpoint.Checkpoint{
		ID:       "advertised",
		ThreadID: "thread",
		Step:     0,
		InFlight: &checkpoint.InFlightBatch{Step: 0, Calls: []core.ToolCall{
			{ID: "c1", Name: "ghost", Args: json.RawMessage(`{}`)},
		}},
		Snapshot: &checkpoint.StepSnapshot{
			Step:           0,
			Advertised:     []string{"rec"},
			ToolDigest:     "some-digest",
			SettingsDigest: "seeded-settings",
		},
	}
	_, err = a.preflightInFlight(context.Background(), cp)
	if !errors.Is(err, ErrToolNotAdvertised) {
		t.Fatalf("preflight = %v, want ErrToolNotAdvertised", err)
	}
	if !errors.Is(err, ErrInFlightUnrecoverable) {
		t.Fatalf("error = %v, want ErrInFlightUnrecoverable", err)
	}
}

func TestResumeInFlightRejectsConflictingHead(t *testing.T) {
	store := checkpoint.NewMemory()
	a, err := New(
		WithModel(mock.New("internal-conflict", func(*llm.Request) *llm.Response { return mock.Text("done") })),
		WithTools(internalRecTool("rec")),
		WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	call := core.ToolCall{ID: "c1", Name: "rec", Args: json.RawMessage(`{}`)}
	cp := &checkpoint.Checkpoint{
		ID:       "conflict",
		ThreadID: "thread",
		Step:     0,
		InFlight: &checkpoint.InFlightBatch{Step: 0, Calls: []core.ToolCall{call}},
		Pause:    &checkpoint.Pause{Phase: "before_tool", Recovery: checkpoint.RecoveryResumeTools},
		Snapshot: internalInFlightSnapshot(0, internalRecTool("rec")),
	}
	_, err = a.resumeInFlight(context.Background(), cp, nil, func() {}, time.Time{})
	if !errors.Is(err, ErrInFlightUnrecoverable) {
		t.Fatalf("resumeInFlight with a conflicting head = %v, want ErrInFlightUnrecoverable", err)
	}
}

func TestWorkflowAgentRejectsInFlight(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	rec := internalRecTool("rec")
	calls := []core.ToolCall{{ID: "c1", Name: "rec", Args: json.RawMessage(`{}`)}}
	seedInternalInFlight(t, store, "inflight-workflow", 0, calls, rec)

	root := wrapWorkflowWithStore("workflow", &sequentialRunner{}, store)
	_, err := root.Resume(ctx, "inflight-workflow")
	if !errors.Is(err, ErrInFlightUnrecoverable) {
		t.Fatalf("workflow Resume of an in-flight head = %v, want ErrInFlightUnrecoverable", err)
	}
	hist, err := store.History(ctx, "inflight-workflow")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("checkpoints = %d, want the seed only (no unexecuted result appended)", len(hist))
	}
	msgs := hist[0].State.Messages
	if len(msgs) != 2 || msgs[len(msgs)-1].Role != core.RoleAssistant {
		t.Fatalf("workflow resume changed the history: %+v", msgs)
	}
}
