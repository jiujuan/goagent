package agent_test

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestPauseCheckpointCarriesSnapshot(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	call := core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)}
	a, err := agent.New(
		agent.WithModel(phase1Calls(call)),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(phase1ApprovalGate{}),
		agent.WithCheckpointer(store),
		agent.WithModelOptions(llm.WithModel("pause-model")),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(ctx, "go", agent.OnThread("pause_snapshot"))
	if _, err := phase1Drain(t, run); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("tool ran = %d before approval, want 0", got)
	}
	cp, err := store.Latest(ctx, "pause_snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || cp.Snapshot == nil {
		t.Fatalf("pause checkpoint snapshot = %+v, want non-nil", cp)
	}
	if cp.Snapshot.Step != 0 {
		t.Fatalf("snapshot step = %d, want 0", cp.Snapshot.Step)
	}
	if !slices.Contains(cp.Snapshot.Advertised, "danger") {
		t.Fatalf("snapshot advertised = %v, want danger", cp.Snapshot.Advertised)
	}
	if cp.Snapshot.ToolDigest == "" || cp.Snapshot.SettingsDigest == "" {
		t.Fatalf("snapshot digests = %q/%q, want both non-empty", cp.Snapshot.ToolDigest, cp.Snapshot.SettingsDigest)
	}
	if cp.Snapshot.Model != "pause-model" {
		t.Fatalf("snapshot model = %q, want pause-model", cp.Snapshot.Model)
	}
}

func TestResumeCheckpointKeepsPausedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	call := core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)}
	pausedSnapshot := &checkpoint.StepSnapshot{
		Step:           0,
		Advertised:     []string{"danger"},
		ToolDigest:     "paused-tool-digest",
		SettingsDigest: "paused-settings-digest",
		Model:          "paused-model",
	}
	if err := store.Save(ctx, &checkpoint.Checkpoint{
		ID:       "paused",
		ThreadID: "resume_keeps_snapshot",
		State: core.State{Messages: []core.Message{
			core.UserText("go"),
			{Role: core.RoleAssistant, Parts: toolParts([]core.ToolCall{call})},
		}},
		Pending:  &checkpoint.PendingHITL{Step: 0, Pending: []core.ToolCall{call}},
		Pause:    &checkpoint.Pause{Phase: "before_tool", Recovery: checkpoint.RecoveryResumeTools},
		Snapshot: pausedSnapshot,
	}); err != nil {
		t.Fatal(err)
	}
	a, err := agent.New(
		agent.WithModel(phase1Calls(call)),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(phase1ApprovalGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	cont, err := a.Resume(ctx, "resume_keeps_snapshot", agent.Allow("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := phase1Drain(t, cont); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("tool ran = %d, want 1", got)
	}

	history, err := store.History(ctx, "resume_keeps_snapshot")
	if err != nil {
		t.Fatal(err)
	}
	var batchCheckpoint *checkpoint.Checkpoint
	for _, cp := range history {
		if cp.ID != "paused" && cp.Step == 0 {
			batchCheckpoint = cp
		}
	}
	if batchCheckpoint == nil {
		t.Fatalf("no resumed-batch checkpoint in history: %+v", history)
	}
	if !reflect.DeepEqual(batchCheckpoint.Snapshot, pausedSnapshot) {
		t.Fatalf("resumed-batch snapshot = %+v, want paused %+v", batchCheckpoint.Snapshot, pausedSnapshot)
	}
}

func TestNormalStepCheckpointCarriesSnapshot(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	var mu sync.Mutex
	var advertised [][]string
	model := mock.New("normal-snapshot", func(req *llm.Request) *llm.Response {
		mu.Lock()
		advertised = append(advertised, toolNames(req.Tools))
		mu.Unlock()
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "danger", `{}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithCheckpointer(store),
		agent.WithModelOptions(llm.WithModel("normal-model")),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(ctx, "go", agent.OnThread("normal_snapshot"))
	if _, err := phase1Drain(t, run); err != nil {
		t.Fatal(err)
	}

	history, err := store.History(ctx, "normal_snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 {
		t.Fatalf("checkpoints = %d, want 3 (in-flight batch + tool step + final)", len(history))
	}
	if history[0].InFlight == nil || history[0].InFlight.Step != 0 {
		t.Fatalf("checkpoint 0 in-flight = %+v, want the step-0 tool batch", history[0].InFlight)
	}
	for i, cp := range history {
		if cp.Snapshot == nil {
			t.Fatalf("checkpoint %d has no snapshot: %+v", i, cp)
		}
		if cp.Snapshot.Step >= len(advertised) {
			t.Fatalf("checkpoint %d step %d has no matching model request", i, cp.Snapshot.Step)
		}
		if !slices.Equal(cp.Snapshot.Advertised, advertised[cp.Snapshot.Step]) {
			t.Fatalf("checkpoint %d advertised = %v, request advertised %v", i, cp.Snapshot.Advertised, advertised[cp.Snapshot.Step])
		}
		if cp.Snapshot.Model != "normal-model" {
			t.Fatalf("checkpoint %d model = %q, want normal-model", i, cp.Snapshot.Model)
		}
		if cp.Snapshot.ToolDigest == "" || cp.Snapshot.SettingsDigest == "" {
			t.Fatalf("checkpoint %d digests = %q/%q, want both non-empty", i, cp.Snapshot.ToolDigest, cp.Snapshot.SettingsDigest)
		}
	}
}

type stopBeforeView struct{ agent.BaseMiddleware }

func (stopBeforeView) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	return core.Directive{Kind: core.Stop, Reason: "stop before view"}, nil
}

func TestFailCheckpointCarriesSnapshot(t *testing.T) {
	t.Run("after view freeze", func(t *testing.T) {
		ctx := context.Background()
		store := checkpoint.NewMemory()
		a, err := agent.New(
			agent.WithModel(errModel{}),
			agent.WithCheckpointer(store),
			agent.WithModelOptions(llm.WithModel("fail-model")),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("fail_after_view"))
		if _, err := phase1Drain(t, run); err == nil {
			t.Fatal("run error = nil, want model failure")
		}
		cp, err := store.Latest(ctx, "fail_after_view")
		if err != nil {
			t.Fatal(err)
		}
		if cp == nil || cp.Snapshot == nil {
			t.Fatalf("failure checkpoint snapshot = %+v, want non-nil", cp)
		}
		if cp.Snapshot.Model != "fail-model" {
			t.Fatalf("snapshot model = %q, want fail-model", cp.Snapshot.Model)
		}
	})

	t.Run("before view freeze", func(t *testing.T) {
		ctx := context.Background()
		store := checkpoint.NewMemory()
		a, err := agent.New(
			agent.WithModel(mock.New("unused", func(*llm.Request) *llm.Response { return mock.Text("unused") })),
			agent.WithCheckpointer(store),
			agent.WithMiddleware(stopBeforeView{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("fail_before_view"))
		if _, err := phase1Drain(t, run); err != nil {
			t.Fatal(err)
		}
		cp, err := store.Latest(ctx, "fail_before_view")
		if err != nil {
			t.Fatal(err)
		}
		if cp == nil {
			t.Fatal("stop before view wrote no checkpoint")
		}
		if cp.Snapshot != nil {
			t.Fatalf("pre-view checkpoint snapshot = %+v, want nil", cp.Snapshot)
		}
	})
}
