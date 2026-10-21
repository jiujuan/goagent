package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func pausedSnapshotCheckpoint(threadID, id string, snapshot *checkpoint.StepSnapshot, calls ...core.ToolCall) *checkpoint.Checkpoint {
	parts := make([]core.Part, len(calls))
	for i := range calls {
		parts[i] = calls[i]
	}
	return &checkpoint.Checkpoint{
		ID:       id,
		ThreadID: threadID,
		State: core.State{Messages: []core.Message{
			core.UserText("go"),
			{Role: core.RoleAssistant, Parts: parts},
		}},
		Pending:  &checkpoint.PendingHITL{Step: 0, Pending: append([]core.ToolCall(nil), calls...)},
		Pause:    &checkpoint.Pause{ID: "pause-" + id, Phase: "before_tool", Recovery: checkpoint.RecoveryResumeTools},
		Snapshot: snapshot,
	}
}

func saveTestCheckpoint(t *testing.T, store checkpoint.Checkpointer, cp *checkpoint.Checkpoint) {
	t.Helper()
	if err := store.Save(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
}

func textModel(name string) llm.Model {
	return mock.New(name, func(*llm.Request) *llm.Response { return mock.Text("done") })
}

func TestResumeRejectsToolNotInPausedView(t *testing.T) {
	call := core.ToolCall{ID: "c1", Name: "drop", Args: json.RawMessage(`{}`)}
	snapshot := &checkpoint.StepSnapshot{
		Step:       0,
		Advertised: []string{"keep"},
		ToolDigest: "tool-digest", SettingsDigest: "settings-digest", Model: "paused-model",
	}

	t.Run("registered but not advertised", func(t *testing.T) {
		store := checkpoint.NewMemory()
		saveTestCheckpoint(t, store, pausedSnapshotCheckpoint("not_advertised", "cp1", snapshot, call))
		var keepRan, dropRan atomic.Int32
		a, err := agent.New(
			agent.WithModel(textModel("unused")),
			agent.WithTools(phase1Tool("keep", &keepRan), phase1Tool("drop", &dropRan)),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = a.Resume(context.Background(), "not_advertised", agent.Allow("c1"))
		if !errors.Is(err, agent.ErrToolNotAdvertised) {
			t.Fatalf("resume error = %v, want ErrToolNotAdvertised", err)
		}
		for _, want := range []string{"not_advertised", "c1", "drop"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not mention %q", err, want)
			}
		}
		if got := keepRan.Load(); got != 0 {
			t.Fatalf("keep ran = %d, want 0", got)
		}
		if got := dropRan.Load(); got != 0 {
			t.Fatalf("drop ran = %d, want 0", got)
		}
	})

	t.Run("advertised check runs before availability", func(t *testing.T) {
		store := checkpoint.NewMemory()
		saveTestCheckpoint(t, store, pausedSnapshotCheckpoint("not_advertised_missing", "cp2", snapshot, call))
		a, err := agent.New(
			agent.WithModel(textModel("unused")),
			agent.WithTools(phase1Tool("keep", new(atomic.Int32))),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = a.Resume(context.Background(), "not_advertised_missing", agent.Allow("c1"))
		if !errors.Is(err, agent.ErrToolNotAdvertised) {
			t.Fatalf("resume error = %v, want ErrToolNotAdvertised before ErrToolUnavailable", err)
		}
	})
}

func TestResumeRejectsRemovedTool(t *testing.T) {
	store := checkpoint.NewMemory()
	call := core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)}
	snapshot := &checkpoint.StepSnapshot{Step: 0, Advertised: []string{"danger"}, ToolDigest: "tool-digest"}
	saveTestCheckpoint(t, store, pausedSnapshotCheckpoint("removed_tool", "cp1", snapshot, call))
	a, err := agent.New(
		agent.WithModel(textModel("unused")),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = a.Resume(context.Background(), "removed_tool", agent.Allow("c1"))
	if !errors.Is(err, agent.ErrToolUnavailable) {
		t.Fatalf("resume error = %v, want ErrToolUnavailable", err)
	}
	if errors.Is(err, agent.ErrToolNotAdvertised) {
		t.Fatalf("resume error = %v, advertised check should have passed", err)
	}
}

func TestResumeRejectsDynamicTool(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	call := core.ToolCall{ID: "c1", Name: "dynamic", Args: json.RawMessage(`{}`)}
	dynamic := tool.New("dynamic", "runtime tool", func(_ *tool.Context, _ struct{}) (string, error) {
		ran.Add(1)
		return "dynamic-done", nil
	})
	a, err := agent.New(
		agent.WithModel(phase1Calls(call)),
		agent.WithMiddleware(&mapToolInjector{tools: map[string]tool.Tool{"dynamic": dynamic}}),
		agent.WithMiddleware(phase1ApprovalGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread("dynamic_resume"))
	events, err := phase1Drain(t, run)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := phase1Interrupted(events); !ok {
		t.Fatal("expected a pause on the injected tool")
	}
	cp, err := store.Latest(ctx, "dynamic_resume")
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || cp.Snapshot == nil || !slices.Contains(cp.Snapshot.Advertised, "dynamic") {
		t.Fatalf("paused snapshot = %+v, want dynamic advertised", cp)
	}

	_, err = a.Resume(ctx, "dynamic_resume", agent.Allow("c1"))
	if !errors.Is(err, agent.ErrToolUnavailable) {
		t.Fatalf("resume error = %v, want ErrToolUnavailable", err)
	}
	if !strings.Contains(err.Error(), "runtime-injected") {
		t.Fatalf("error %q does not explain that runtime-injected tools do not survive resume", err)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("dynamic tool ran = %d, want 0", got)
	}
}

type claimCountingStore struct {
	*checkpoint.Memory
	claims atomic.Int32
}

func (s *claimCountingStore) Claim(ctx context.Context, req checkpoint.ClaimRequest) (checkpoint.Claim, error) {
	s.claims.Add(1)
	return s.Memory.Claim(ctx, req)
}

func TestResumePreflightBeforeClaim(t *testing.T) {
	snapshot := &checkpoint.StepSnapshot{Step: 0, Advertised: []string{"keep"}, ToolDigest: "tool-digest"}

	t.Run("preflight failure takes no claim", func(t *testing.T) {
		store := &claimCountingStore{Memory: checkpoint.NewMemory()}
		call := core.ToolCall{ID: "c1", Name: "drop", Args: json.RawMessage(`{}`)}
		saveTestCheckpoint(t, store, pausedSnapshotCheckpoint("preflight_fail", "cp1", snapshot, call))
		a, err := agent.New(
			agent.WithModel(textModel("unused")),
			agent.WithTools(phase1Tool("keep", new(atomic.Int32)), phase1Tool("drop", new(atomic.Int32))),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := a.Resume(context.Background(), "preflight_fail", agent.Allow("c1")); !errors.Is(err, agent.ErrToolNotAdvertised) {
			t.Fatalf("resume error = %v, want ErrToolNotAdvertised", err)
		}
		if got := store.claims.Load(); got != 0 {
			t.Fatalf("claims = %d, want 0 before a failed preflight", got)
		}
	})

	t.Run("successful preflight takes one claim", func(t *testing.T) {
		store := &claimCountingStore{Memory: checkpoint.NewMemory()}
		call := core.ToolCall{ID: "c1", Name: "keep", Args: json.RawMessage(`{}`)}
		saveTestCheckpoint(t, store, pausedSnapshotCheckpoint("preflight_ok", "cp2", snapshot, call))
		a, err := agent.New(
			agent.WithModel(textModel("unused")),
			agent.WithTools(phase1Tool("keep", new(atomic.Int32))),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := a.Resume(context.Background(), "preflight_ok", agent.Allow("c1")); err != nil {
			t.Fatal(err)
		}
		if got := store.claims.Load(); got != 1 {
			t.Fatalf("claims = %d, want 1", got)
		}
	})
}

func capturedResumeView(t *testing.T, spy *resumeViewSpy) (*agent.StepView, *llm.Request) {
	t.Helper()
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if !spy.seen {
		t.Fatal("BeforeTool did not record an approved resumed call")
	}
	return spy.view, spy.request
}

func TestResumeBatchUsesPausedView(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	snapshot := &checkpoint.StepSnapshot{
		Step:           0,
		Advertised:     []string{"keep", "other"},
		ToolDigest:     "tool-digest",
		SettingsDigest: "settings-digest",
		Model:          "paused-model",
	}
	call := core.ToolCall{ID: "c1", Name: "keep", Args: json.RawMessage(`{}`)}
	paused := pausedSnapshotCheckpoint("paused_view", "cp1", snapshot, call)
	saveTestCheckpoint(t, store, paused)

	var ran atomic.Int32
	spy := &resumeViewSpy{}
	a, err := agent.New(
		agent.WithModel(textModel("resume")),
		agent.WithTools(phase1Tool("keep", &ran), phase1Tool("other", new(atomic.Int32))),
		agent.WithMiddleware(spy),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	cont, err := a.Resume(ctx, "paused_view", agent.Allow("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := phase1Drain(t, cont); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("keep ran = %d, want 1", got)
	}

	view, req := capturedResumeView(t, spy)
	if view == nil {
		t.Fatal("resumed batch ran without a step view")
	}
	if req != nil {
		t.Fatalf("resumed batch Request = %+v, want nil", req)
	}
	if got := view.Tools().Names(); !slices.Equal(got, snapshot.Advertised) {
		t.Fatalf("view names = %v, want paused snapshot names %v", got, snapshot.Advertised)
	}
	if got := view.Digest().Advertised; !slices.Equal(got, snapshot.Advertised) {
		t.Fatalf("digest advertised = %v, want %v", got, snapshot.Advertised)
	}
	digest := view.Digest()
	if digest.ToolDigest != snapshot.ToolDigest || digest.SettingsDigest != snapshot.SettingsDigest {
		t.Fatalf("digest = %+v, want paused snapshot digests", digest)
	}
	if got := view.Model(); got != snapshot.Model {
		t.Fatalf("view model = %q, want %q", got, snapshot.Model)
	}

	history, err := store.History(ctx, "paused_view")
	if err != nil {
		t.Fatal(err)
	}
	var preserved *checkpoint.Checkpoint
	for _, cp := range history {
		if cp.Revision > paused.Revision && cp.Step == 0 && cp.Pending == nil && cp.Snapshot != nil {
			preserved = cp
			break
		}
	}
	if preserved == nil {
		t.Fatalf("resumed batch wrote no step-0 checkpoint with a snapshot; history=%+v", history)
	}
	if !reflect.DeepEqual(preserved.Snapshot, snapshot) {
		t.Fatalf("resumed checkpoint snapshot = %+v, want %+v", preserved.Snapshot, snapshot)
	}
}

type resumeBlockingGate struct{ agent.BaseMiddleware }

func (resumeBlockingGate) BeforeTool(lc *agent.LoopContext, call *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(call) {
		return core.Directive{Kind: core.Interrupt, Reason: "reapproval required"}, nil
	}
	return core.Directive{}, nil
}

func TestResumeReinterruptPreservesSnapshot(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	snapshot := &checkpoint.StepSnapshot{
		Step:           0,
		Advertised:     []string{"danger"},
		ToolDigest:     "tool-digest",
		SettingsDigest: "settings-digest",
		Model:          "paused-model",
	}
	call := core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)}
	saveTestCheckpoint(t, store, pausedSnapshotCheckpoint("reinterrupt", "cp1", snapshot, call))

	var ran atomic.Int32
	first, err := agent.New(
		agent.WithModel(textModel("first")),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(resumeBlockingGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	cont, err := first.Resume(ctx, "reinterrupt", agent.Allow("c1"))
	if err != nil {
		t.Fatal(err)
	}
	events, err := phase1Drain(t, cont)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := phase1Interrupted(events); !ok {
		t.Fatal("expected the second pause")
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("tool ran before reapproval = %d, want 0", got)
	}
	repaused, err := store.Latest(ctx, "reinterrupt")
	if err != nil {
		t.Fatal(err)
	}
	if repaused == nil || repaused.Pending == nil {
		t.Fatalf("latest checkpoint = %+v, want a pending pause", repaused)
	}
	if !reflect.DeepEqual(repaused.Snapshot, snapshot) {
		t.Fatalf("repaused snapshot = %+v, want original %+v", repaused.Snapshot, snapshot)
	}

	second, err := agent.New(
		agent.WithModel(textModel("second")),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(phase1ApprovalGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	cont, err = second.Resume(ctx, "reinterrupt", agent.Allow("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := phase1Drain(t, cont); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("tool ran after second resume = %d, want 1", got)
	}
}

func TestLegacyPendingCheckpointView(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	call := core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)}
	legacy := pausedSnapshotCheckpoint("legacy_view", "cp1", nil, call)
	legacy.Pause = nil
	saveTestCheckpoint(t, store, legacy)

	var ran atomic.Int32
	spy := &resumeViewSpy{}
	a, err := agent.New(
		agent.WithModel(textModel("legacy")),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(spy),
		agent.WithMiddleware(phase1ApprovalGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	cont, err := a.Resume(ctx, "legacy_view", agent.Allow("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := phase1Drain(t, cont); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("tool ran = %d, want 1", got)
	}

	view, req := capturedResumeView(t, spy)
	if view == nil {
		t.Fatal("legacy resumed batch has no view")
	}
	if !slices.Contains(view.Tools().Names(), "danger") {
		t.Fatalf("view names = %v, want danger", view.Tools().Names())
	}
	if got := view.Digest(); !reflect.DeepEqual(got, agent.StepDigest{}) {
		t.Fatalf("legacy view digest = %+v, want empty", got)
	}
	if req != nil {
		t.Fatalf("legacy resumed batch Request = %+v, want nil", req)
	}
}

func TestWorkflowResumeSkipsPreflight(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	plan := agent.NewPlan("workflow",
		agent.Plan{Nodes: []agent.Node{{ID: "main", Task: "hello"}}},
		agent.WithWorker(echoWorker(t)),
		agent.WithPlanCheckpointer(store),
	)
	call := core.ToolCall{ID: "ghost", Name: "ghost", Args: json.RawMessage(`{}`)}
	saveTestCheckpoint(t, store, pausedSnapshotCheckpoint("workflow_resume", "cp1", nil, call))

	cont, err := plan.Resume(ctx, "workflow_resume", agent.Allow("ghost"))
	if err != nil {
		t.Fatalf("workflow resume hit tool preflight: %v", err)
	}
	if _, err := phase1Drain(t, cont); err != nil {
		t.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Message.Text(); got != "hello" {
		t.Fatalf("plan result = %q, want hello", got)
	}
	latest, err := store.Latest(ctx, "workflow_resume")
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil {
		t.Fatal("workflow resume wrote no checkpoint")
	}
	found := false
	for _, msg := range latest.State.Messages {
		for _, part := range msg.Parts {
			tr, ok := part.(core.ToolResult)
			if !ok {
				continue
			}
			for _, content := range tr.Content {
				if txt, ok := content.(core.Text); ok && strings.Contains(txt.Text, "not executed: the thread was resumed by a workflow agent") {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("workflow resume did not report the pending call as unexecuted: %+v", latest.State.Messages)
	}
}
