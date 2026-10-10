package agent_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

func phase1Drain(t *testing.T, run *agent.Run) ([]core.Event, error) {
	t.Helper()
	var events []core.Event
	var iterErr error
	for ev, err := range run.Iter() {
		if ev != nil {
			events = append(events, ev)
		}
		if err != nil {
			iterErr = err
		}
	}
	_, waitErr := run.Wait()
	if waitErr != nil {
		return events, waitErr
	}
	return events, iterErr
}

func phase1Interrupted(events []core.Event) (core.Interrupted, bool) {
	for _, ev := range events {
		if paused, ok := ev.(core.Interrupted); ok {
			return paused, true
		}
	}
	return core.Interrupted{}, false
}

func phase1HasTerminal(events []core.Event, want any) bool {
	for _, ev := range events {
		switch want.(type) {
		case core.RunDone:
			if _, ok := ev.(core.RunDone); ok {
				return true
			}
		case core.RunFailed:
			if _, ok := ev.(core.RunFailed); ok {
				return true
			}
		}
	}
	return false
}

func phase1Tool(name string, ran *atomic.Int32) tool.Tool {
	return tool.New(name, name, func(_ *tool.Context, _ struct{}) (string, error) {
		ran.Add(1)
		return name + "-done", nil
	})
}

func phase1Calls(calls ...core.ToolCall) llm.Model {
	return mock.New("phase1", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return &llm.Response{
			Message:    core.Message{Role: core.RoleAssistant, Parts: toolParts(calls)},
			StopReason: llm.StopToolUse,
		}
	})
}

func toolParts(calls []core.ToolCall) []core.Part {
	parts := make([]core.Part, len(calls))
	for i := range calls {
		parts[i] = calls[i]
	}
	return parts
}

type phase1ApprovalGate struct{ agent.BaseMiddleware }

func (phase1ApprovalGate) BeforeTool(lc *agent.LoopContext, call *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(call) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

type phase1InterruptToolGate struct {
	agent.BaseMiddleware
	name string
}

func (g phase1InterruptToolGate) BeforeTool(lc *agent.LoopContext, call *core.ToolCall) (core.Directive, error) {
	if call.Name == g.name && !lc.IsApproved(call) {
		return core.Directive{Kind: core.Interrupt, Reason: "approve " + call.Name}, nil
	}
	return core.Directive{}, nil
}

func TestInterruptKeepsWholePendingBatch(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32

	calls := []core.ToolCall{
		{ID: "a", Name: "a", Args: json.RawMessage(`{}`)},
		{ID: "b", Name: "b", Args: json.RawMessage(`{}`)},
		{ID: "c", Name: "c", Args: json.RawMessage(`{}`)},
	}
	a, err := agent.New(
		agent.WithModel(phase1Calls(calls...)),
		agent.WithTools(phase1Tool("a", &ran), phase1Tool("b", &ran), phase1Tool("c", &ran)),
		agent.WithMiddleware(phase1InterruptToolGate{name: "b"}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread("whole_batch"))
	events, err := phase1Drain(t, run)
	if err != nil {
		t.Fatal(err)
	}
	paused, ok := phase1Interrupted(events)
	if !ok {
		t.Fatal("expected an interrupt")
	}
	if got := len(paused.Pending); got != 3 {
		t.Fatalf("pending calls = %d, want whole batch of 3", got)
	}
	for i, want := range []string{"a", "b", "c"} {
		if got := paused.Pending[i].CallID; got != want {
			t.Fatalf("pending[%d] = %q, want %q", i, got, want)
		}
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("tools ran before pause = %d, want 0", got)
	}
	cp, err := store.Latest(ctx, "whole_batch")
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || cp.Pending == nil || len(cp.Pending.Pending) != 3 {
		t.Fatalf("checkpoint pending = %+v, want all calls", cp)
	}

	for _, call := range paused.Pending {
		run.Decide(agent.Allow(call.CallID))
	}
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := phase1Drain(t, cont); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 3 {
		t.Fatalf("tools ran after approval = %d, want 3", got)
	}
}

func TestAskThenDenyCannotBeApproved(t *testing.T) {
	ctx := context.Background()
	var ran atomic.Int32
	calls := []core.ToolCall{
		{ID: "ask", Name: "ask", Args: json.RawMessage(`{}`)},
		{ID: "deny", Name: "deny", Args: json.RawMessage(`{}`)},
	}
	policy := middleware.Permission(
		middleware.RequireApprovalFor("ask"),
		middleware.DenyFor("deny"),
	)

	t.Run("new batch rejects before any handler", func(t *testing.T) {
		a, err := agent.New(
			agent.WithModel(phase1Calls(calls...)),
			agent.WithTools(phase1Tool("ask", &ran), phase1Tool("deny", &ran)),
			agent.WithMiddleware(policy),
		)
		if err != nil {
			t.Fatal(err)
		}
		_, err = phase1Drain(t, a.Stream(ctx, "go", agent.OnThread("ask_deny_new")))
		if err == nil || !strings.Contains(err.Error(), "denied") {
			t.Fatalf("run error = %v, want policy denial", err)
		}
		if got := ran.Load(); got != 0 {
			t.Fatalf("tools ran = %d, want 0", got)
		}
	})

	t.Run("old pending batch is revalidated", func(t *testing.T) {
		store := checkpoint.NewMemory()
		state := core.State{Messages: []core.Message{
			core.UserText("go"),
			{Role: core.RoleAssistant, Parts: toolParts(calls)},
		}}
		if err := store.Save(ctx, &checkpoint.Checkpoint{
			ID:       "seed",
			ThreadID: "ask_deny_resume",
			State:    state,
			Pending:  &checkpoint.PendingHITL{Step: 0, Pending: calls},
			Pause:    &checkpoint.Pause{Phase: "before_tool", Recovery: checkpoint.RecoveryResumeTools},
		}); err != nil {
			t.Fatal(err)
		}
		a, err := agent.New(
			agent.WithModel(phase1Calls(calls...)),
			agent.WithTools(phase1Tool("ask", &ran), phase1Tool("deny", &ran)),
			agent.WithMiddleware(policy),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}
		cont, err := a.Resume(ctx, "ask_deny_resume", agent.Allow("deny"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = phase1Drain(t, cont)
		if err == nil || !strings.Contains(err.Error(), "denied") {
			t.Fatalf("resume error = %v, want policy denial", err)
		}
		if got := ran.Load(); got != 0 {
			t.Fatalf("tools ran = %d, want 0", got)
		}
	})
}

func TestDenyWinsOverAskAcrossRulesAndStack(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		mws  []agent.Middleware
	}{
		{
			name: "ask then deny in one permission",
			mws: []agent.Middleware{middleware.Permission(
				middleware.RequireApprovalFor("danger"),
				middleware.DenyFor("danger"),
			)},
		},
		{
			name: "deny then ask in one permission",
			mws: []agent.Middleware{middleware.Permission(
				middleware.DenyFor("danger"),
				middleware.RequireApprovalFor("danger"),
			)},
		},
		{
			name: "ask middleware before deny middleware",
			mws: []agent.Middleware{
				middleware.Permission(middleware.RequireApprovalFor("danger")),
				middleware.Permission(middleware.DenyFor("danger")),
			},
		},
		{
			name: "deny middleware before ask middleware",
			mws: []agent.Middleware{
				middleware.Permission(middleware.DenyFor("danger")),
				middleware.Permission(middleware.RequireApprovalFor("danger")),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ran atomic.Int32
			a, err := agent.New(
				agent.WithModel(phase1Calls(core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)})),
				agent.WithTools(phase1Tool("danger", &ran)),
				agent.WithMiddleware(tc.mws...),
			)
			if err != nil {
				t.Fatal(err)
			}
			events, err := phase1Drain(t, a.Stream(ctx, "go"))
			if err == nil || !strings.Contains(err.Error(), "denied") {
				t.Fatalf("run error = %v, want policy denial", err)
			}
			if _, paused := phase1Interrupted(events); paused {
				t.Fatal("hard denial was masked by an interrupt")
			}
			if got := ran.Load(); got != 0 {
				t.Fatalf("tool ran = %d, want 0", got)
			}
		})
	}
}

func TestResumeRevalidatesChangedPolicy(t *testing.T) {
	ctx := context.Background()
	var deny atomic.Bool
	var ran atomic.Int32
	policy := middleware.Permission(func(call *core.ToolCall) middleware.Decision {
		if call.Name != "danger" {
			return middleware.AllowTool
		}
		if deny.Load() {
			return middleware.DenyTool
		}
		return middleware.AskTool
	})
	a, err := agent.New(
		agent.WithModel(phase1Calls(core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)})),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(policy),
		agent.WithCheckpointer(checkpoint.NewMemory()),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread("policy_change"))
	if _, err := phase1Drain(t, run); err != nil {
		t.Fatal(err)
	}
	deny.Store(true)
	run.Decide(agent.Allow("c1"))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = phase1Drain(t, cont)
	if err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("resume error = %v, want policy denial", err)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("tool ran = %d, want 0", got)
	}
}

type phase1AlwaysInterrupt struct{ agent.BaseMiddleware }

func (phase1AlwaysInterrupt) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	return core.Directive{Kind: core.Interrupt, Reason: "custom gate"}, nil
}

func TestResumeApprovalIsCallScoped(t *testing.T) {
	ctx := context.Background()

	t.Run("unknown decision does not approve a call", func(t *testing.T) {
		var ran atomic.Int32
		a, err := agent.New(
			agent.WithModel(phase1Calls(core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)})),
			agent.WithTools(phase1Tool("danger", &ran)),
			agent.WithMiddleware(phase1ApprovalGate{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("unknown_decision"))
		if _, err := phase1Drain(t, run); err != nil {
			t.Fatal(err)
		}
		run.Decide(agent.Allow("not-a-call"))
		cont, err := run.Resume(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := phase1Drain(t, cont); err != nil {
			t.Fatal(err)
		}
		if got := ran.Load(); got != 0 {
			t.Fatalf("tool ran = %d, want 0", got)
		}
	})

	t.Run("duplicate call IDs are not approvable", func(t *testing.T) {
		var ran atomic.Int32
		calls := []core.ToolCall{
			{ID: "duplicate", Name: "danger", Args: json.RawMessage(`{"n":1}`)},
			{ID: "duplicate", Name: "danger", Args: json.RawMessage(`{"n":2}`)},
		}
		a, err := agent.New(
			agent.WithModel(phase1Calls(calls...)),
			agent.WithTools(phase1Tool("danger", &ran)),
			agent.WithMiddleware(phase1ApprovalGate{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("duplicate_decision"))
		if _, err := phase1Drain(t, run); err != nil {
			t.Fatal(err)
		}
		run.Decide(agent.Allow("duplicate"))
		cont, err := run.Resume(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := phase1Drain(t, cont); err != nil {
			t.Fatal(err)
		}
		if got := ran.Load(); got != 0 {
			t.Fatalf("tool ran = %d, want 0", got)
		}
	})

	t.Run("custom gate must opt in to exact approval", func(t *testing.T) {
		var ran atomic.Int32
		a, err := agent.New(
			agent.WithModel(phase1Calls(core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)})),
			agent.WithTools(phase1Tool("danger", &ran)),
			agent.WithMiddleware(phase1AlwaysInterrupt{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("custom_gate"))
		if _, err := phase1Drain(t, run); err != nil {
			t.Fatal(err)
		}
		run.Decide(agent.Allow("c1"))
		cont, err := run.Resume(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events, err := phase1Drain(t, cont)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := phase1Interrupted(events); !ok {
			t.Fatal("custom gate did not interrupt the resumed batch")
		}
		if got := ran.Load(); got != 0 {
			t.Fatalf("tool ran = %d, want 0", got)
		}
	})
}

type phase1RewriteGate struct{ agent.BaseMiddleware }

func (phase1RewriteGate) BeforeTool(lc *agent.LoopContext, call *core.ToolCall) (core.Directive, error) {
	if string(call.Args) == `{"original":true}` {
		call.Args = json.RawMessage(`{"approved":true}`)
	}
	if lc.IsApproved(call) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "approve rewritten arguments"}, nil
}

type phase1ChangingGate struct{ agent.BaseMiddleware }

func (phase1ChangingGate) BeforeTool(lc *agent.LoopContext, call *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(call) {
		call.Args = json.RawMessage(`{"version":2}`)
		if lc.IsApproved(call) {
			return core.Directive{}, nil
		}
	}
	return core.Directive{Kind: core.Interrupt, Reason: "approval arguments changed"}, nil
}

func TestApprovalUsesExecutedArguments(t *testing.T) {
	ctx := context.Background()
	t.Run("approval records arguments that execute", func(t *testing.T) {
		var executed atomic.Bool
		danger := tool.New("danger", "danger", func(_ *tool.Context, in struct {
			Approved bool `json:"approved"`
		}) (string, error) {
			executed.Store(in.Approved)
			return "ran", nil
		})
		a, err := agent.New(
			agent.WithModel(phase1Calls(core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{"original":true}`)})),
			agent.WithTools(danger),
			agent.WithMiddleware(phase1RewriteGate{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("rewritten_args"))
		events, err := phase1Drain(t, run)
		if err != nil {
			t.Fatal(err)
		}
		paused, ok := phase1Interrupted(events)
		if !ok || len(paused.Pending) != 1 || string(paused.Pending[0].Args) != `{"approved":true}` {
			t.Fatalf("pending = %+v, want rewritten arguments", paused)
		}
		run.Decide(agent.Allow("c1"))
		cont, err := run.Resume(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := phase1Drain(t, cont); err != nil {
			t.Fatal(err)
		}
		if !executed.Load() {
			t.Fatal("tool did not receive the approved, persisted arguments")
		}
	})

	t.Run("argument mutation requires another approval", func(t *testing.T) {
		var ran atomic.Int32
		a, err := agent.New(
			agent.WithModel(phase1Calls(core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{"version":1}`)})),
			agent.WithTools(phase1Tool("danger", &ran)),
			agent.WithMiddleware(phase1ChangingGate{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(ctx, "go", agent.OnThread("changed_args"))
		if _, err := phase1Drain(t, run); err != nil {
			t.Fatal(err)
		}
		run.Decide(agent.Allow("c1"))
		cont, err := run.Resume(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events, err := phase1Drain(t, cont)
		if err != nil {
			t.Fatal(err)
		}
		paused, ok := phase1Interrupted(events)
		if !ok || len(paused.Pending) != 1 || string(paused.Pending[0].Args) != `{"version":2}` {
			t.Fatalf("second pending = %+v, want changed arguments", paused)
		}
		if got := ran.Load(); got != 0 {
			t.Fatalf("tool ran = %d, want 0", got)
		}
	})
}

type phase1ModelInterrupt struct {
	agent.BaseMiddleware
	phase string
	once  atomic.Bool
}

func (m *phase1ModelInterrupt) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	if m.phase == "before_model" && m.once.CompareAndSwap(false, true) {
		return core.Directive{Kind: core.Interrupt, Reason: "pause before model"}, nil
	}
	return core.Directive{}, nil
}

func (m *phase1ModelInterrupt) AfterModel(*agent.LoopContext, *llm.Response) (core.Directive, error) {
	if m.phase == "after_model" && m.once.CompareAndSwap(false, true) {
		return core.Directive{Kind: core.Interrupt, Reason: "pause after model"}, nil
	}
	return core.Directive{}, nil
}

func TestModelPhaseInterruptReplaysSafely(t *testing.T) {
	ctx := context.Background()
	for _, phase := range []string{"before_model", "after_model"} {
		t.Run(phase, func(t *testing.T) {
			store := checkpoint.NewMemory()
			var toolRuns atomic.Int32
			var modelRuns atomic.Int32
			model := mock.New("phase", func(req *llm.Request) *llm.Response {
				modelRuns.Add(1)
				if _, ok := mock.LastToolResult(req); ok {
					return mock.Text("done")
				}
				return mock.CallTool("c1", "danger", `{}`)
			})
			gate := &phase1ModelInterrupt{phase: phase}
			a, err := agent.New(
				agent.WithModel(model),
				agent.WithTools(phase1Tool("danger", &toolRuns)),
				agent.WithMiddleware(gate),
				agent.WithCheckpointer(store),
			)
			if err != nil {
				t.Fatal(err)
			}

			run := a.Stream(ctx, "go", agent.OnThread("phase_"+phase))
			run.Steer(core.UserText("steer"))
			events, err := phase1Drain(t, run)
			if err != nil {
				t.Fatal(err)
			}
			paused, ok := phase1Interrupted(events)
			if !ok {
				t.Fatal("expected interrupt")
			}
			if paused.Phase != phase || paused.Recovery != checkpoint.RecoveryReplayModel {
				t.Fatalf("pause = %+v, want replay metadata for %s", paused, phase)
			}
			if len(paused.Pending) != 0 {
				t.Fatalf("model-phase pause has pending calls: %+v", paused.Pending)
			}
			if got := toolRuns.Load(); got != 0 {
				t.Fatalf("tool ran before resume = %d, want 0", got)
			}
			cp, err := store.Latest(ctx, "phase_"+phase)
			if err != nil {
				t.Fatal(err)
			}
			if cp == nil || cp.Pause == nil || cp.Pause.Phase != phase || cp.Pause.Recovery != checkpoint.RecoveryReplayModel {
				t.Fatalf("checkpoint pause = %+v", cp)
			}
			if phase == "after_model" && len(cp.State.Messages) != 2 {
				t.Fatalf("after-model checkpoint persisted an uncommitted reply: %s", digest(cp.State.Messages))
			}

			cont, err := run.Resume(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := phase1Drain(t, cont); err != nil {
				t.Fatal(err)
			}
			if got := toolRuns.Load(); got != 1 {
				t.Fatalf("tool runs after resume = %d, want 1", got)
			}
			wantModels := int32(2)
			if phase == "after_model" {
				wantModels = 3
			}
			if got := modelRuns.Load(); got != wantModels {
				t.Fatalf("model calls = %d, want %d", got, wantModels)
			}
		})
	}
}

type phase1AfterToolInterrupt struct {
	agent.BaseMiddleware
	once atomic.Bool
}

func (m *phase1AfterToolInterrupt) AfterTool(*agent.LoopContext, *core.ToolResult) (core.Directive, error) {
	if m.once.CompareAndSwap(false, true) {
		return core.Directive{Kind: core.Interrupt, Reason: "review completed batch"}, nil
	}
	return core.Directive{}, nil
}

func TestAfterToolsInterruptResumesAfterBatch(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	calls := []core.ToolCall{
		{ID: "a", Name: "a", Args: json.RawMessage(`{}`)},
		{ID: "b", Name: "b", Args: json.RawMessage(`{}`)},
	}
	a, err := agent.New(
		agent.WithModel(phase1Calls(calls...)),
		agent.WithTools(phase1Tool("a", &ran), phase1Tool("b", &ran)),
		agent.WithMiddleware(&phase1AfterToolInterrupt{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(ctx, "go", agent.OnThread("after_tools"))
	events, err := phase1Drain(t, run)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := phase1Interrupted(events); !ok {
		t.Fatal("expected batch-level interrupt")
	}
	if got := ran.Load(); got != 2 {
		t.Fatalf("tools ran = %d, want complete batch of 2", got)
	}
	cp, err := store.Latest(ctx, "after_tools")
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || len(cp.State.Messages) == 0 || cp.State.Messages[len(cp.State.Messages)-1].Role != core.RoleTool {
		t.Fatalf("checkpoint did not retain completed tool batch: %+v", cp)
	}

	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := phase1Drain(t, cont); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 2 {
		t.Fatalf("tools reran after batch interrupt = %d, want 2", got)
	}
}

func TestPauseMetadataCompatibility(t *testing.T) {
	ctx := context.Background()
	t.Run("unknown recovery fails before executing", func(t *testing.T) {
		store := checkpoint.NewMemory()
		var ran atomic.Int32
		call := core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)}
		if err := store.Save(ctx, &checkpoint.Checkpoint{
			ID:       "unknown",
			ThreadID: "unknown_recovery",
			State: core.State{Messages: []core.Message{
				core.UserText("go"),
				{Role: core.RoleAssistant, Parts: toolParts([]core.ToolCall{call})},
			}},
			Pending: &checkpoint.PendingHITL{Step: 0, Pending: []core.ToolCall{call}},
			Pause:   &checkpoint.Pause{Phase: "before_tool", Recovery: "unknown"},
		}); err != nil {
			t.Fatal(err)
		}
		a, err := agent.New(
			agent.WithModel(phase1Calls(call)),
			agent.WithTools(phase1Tool("danger", &ran)),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Resume(ctx, "unknown_recovery", agent.Allow("c1")); err == nil || !strings.Contains(err.Error(), "unsupported") {
			t.Fatalf("resume error = %v, want unknown-recovery error", err)
		}
		if got := ran.Load(); got != 0 {
			t.Fatalf("tool ran = %d, want 0", got)
		}
	})

	t.Run("legacy pending checkpoint remains resumable", func(t *testing.T) {
		store := checkpoint.NewMemory()
		var ran atomic.Int32
		call := core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)}
		if err := store.Save(ctx, &checkpoint.Checkpoint{
			ID:       "legacy",
			ThreadID: "legacy_pending",
			State: core.State{Messages: []core.Message{
				core.UserText("go"),
				{Role: core.RoleAssistant, Parts: toolParts([]core.ToolCall{call})},
			}},
			Pending: &checkpoint.PendingHITL{Step: 0, Pending: []core.ToolCall{call}},
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
		cont, err := a.Resume(ctx, "legacy_pending", agent.Allow("c1"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := phase1Drain(t, cont); err != nil {
			t.Fatal(err)
		}
		if got := ran.Load(); got != 1 {
			t.Fatalf("tool ran = %d, want 1", got)
		}
	})
}

func TestResumedSteeringDeliveryOrder(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var ran atomic.Int32
	var mu sync.Mutex
	var requests [][]core.Message
	model := mock.New("order", func(req *llm.Request) *llm.Response {
		mu.Lock()
		requests = append(requests, append([]core.Message(nil), req.Messages...))
		mu.Unlock()
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "danger", `{}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(phase1ApprovalGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(ctx, "go", agent.OnThread("steering_resume"))
	if _, err := phase1Drain(t, run); err != nil {
		t.Fatal(err)
	}
	// This queue belongs to the completed, paused Run and must not migrate into
	// the fresh Run created by Resume.
	run.Steer(core.UserText("old run message"))
	run.Decide(agent.Allow("c1"))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cont.Steer(core.UserText("new run message"))
	if _, err := phase1Drain(t, cont); err != nil {
		t.Fatal(err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("tool ran = %d, want 1 before resumed model request", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("model requests = %d, want 2", len(requests))
	}
	history := requests[1]
	if got := history[len(history)-1]; got.Role != core.RoleUser || got.Text() != "new run message" {
		t.Fatalf("last resumed message = %+v, want new-run steering", got)
	}
	if history[len(history)-2].Role != core.RoleTool {
		t.Fatalf("tool result did not precede steering: %s", digest(history))
	}
	for _, message := range history {
		if message.Text() == "old run message" {
			t.Fatalf("old paused Run steering leaked into resumed request: %s", digest(history))
		}
	}
}
