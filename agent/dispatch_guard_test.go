package agent

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func TestSnapshotConsistencyDispatchAfterLeaseLoss(t *testing.T) {
	t.Run("between sequential tools", func(t *testing.T) {
		leaseErr := errors.New("lease renewal failed")
		capture := &dispatchContextCapture{}
		var firstCalls atomic.Int32
		var secondCalls atomic.Int32

		first := tool.New("first", "first tool", func(*tool.Context, struct{}) (string, error) {
			firstCalls.Add(1)
			capture.fail(leaseErr)
			return "first done", nil
		})
		second := tool.New("second", "second tool", func(*tool.Context, struct{}) (string, error) {
			secondCalls.Add(1)
			return "second done", nil
		})
		model := &dispatchIgnoringModel{respond: func(*llm.Request) *llm.Response {
			return dispatchToolResponse(
				core.ToolCall{ID: "call-1", Name: "first", Args: []byte(`{}`)},
				core.ToolCall{ID: "call-2", Name: "second", Args: []byte(`{}`)},
			)
		}}
		a, err := New(
			WithModel(model),
			WithTools(first, second),
			WithMiddleware(capture),
			WithToolExecution(ToolSequential),
			WithCheckpointer(checkpoint.NewMemory()),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = a.Stream(context.Background(), "run", OnThread("dispatch-lease-loss")).Wait()
		if !errors.Is(err, leaseErr) {
			t.Fatalf("run error = %v, want lease renewal failure", err)
		}
		if got := firstCalls.Load(); got != 1 {
			t.Fatalf("first tool calls = %d, want 1", got)
		}
		if got := secondCalls.Load(); got != 0 {
			t.Fatalf("second tool calls = %d, want 0 after lease loss", got)
		}
	})

	t.Run("expired after model", func(t *testing.T) {
		var toolCalls atomic.Int32
		expire := &dispatchHooks{afterModel: func(lc *LoopContext) {
			guard := dispatchGuard(lc.RunContext)
			guard.mu.Lock()
			guard.claim.ExpiresAt = time.Now().Add(-time.Second)
			guard.mu.Unlock()
		}}
		model := &dispatchIgnoringModel{respond: func(*llm.Request) *llm.Response {
			return mock.CallTool("call-1", "danger", `{}`)
		}}
		danger := tool.New("danger", "dangerous tool", func(*tool.Context, struct{}) (string, error) {
			toolCalls.Add(1)
			return "done", nil
		})
		a, err := New(
			WithModel(model),
			WithTools(danger),
			WithMiddleware(expire),
			WithCheckpointer(checkpoint.NewMemory()),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = a.Stream(context.Background(), "run", OnThread("dispatch-expired")).Wait()
		if !errors.Is(err, checkpoint.ErrStaleFencingToken) {
			t.Fatalf("run error = %v, want ErrStaleFencingToken", err)
		}
		if got := toolCalls.Load(); got != 0 {
			t.Fatalf("tool calls = %d, want 0 after lease expiry", got)
		}
	})

	t.Run("parallel after claim", func(t *testing.T) {
		leaseErr := errors.New("parallel lease lost")
		capture := &dispatchContextCapture{}
		base := checkpoint.NewMemory()
		secondClaimed := make(chan struct{})
		firstDone := make(chan struct{})
		store := &dispatchBlockingClaimStore{
			ThreadCoordinator: base,
			beforeReturn: func(req checkpoint.ToolClaimRequest) {
				if req.CallID == "call-2" {
					close(secondClaimed)
					<-firstDone
				}
			},
		}
		var firstCalls atomic.Int32
		var secondCalls atomic.Int32
		var afterSecond atomic.Int32
		first := tool.New("first", "first tool", func(*tool.Context, struct{}) (string, error) {
			firstCalls.Add(1)
			<-secondClaimed
			capture.fail(leaseErr)
			close(firstDone)
			return "first done", nil
		})
		second := tool.New("second", "second tool", func(*tool.Context, struct{}) (string, error) {
			secondCalls.Add(1)
			return "second done", nil
		})
		after := &dispatchHooks{afterToolResult: func(_ *LoopContext, result *core.ToolResult) {
			if result.CallID == "call-2" {
				afterSecond.Add(1)
			}
		}}
		model := &dispatchIgnoringModel{respond: func(*llm.Request) *llm.Response {
			return dispatchToolResponse(
				core.ToolCall{ID: "call-1", Name: "first", Args: []byte(`{}`)},
				core.ToolCall{ID: "call-2", Name: "second", Args: []byte(`{}`)},
			)
		}}
		a, err := New(
			WithModel(model),
			WithTools(first, second),
			WithMiddleware(capture, after),
			WithToolExecution(ToolParallel),
			WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}

		var secondDone int
		run := a.Stream(context.Background(), "run", OnThread("dispatch-parallel-lease-loss"))
		for event, iterErr := range run.Iter() {
			if done, ok := event.(core.ToolDone); ok && done.Result.CallID == "call-2" {
				secondDone++
			}
			if iterErr != nil {
				err = iterErr
			}
		}
		if !errors.Is(err, leaseErr) {
			t.Fatalf("run error = %v, want lease renewal failure", err)
		}
		if got := firstCalls.Load(); got != 1 {
			t.Fatalf("first tool calls = %d, want 1", got)
		}
		if got := secondCalls.Load(); got != 0 {
			t.Fatalf("second tool calls = %d, want 0 after lease loss", got)
		}
		if got := afterSecond.Load(); got != 0 {
			t.Fatalf("second AfterTool calls = %d, want 0", got)
		}
		if secondDone != 0 {
			t.Fatalf("second ToolDone events = %d, want 0", secondDone)
		}

		call := core.ToolCall{ID: "call-2", Name: "second", Args: []byte(`{}`)}
		invocationID, _ := toolInvocation("dispatch-parallel-lease-loss", 0, call)
		execution, ok, lookupErr := base.ToolExecution(context.Background(), "dispatch-parallel-lease-loss", invocationID)
		if lookupErr != nil {
			t.Fatal(lookupErr)
		}
		if !ok || execution.State != checkpoint.ToolClaimed || execution.Result != nil {
			t.Fatalf("second tool execution = %+v, found %v; want claimed with no result", execution, ok)
		}
	})
}

func TestSnapshotConsistencyDispatchAfterCancel(t *testing.T) {
	t.Run("model", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		model := &dispatchIgnoringModel{respond: func(*llm.Request) *llm.Response {
			return mock.Text("ignored cancellation")
		}}
		mw := &dispatchHooks{beforeModel: func(*LoopContext) { cancel() }}
		a, err := New(WithModel(model), WithMiddleware(mw), WithCheckpointer(checkpoint.NewMemory()))
		if err != nil {
			t.Fatal(err)
		}

		_, err = a.Stream(ctx, "run", OnThread("dispatch-cancel-model")).Wait()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context.Canceled", err)
		}
		if got := model.calls.Load(); got != 0 {
			t.Fatalf("model Generate calls = %d, want 0 after cancellation", got)
		}
	})

	t.Run("tool batch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		model := &dispatchIgnoringModel{respond: func(*llm.Request) *llm.Response {
			return mock.CallTool("call-1", "danger", `{}`)
		}}
		var toolCalls atomic.Int32
		danger := tool.New("danger", "dangerous tool", func(*tool.Context, struct{}) (string, error) {
			toolCalls.Add(1)
			return "ignored cancellation", nil
		})
		mw := &dispatchHooks{beforeTool: func(*LoopContext) { cancel() }}
		a, err := New(
			WithModel(model),
			WithTools(danger),
			WithMiddleware(mw),
			WithCheckpointer(checkpoint.NewMemory()),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = a.Stream(ctx, "run", OnThread("dispatch-cancel-tool")).Wait()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context.Canceled", err)
		}
		if got := toolCalls.Load(); got != 0 {
			t.Fatalf("tool calls = %d, want 0 after cancellation", got)
		}
	})
}

func TestSnapshotConsistencyResumeDispatchGuard(t *testing.T) {
	ctx := context.Background()
	leaseErr := errors.New("resume lease lost after tool claim")
	base := checkpoint.NewMemory()
	store := &dispatchClaimStore{ThreadCoordinator: base}
	capture := &dispatchContextCapture{}
	gate := dispatchApprovalGate{}
	var toolCalls atomic.Int32
	var afterCalls atomic.Int32

	model := &dispatchIgnoringModel{respond: func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("call-1", "danger", `{}`)
	}}
	danger := tool.New("danger", "dangerous tool", func(*tool.Context, struct{}) (string, error) {
		toolCalls.Add(1)
		return "done", nil
	})
	after := &dispatchHooks{afterTool: func(*LoopContext) { afterCalls.Add(1) }}
	a, err := New(
		WithModel(model),
		WithTools(danger),
		WithMiddleware(capture, gate, after),
		WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "pause", OnThread("resume-dispatch-guard"))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	run.Decide(Allow("call-1"))
	store.setAfterClaim(func() { capture.fail(leaseErr) })

	continued, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var toolDone int
	for event, iterErr := range continued.Iter() {
		if _, ok := event.(core.ToolDone); ok {
			toolDone++
		}
		if iterErr != nil {
			err = iterErr
		}
	}
	if !errors.Is(err, leaseErr) {
		t.Fatalf("resume error = %v, want lease loss", err)
	}
	if got := toolCalls.Load(); got != 0 {
		t.Fatalf("tool handler calls = %d, want 0 after post-claim lease loss", got)
	}
	if got := afterCalls.Load(); got != 0 {
		t.Fatalf("AfterTool calls = %d, want 0 without a handler result", got)
	}
	if toolDone != 0 {
		t.Fatalf("ToolDone events = %d, want 0 without a handler result", toolDone)
	}

	call := core.ToolCall{ID: "call-1", Name: "danger", Args: []byte(`{}`)}
	invocationID, _ := toolInvocation("resume-dispatch-guard", 0, call)
	execution, ok, err := base.ToolExecution(ctx, "resume-dispatch-guard", invocationID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("claimed tool execution was not preserved")
	}
	if execution.State != checkpoint.ToolClaimed || execution.Result != nil {
		t.Fatalf("tool execution = %+v, want claimed with no result", execution)
	}
}

type dispatchIgnoringModel struct {
	calls   atomic.Int32
	respond func(*llm.Request) *llm.Response
}

func (*dispatchIgnoringModel) Name() string { return "dispatch-ignores-context" }

func (m *dispatchIgnoringModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	m.calls.Add(1)
	return func(yield func(*llm.Response, error) bool) {
		yield(m.respond(req), nil)
	}
}

type dispatchHooks struct {
	BaseMiddleware
	beforeModel     func(*LoopContext)
	afterModel      func(*LoopContext)
	beforeTool      func(*LoopContext)
	afterTool       func(*LoopContext)
	afterToolResult func(*LoopContext, *core.ToolResult)
}

func (m *dispatchHooks) BeforeModel(lc *LoopContext) (core.Directive, error) {
	if m.beforeModel != nil {
		m.beforeModel(lc)
	}
	return core.Directive{}, nil
}

func (m *dispatchHooks) AfterModel(lc *LoopContext, _ *llm.Response) (core.Directive, error) {
	if m.afterModel != nil {
		m.afterModel(lc)
	}
	return core.Directive{}, nil
}

func (m *dispatchHooks) BeforeTool(lc *LoopContext, _ *core.ToolCall) (core.Directive, error) {
	if m.beforeTool != nil {
		m.beforeTool(lc)
	}
	return core.Directive{}, nil
}

func (m *dispatchHooks) AfterTool(lc *LoopContext, result *core.ToolResult) (core.Directive, error) {
	if m.afterTool != nil {
		m.afterTool(lc)
	}
	if m.afterToolResult != nil {
		m.afterToolResult(lc, result)
	}
	return core.Directive{}, nil
}

type dispatchContextCapture struct {
	BaseMiddleware
	mu sync.Mutex
	rc *RunContext
}

func (m *dispatchContextCapture) BeforeTool(lc *LoopContext, _ *core.ToolCall) (core.Directive, error) {
	m.mu.Lock()
	m.rc = lc.RunContext
	m.mu.Unlock()
	return core.Directive{}, nil
}

func (m *dispatchContextCapture) fail(err error) {
	m.mu.Lock()
	rc := m.rc
	m.mu.Unlock()
	dispatchGuard(rc).recordError(err)
}

type dispatchApprovalGate struct{ BaseMiddleware }

func (dispatchApprovalGate) BeforeTool(lc *LoopContext, call *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(call) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

type dispatchClaimStore struct {
	checkpoint.ThreadCoordinator
	mu         sync.Mutex
	afterClaim func()
}

type dispatchBlockingClaimStore struct {
	checkpoint.ThreadCoordinator
	beforeReturn func(checkpoint.ToolClaimRequest)
}

func (s *dispatchBlockingClaimStore) ClaimTool(ctx context.Context, req checkpoint.ToolClaimRequest) (checkpoint.ToolExecution, error) {
	execution, err := s.ThreadCoordinator.ClaimTool(ctx, req)
	if err == nil && s.beforeReturn != nil {
		s.beforeReturn(req)
	}
	return execution, err
}

func (s *dispatchClaimStore) ClaimTool(ctx context.Context, req checkpoint.ToolClaimRequest) (checkpoint.ToolExecution, error) {
	execution, err := s.ThreadCoordinator.ClaimTool(ctx, req)
	if err != nil {
		return execution, err
	}
	s.mu.Lock()
	afterClaim := s.afterClaim
	s.mu.Unlock()
	if afterClaim != nil {
		afterClaim()
	}
	return execution, nil
}

func (s *dispatchClaimStore) setAfterClaim(afterClaim func()) {
	s.mu.Lock()
	s.afterClaim = afterClaim
	s.mu.Unlock()
}

func dispatchGuard(rc *RunContext) *writeGuard {
	d := rc.ensureDurableState()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.guard
}

func dispatchToolResponse(calls ...core.ToolCall) *llm.Response {
	parts := make([]core.Part, len(calls))
	for i := range calls {
		parts[i] = calls[i]
	}
	return &llm.Response{
		Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
		StopReason: llm.StopToolUse,
	}
}
