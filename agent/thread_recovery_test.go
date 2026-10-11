package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func TestSnapshotConsistencyResumeLongCall(t *testing.T) {
	t.Run("renews while tool and model calls are blocked", func(t *testing.T) {
		ctx := context.Background()
		base := checkpoint.NewMemory()
		store := snapshotConsistencyGuardStore(base)
		begun, err := store.BeginRun(ctx, checkpoint.BeginRunRequest{
			ThreadID: "resume-long-call",
			Owner:    "long-call-owner",
			Lease:    time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}

		ticker := newGuardTestTicker()
		guard := newWriteGuard(store, begun.Claim, 0, time.Minute)
		guard.newTicker = func(time.Duration) writeGuardTicker { return ticker }
		rootCtx, cancelRoot := context.WithCancel(context.Background())
		defer cancelRoot()
		if err := guard.start(rootCtx, cancelRoot); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := guard.stopAndRelease(rootCtx); err != nil {
				t.Errorf("stop guard: %v", err)
			}
		}()

		toolEntered, toolRelease, releaseTool := snapshotConsistencyBlocker()
		modelEntered, modelRelease, releaseModel := snapshotConsistencyBlocker()
		defer releaseTool()
		defer releaseModel()
		var toolOnce sync.Once
		var modelOnce sync.Once
		var toolCalls atomic.Int32

		danger := tool.New("danger", "long-running tool", func(*tool.Context, struct{}) (string, error) {
			toolCalls.Add(1)
			toolOnce.Do(func() { close(toolEntered) })
			<-toolRelease
			return "tool-complete", nil
		})
		model := &dispatchIgnoringModel{respond: func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); !ok {
				return mock.Text("missing tool result")
			}
			modelOnce.Do(func() { close(modelEntered) })
			<-modelRelease
			return mock.Text("complete")
		}}
		a, err := New(WithModel(model), WithTools(danger), WithCheckpointer(store))
		if err != nil {
			t.Fatal(err)
		}

		call := core.ToolCall{ID: "call-1", Name: "danger", Args: []byte(`{}`)}
		rc := snapshotConsistencyResumeContext(rootCtx, "resume-long-call", store, begun.Claim, guard, call)
		outcomes := make(chan runOutcome, 1)
		go func() { outcomes <- a.loop.run(rc) }()

		waitSnapshotConsistencySignal(t, toolEntered, "long-running tool")
		snapshotConsistencyTickAndWait(t, ticker, store, 2)
		snapshotConsistencyTickAndWait(t, ticker, store, 3)
		releaseTool()

		waitSnapshotConsistencySignal(t, modelEntered, "long-running model")
		snapshotConsistencyTickAndWait(t, ticker, store, 4)
		snapshotConsistencyTickAndWait(t, ticker, store, 5)
		releaseModel()

		outcome := waitSnapshotConsistencyOutcome(t, outcomes, "long-call run")
		if outcome.Err != nil {
			t.Fatal(outcome.Err)
		}
		if got := outcome.Result.Message.Text(); got != "complete" {
			t.Fatalf("result = %q, want complete", got)
		}
		if got := toolCalls.Load(); got != 1 {
			t.Fatalf("tool calls = %d, want 1", got)
		}
		if got := model.calls.Load(); got != 1 {
			t.Fatalf("model calls = %d, want 1", got)
		}
		latest, err := base.Latest(ctx, "resume-long-call")
		if err != nil {
			t.Fatal(err)
		}
		if latest == nil || latest.Revision != 2 {
			t.Fatalf("latest checkpoint = %+v, want revision 2", latest)
		}
	})

	t.Run("renewal failure prevents the next model call", func(t *testing.T) {
		leaseErr := errors.New("lease lost during tool call")
		base := checkpoint.NewMemory()
		store := snapshotConsistencyGuardStore(base)
		var renews atomic.Int32
		store.renewFn = func(ctx context.Context, claim checkpoint.Claim, lease time.Duration) (checkpoint.Claim, error) {
			if renews.Add(1) == 1 {
				return base.RenewClaim(ctx, claim, lease)
			}
			return checkpoint.Claim{}, leaseErr
		}
		begun, err := store.BeginRun(context.Background(), checkpoint.BeginRunRequest{
			ThreadID: "resume-renewal-failure",
			Owner:    "failing-owner",
			Lease:    time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}

		ticker := newGuardTestTicker()
		guard := newWriteGuard(store, begun.Claim, 0, time.Minute)
		guard.newTicker = func(time.Duration) writeGuardTicker { return ticker }
		rootCtx, cancelRoot := context.WithCancel(context.Background())
		defer cancelRoot()
		if err := guard.start(rootCtx, cancelRoot); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := guard.stopAndRelease(rootCtx); !errors.Is(err, leaseErr) {
				t.Errorf("stop guard error = %v, want lease failure", err)
			}
		}()

		toolEntered, toolRelease, releaseTool := snapshotConsistencyBlocker()
		defer releaseTool()
		var toolCalls atomic.Int32
		danger := tool.New("danger", "long-running tool", func(*tool.Context, struct{}) (string, error) {
			toolCalls.Add(1)
			close(toolEntered)
			<-toolRelease
			return "tool-complete", nil
		})
		model := &dispatchIgnoringModel{respond: func(*llm.Request) *llm.Response {
			return mock.Text("must not run")
		}}
		a, err := New(WithModel(model), WithTools(danger), WithCheckpointer(store))
		if err != nil {
			t.Fatal(err)
		}

		call := core.ToolCall{ID: "call-1", Name: "danger", Args: []byte(`{}`)}
		rc := snapshotConsistencyResumeContext(rootCtx, "resume-renewal-failure", store, begun.Claim, guard, call)
		outcomes := make(chan runOutcome, 1)
		go func() { outcomes <- a.loop.run(rc) }()

		waitSnapshotConsistencySignal(t, toolEntered, "tool before renewal failure")
		ticker.tick()
		waitSnapshotConsistencySignal(t, rootCtx.Done(), "root cancellation after renewal failure")
		releaseTool()

		outcome := waitSnapshotConsistencyOutcome(t, outcomes, "renewal-failure run")
		if !errors.Is(outcome.Err, leaseErr) {
			t.Fatalf("run error = %v, want lease failure", outcome.Err)
		}
		if got := toolCalls.Load(); got != 1 {
			t.Fatalf("tool calls = %d, want 1", got)
		}
		if got := model.calls.Load(); got != 0 {
			t.Fatalf("model calls = %d, want 0 after renewal failure", got)
		}
	})
}

func TestSnapshotConsistencyClaimedToolOnLeaseLoss(t *testing.T) {
	ctx := context.Background()
	leaseErr := errors.New("lease lost after tool started")
	base := checkpoint.NewMemory()
	store := snapshotConsistencyGuardStore(base)
	var renews atomic.Int32
	store.renewFn = func(ctx context.Context, claim checkpoint.Claim, lease time.Duration) (checkpoint.Claim, error) {
		if renews.Add(1) == 1 {
			return base.RenewClaim(ctx, claim, lease)
		}
		return checkpoint.Claim{}, leaseErr
	}
	first, err := store.BeginRun(ctx, checkpoint.BeginRunRequest{
		ThreadID: "claimed-tool-lease-loss",
		Owner:    "first-owner",
		Lease:    time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	firstTicker := newGuardTestTicker()
	firstGuard := newWriteGuard(store, first.Claim, 0, time.Minute)
	firstGuard.newTicker = func(time.Duration) writeGuardTicker { return firstTicker }
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	if err := firstGuard.start(firstCtx, cancelFirst); err != nil {
		t.Fatal(err)
	}

	toolEntered, toolRelease, releaseTool := snapshotConsistencyBlocker()
	defer releaseTool()
	var toolCalls atomic.Int32
	danger := tool.New("danger", "external side effect", func(*tool.Context, struct{}) (string, error) {
		toolCalls.Add(1)
		close(toolEntered)
		<-toolRelease
		return "side-effect-complete", nil
	})
	firstModel := &dispatchIgnoringModel{respond: func(*llm.Request) *llm.Response {
		return mock.Text("must not run before recovery")
	}}
	firstAgent, err := New(WithModel(firstModel), WithTools(danger), WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	call := core.ToolCall{ID: "call-1", Name: "danger", Args: []byte(`{"target":"external"}`)}
	firstRC := snapshotConsistencyResumeContext(firstCtx, "claimed-tool-lease-loss", store, first.Claim, firstGuard, call)
	firstOutcomes := make(chan runOutcome, 1)
	go func() { firstOutcomes <- firstAgent.loop.run(firstRC) }()

	waitSnapshotConsistencySignal(t, toolEntered, "claimed tool handler")
	firstTicker.tick()
	waitSnapshotConsistencySignal(t, firstCtx.Done(), "lease-loss cancellation")
	releaseTool()
	firstOutcome := waitSnapshotConsistencyOutcome(t, firstOutcomes, "lease-loss run")
	if !errors.Is(firstOutcome.Err, leaseErr) {
		t.Fatalf("first run error = %v, want lease failure", firstOutcome.Err)
	}
	if got := firstModel.calls.Load(); got != 0 {
		t.Fatalf("first model calls = %d, want 0", got)
	}

	invocationID, _ := toolInvocation("claimed-tool-lease-loss", 0, call)
	claimed, ok, err := base.ToolExecution(ctx, "claimed-tool-lease-loss", invocationID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || claimed.State != checkpoint.ToolClaimed || claimed.Result != nil {
		t.Fatalf("tool execution after lease loss = %+v, ok=%v; want claimed with no result", claimed, ok)
	}
	if err := firstGuard.stopAndRelease(firstCtx); !errors.Is(err, leaseErr) {
		t.Fatalf("stop first guard error = %v, want lease failure", err)
	}

	secondStore := snapshotConsistencyGuardStore(base)
	second, err := secondStore.BeginRun(ctx, checkpoint.BeginRunRequest{
		ThreadID: "claimed-tool-lease-loss",
		Owner:    "recovery-owner",
		Lease:    time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Claim.FencingToken <= first.Claim.FencingToken {
		t.Fatalf("recovery fencing token = %d, want greater than %d", second.Claim.FencingToken, first.Claim.FencingToken)
	}

	secondTicker := newGuardTestTicker()
	secondGuard := newWriteGuard(secondStore, second.Claim, 0, time.Minute)
	secondGuard.newTicker = func(time.Duration) writeGuardTicker { return secondTicker }
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	if err := secondGuard.start(secondCtx, cancelSecond); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := secondGuard.stopAndRelease(secondCtx); err != nil {
			t.Errorf("stop second guard: %v", err)
		}
	}()

	var sawUnknown atomic.Bool
	secondModel := &dispatchIgnoringModel{respond: func(req *llm.Request) *llm.Response {
		result, ok := mock.LastToolResult(req)
		if ok && result.IsError && len(result.Content) == 1 {
			if text, ok := result.Content[0].(core.Text); ok && strings.Contains(text.Text, "outcome was not durably recorded") {
				sawUnknown.Store(true)
			}
		}
		return mock.Text("handled unknown outcome")
	}}
	secondAgent, err := New(WithModel(secondModel), WithTools(danger), WithCheckpointer(secondStore))
	if err != nil {
		t.Fatal(err)
	}
	secondRC := snapshotConsistencyResumeContext(secondCtx, "claimed-tool-lease-loss", secondStore, second.Claim, secondGuard, call)
	secondOutcome := secondAgent.loop.run(secondRC)
	if secondOutcome.Err != nil {
		t.Fatal(secondOutcome.Err)
	}
	if got := secondOutcome.Result.Message.Text(); got != "handled unknown outcome" {
		t.Fatalf("recovery result = %q", got)
	}
	if got := toolCalls.Load(); got != 1 {
		t.Fatalf("tool calls after recovery = %d, want 1", got)
	}
	if !sawUnknown.Load() {
		t.Fatal("model did not receive the unknown-outcome tool result")
	}

	unknown, ok, err := base.ToolExecution(ctx, "claimed-tool-lease-loss", invocationID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || unknown.State != checkpoint.ToolUnknown || unknown.Result != nil {
		t.Fatalf("tool execution after recovery = %+v, ok=%v; want unknown with no result", unknown, ok)
	}
}

func snapshotConsistencyGuardStore(base checkpoint.ThreadCoordinator) *guardTestCoordinator {
	return &guardTestCoordinator{
		ThreadCoordinator: base,
		renewFn:           base.RenewClaim,
		commitFn:          base.Commit,
		releaseFn:         base.ReleaseClaim,
	}
}

func snapshotConsistencyResumeContext(
	ctx context.Context,
	threadID string,
	store checkpoint.Checkpointer,
	claim checkpoint.Claim,
	guard *writeGuard,
	call core.ToolCall,
) *RunContext {
	assistant := core.Message{Role: core.RoleAssistant, Parts: []core.Part{call}}
	rc := &RunContext{
		Context:  ctx,
		RunID:    threadID + "-run",
		ThreadID: threadID,
		Bus:      bus.New(),
		Topic:    bus.Topic(threadID),
		Store:    store,
		State: &core.State{Messages: []core.Message{
			core.UserText("resume"),
			assistant,
		}},
		durable: newDurableRunState(),
	}
	rc.setDurableClaim(&claim)
	rc.setDurableGuard(guard)
	rc.resumed = newResumeBatch(0, []core.ToolCall{call}, []Approval{Allow(call.ID)}, assistant)
	return rc
}

func snapshotConsistencyTickAndWait(t *testing.T, ticker *guardTestTicker, store *guardTestCoordinator, want int) {
	t.Helper()
	ticker.tick()
	waitForGuard(t, func() bool { return store.renewCount() == want })
}

func waitSnapshotConsistencyOutcome(t *testing.T, outcomes <-chan runOutcome, what string) runOutcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return runOutcome{}
	}
}
