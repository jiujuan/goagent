package agent_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func planDrain(t *testing.T, run *agent.Run) ([]core.Event, error) {
	t.Helper()
	return parallelDrain(t, run, 5*time.Second)
}

func planStatuses(events []core.Event) map[string]string {
	out := map[string]string{}
	for _, ev := range events {
		if d, ok := ev.(core.PlanNodeDone); ok {
			out[d.NodeID] = d.Status
		}
	}
	return out
}

func planCountTaskDone(events []core.Event, status, rootID string) int {
	n := 0
	for _, ev := range events {
		if d, ok := ev.(core.TaskDone); ok && d.TaskID != rootID && d.Status == status {
			n++
		}
	}
	return n
}

// failOnCancel returns its own error once the node context is canceled.
type failOnCancel struct {
	agent.BaseMiddleware
	msg string
}

func (m failOnCancel) BeforeModel(lc *agent.LoopContext) (core.Directive, error) {
	<-lc.Context.Done()
	return core.Directive{}, errors.New(m.msg)
}

// failOnceThenRelease fails the first attempt, then releases waiters on the
// retry attempt.
type failOnceThenRelease struct {
	agent.BaseMiddleware
	calls   atomic.Int32
	release chan struct{}
	once    sync.Once
}

func (m *failOnceThenRelease) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	if m.calls.Add(1) == 1 {
		return core.Directive{}, errors.New("a-first-attempt-failed")
	}
	m.once.Do(func() { close(m.release) })
	return core.Directive{}, nil
}

func planBlockingTool(entered chan struct{}, canceled *atomic.Bool) tool.Tool {
	var once sync.Once
	return tool.New("block", "block until canceled", func(ctx *tool.Context, _ struct{}) (string, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		canceled.Store(true)
		return "", ctx.Err()
	})
}

func planWaitTool(wait <-chan struct{}, ran *atomic.Int32) tool.Tool {
	return tool.New("wait", "wait for release", func(_ *tool.Context, _ struct{}) (string, error) {
		ran.Add(1)
		<-wait
		return "wait-done", nil
	})
}

func planToolThenText(toolName, final string) llm.Model {
	return mock.New(toolName, func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text(final)
		}
		return mock.CallTool("call-1", toolName, `{}`)
	})
}

func TestPlanFailFastCancelsInflight(t *testing.T) {
	entered := make(chan struct{})
	var canceled atomic.Bool
	bWorker, err := agent.New(
		agent.WithModel(planToolThenText("block", "unreachable")),
		agent.WithTools(planBlockingTool(entered, &canceled)),
	)
	if err != nil {
		t.Fatal(err)
	}
	aWorker, err := agent.New(
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(failAfterSignal{msg: "a-node-failed", wait: entered}),
	)
	if err != nil {
		t.Fatal(err)
	}

	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "a", Task: "fail", Worker: aWorker, MaxRetries: 1},
		{ID: "b", Task: "block", Worker: bWorker},
	}}
	pa := agent.NewPlan("ff", plan, agent.WithConcurrency(2))
	run := pa.Stream(context.Background(), "go")
	events, runErr := planDrain(t, run)
	if runErr == nil || !strings.Contains(runErr.Error(), "a-node-failed") {
		t.Fatalf("plan error = %v, want the exhausted node A error", runErr)
	}
	if !canceled.Load() {
		t.Fatal("in-flight node B was not canceled")
	}
	status := planStatuses(events)
	if status["a"] != "failed" || status["b"] != "canceled" {
		t.Fatalf("node statuses = %v, want a:failed b:canceled", status)
	}
	if got := planCountTaskDone(events, "canceled", run.Task().ID); got != 1 {
		t.Fatalf("canceled TaskDone events = %d, want 1 for node B", got)
	}
}

func TestPlanFailFastRetryDoesNotCancel(t *testing.T) {
	release := make(chan struct{})
	var bRan atomic.Int32
	aWorker, err := agent.New(
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("A-OK") })),
		agent.WithMiddleware(&failOnceThenRelease{release: release}),
	)
	if err != nil {
		t.Fatal(err)
	}
	bWorker, err := agent.New(
		agent.WithModel(planToolThenText("wait", "B-OK")),
		agent.WithTools(planWaitTool(release, &bRan)),
	)
	if err != nil {
		t.Fatal(err)
	}

	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "a", Task: "retry then succeed", Worker: aWorker, MaxRetries: 1},
		{ID: "b", Task: "wait for the retry", Worker: bWorker},
	}}
	pa := agent.NewPlan("retry", plan, agent.WithConcurrency(2))
	events, runErr := planDrain(t, pa.Stream(context.Background(), "go"))
	if runErr != nil {
		t.Fatalf("retried node failed the plan: %v", runErr)
	}
	status := planStatuses(events)
	if status["a"] != "done" || status["b"] != "done" {
		t.Fatalf("node statuses = %v, want a:done b:done", status)
	}
	if got := planCountTaskDone(events, "canceled", ""); got != 0 {
		t.Fatalf("canceled TaskDone events = %d, want 0 (a retry must not cancel siblings)", got)
	}
	if got := bRan.Load(); got != 1 {
		t.Fatalf("sibling tool runs = %d, want 1", got)
	}
}

func TestPlanFailFastRootErrorNotOverwritten(t *testing.T) {
	aWorker, err := agent.New(
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(failAfterSignal{msg: "a-node-failed"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	bWorker, err := agent.New(
		agent.WithModel(mock.New("b", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(failOnCancel{msg: "b-node-failed"}),
	)
	if err != nil {
		t.Fatal(err)
	}

	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "a", Task: "fail first", Worker: aWorker},
		{ID: "b", Task: "fail after cancellation", Worker: bWorker},
	}}
	pa := agent.NewPlan("root", plan, agent.WithConcurrency(2))
	run := pa.Stream(context.Background(), "go")
	events, runErr := planDrain(t, run)
	if runErr == nil || !strings.Contains(runErr.Error(), "a-node-failed") {
		t.Fatalf("plan error = %v, want node A", runErr)
	}
	if strings.Contains(runErr.Error(), "b-node-failed") {
		t.Fatalf("plan error = %v, the later failure overwrote the root cause", runErr)
	}
	status := planStatuses(events)
	// B settles only after the node context is canceled, so its PlanNodeDone is
	// "canceled" even though it reported its own error; TaskDone keeps that own
	// error as "failed" (each status uses its own source of truth).
	if status["a"] != "failed" || status["b"] != "canceled" {
		t.Fatalf("node statuses = %v, want a:failed b:canceled", status)
	}
	if got := planCountTaskDone(events, "failed", run.Task().ID); got != 2 {
		t.Fatalf("failed TaskDone events = %d, want 2 (both nodes returned their own errors)", got)
	}
	if got := planCountTaskDone(events, "canceled", run.Task().ID); got != 0 {
		// Node B returns its own non-context error, so its child task is failed,
		// not canceled; TaskDone uses the returned error, not nodeCtx.
		t.Fatalf("canceled TaskDone events = %d, want 0", got)
	}
}

func TestPlanContinueOnErrorUnaffected(t *testing.T) {
	aFailed := make(chan struct{})
	var bRan atomic.Int32
	aWorker, err := agent.New(
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(&failAndSignal{msg: "a-node-failed", signal: aFailed}),
	)
	if err != nil {
		t.Fatal(err)
	}
	bWorker, err := agent.New(
		agent.WithModel(planToolThenText("wait", "B-OK")),
		agent.WithTools(planWaitTool(aFailed, &bRan)),
	)
	if err != nil {
		t.Fatal(err)
	}

	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "a", Task: "fail", Worker: aWorker},
		{ID: "b", Task: "independent", Worker: bWorker},
	}}
	pa := agent.NewPlan("coe", plan, agent.WithConcurrency(2), agent.WithErrorPolicy(agent.ContinueOnError))
	events, runErr := planDrain(t, pa.Stream(context.Background(), "go"))
	if runErr != nil {
		t.Fatalf("ContinueOnError plan failed: %v", runErr)
	}
	status := planStatuses(events)
	if status["a"] != "failed" || status["b"] != "done" {
		t.Fatalf("node statuses = %v, want a:failed b:done", status)
	}
	if got := planCountTaskDone(events, "canceled", ""); got != 0 {
		t.Fatalf("canceled TaskDone events = %d, want 0", got)
	}
	if got := bRan.Load(); got != 1 {
		t.Fatalf("independent node tool runs = %d, want 1", got)
	}
}

func TestPlanParentCancelReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	var canceled atomic.Bool
	bWorker, err := agent.New(
		agent.WithModel(planToolThenText("block", "unreachable")),
		agent.WithTools(planBlockingTool(entered, &canceled)),
	)
	if err != nil {
		t.Fatal(err)
	}

	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "a", Task: "quick"},
		{ID: "b", Task: "block", Worker: bWorker, DependsOn: []string{"a"}},
	}}
	pa := agent.NewPlan("cancel", plan, agent.WithWorker(echoWorker(t)), agent.WithConcurrency(2))
	run := pa.Stream(ctx, "go")
	type outcome struct {
		events []core.Event
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		events, err := phase1Drain(t, run)
		done <- outcome{events: events, err: err}
	}()
	<-entered
	cancel()
	var got outcome
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		run.Cancel()
		t.Fatal("plan did not settle after parent cancellation")
	}
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("plan error = %v, want context.Canceled", got.err)
	}
	status := planStatuses(got.events)
	if status["a"] != "done" || status["b"] != "canceled" {
		t.Fatalf("node statuses = %v, want a:done b:canceled", status)
	}
}

func TestPlanCanceledNodeSkipsDependents(t *testing.T) {
	entered := make(chan struct{})
	var canceled atomic.Bool
	aWorker, err := agent.New(
		agent.WithModel(planToolThenText("block", "unreachable")),
		agent.WithTools(planBlockingTool(entered, &canceled)),
	)
	if err != nil {
		t.Fatal(err)
	}
	bWorker, err := agent.New(
		agent.WithModel(mock.New("b", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(failAfterSignal{msg: "b-node-failed", wait: entered}),
	)
	if err != nil {
		t.Fatal(err)
	}

	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "a", Task: "block", Worker: aWorker},
		{ID: "b", Task: "fail", Worker: bWorker},
		{ID: "c", Task: "needs a", DependsOn: []string{"a"}},
	}}
	pa := agent.NewPlan("skip", plan, agent.WithWorker(echoWorker(t)), agent.WithConcurrency(2))
	events, runErr := planDrain(t, pa.Stream(context.Background(), "go"))
	if runErr == nil || !strings.Contains(runErr.Error(), "b-node-failed") {
		t.Fatalf("plan error = %v, want node B", runErr)
	}
	if !canceled.Load() {
		t.Fatal("node A was not canceled")
	}
	status := planStatuses(events)
	if status["a"] != "canceled" || status["b"] != "failed" || status["c"] != "skipped" {
		t.Fatalf("node statuses = %v, want a:canceled b:failed c:skipped", status)
	}
}
