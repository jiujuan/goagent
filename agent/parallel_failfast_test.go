package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func parallelDrain(t *testing.T, run *agent.Run, timeout time.Duration) ([]core.Event, error) {
	t.Helper()
	type drained struct {
		events []core.Event
		err    error
	}
	done := make(chan drained, 1)
	go func() {
		events, err := phase1Drain(t, run)
		done <- drained{events: events, err: err}
	}()
	select {
	case r := <-done:
		return r.events, r.err
	case <-time.After(timeout):
		run.Cancel()
		t.Fatalf("run did not settle within %v", timeout)
		return nil, nil
	}
}

func parallelBranchStarts(events []core.Event, rootID string) []core.TaskStarted {
	var starts []core.TaskStarted
	for _, ev := range events {
		if ts, ok := ev.(core.TaskStarted); ok && ts.ParentID == rootID {
			starts = append(starts, ts)
		}
	}
	return starts
}

func parallelCanceled(events []core.Event) []core.BranchCanceled {
	var canceled []core.BranchCanceled
	for _, ev := range events {
		if bc, ok := ev.(core.BranchCanceled); ok {
			canceled = append(canceled, bc)
		}
	}
	return canceled
}

func parallelTaskDone(events []core.Event, taskID string) (core.TaskDone, bool) {
	for _, ev := range events {
		if done, ok := ev.(core.TaskDone); ok && done.TaskID == taskID {
			return done, true
		}
	}
	return core.TaskDone{}, false
}

// failAfterSignal fails every model call after wait is closed.
type failAfterSignal struct {
	agent.BaseMiddleware
	msg  string
	wait <-chan struct{}
}

func (m failAfterSignal) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	if m.wait != nil {
		<-m.wait
	}
	return core.Directive{}, errors.New(m.msg)
}

// failAndSignal closes signal once, then fails every model call.
type failAndSignal struct {
	agent.BaseMiddleware
	msg    string
	signal chan struct{}
	once   sync.Once
}

func (m *failAndSignal) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	m.once.Do(func() { close(m.signal) })
	return core.Directive{}, errors.New(m.msg)
}

// lateCancel returns an error wrapping context.Canceled after wait closes,
// simulating a branch that only notices cancellation after the fact.
type lateCancel struct {
	agent.BaseMiddleware
	msg  string
	wait <-chan struct{}
}

func (m lateCancel) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	<-m.wait
	return core.Directive{}, fmt.Errorf("%s: %w", m.msg, context.Canceled)
}

func parallelToolThenText(name string) llm.Model {
	return parallelToolCallThenText(name, "call-1")
}

func parallelToolCallThenText(name, callID string) llm.Model {
	return mock.New(name, func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool(callID, name, `{}`)
	})
}

func parallelBlockingTool(entered chan<- struct{}, canceled *atomic.Bool) tool.Tool {
	return tool.New("block", "block until canceled", func(ctx *tool.Context, _ struct{}) (string, error) {
		entered <- struct{}{}
		<-ctx.Done()
		canceled.Store(true)
		return "", ctx.Err()
	})
}

func parallelWaitTool(wait <-chan struct{}, ran *atomic.Int32) tool.Tool {
	return tool.New("wait", "wait for release", func(_ *tool.Context, _ struct{}) (string, error) {
		ran.Add(1)
		<-wait
		return "wait-done", nil
	})
}

func TestParallelCancelSiblings(t *testing.T) {
	bEntered := make(chan struct{}, 1)
	var canceled atomic.Bool
	branchB, err := agent.New(
		agent.WithName("b"),
		agent.WithModel(parallelToolThenText("block")),
		agent.WithTools(parallelBlockingTool(bEntered, &canceled)),
	)
	if err != nil {
		t.Fatal(err)
	}
	branchA, err := agent.New(
		agent.WithName("a"),
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(failAfterSignal{msg: "a-branch-failed", wait: bEntered}),
	)
	if err != nil {
		t.Fatal(err)
	}

	flow := agent.Parallel("p", branchA, branchB)
	run := flow.Stream(context.Background(), "go")
	events, runErr := parallelDrain(t, run, 5*time.Second)
	if runErr == nil || !strings.Contains(runErr.Error(), "a-branch-failed") {
		t.Fatalf("run error = %v, want the branch A error", runErr)
	}
	if !canceled.Load() {
		t.Fatal("branch B's tool was not canceled")
	}
	canceledEvents := parallelCanceled(events)
	if len(canceledEvents) != 1 || canceledEvents[0].Name != "b" {
		t.Fatalf("BranchCanceled events = %+v, want exactly one for branch b", canceledEvents)
	}
	if canceledEvents[0].Reason == nil || !strings.Contains(canceledEvents[0].Reason.Error(), "a-branch-failed") {
		t.Fatalf("BranchCanceled reason = %v, want branch A's failure", canceledEvents[0].Reason)
	}
	starts := parallelBranchStarts(events, run.Task().ID)
	if len(starts) != 2 {
		t.Fatalf("branch TaskStarted count = %d, want 2", len(starts))
	}
	doneA, ok := parallelTaskDone(events, starts[0].TaskID)
	if !ok || doneA.Status != "failed" {
		t.Fatalf("branch A TaskDone = %+v, want failed", doneA)
	}
	doneB, ok := parallelTaskDone(events, starts[1].TaskID)
	if !ok || doneB.Status != "canceled" {
		t.Fatalf("branch B TaskDone = %+v, want canceled", doneB)
	}
}

func TestParallelWaitForAll(t *testing.T) {
	aFailed := make(chan struct{})
	var bRan atomic.Int32
	branchA, err := agent.New(
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(&failAndSignal{msg: "a-branch-failed", signal: aFailed}),
	)
	if err != nil {
		t.Fatal(err)
	}
	branchB, err := agent.New(
		agent.WithModel(parallelToolThenText("wait")),
		agent.WithTools(parallelWaitTool(aFailed, &bRan)),
	)
	if err != nil {
		t.Fatal(err)
	}

	flow := agent.ParallelWithOptions("p", agent.ParallelOptions{Failure: agent.WaitForAll}, branchA, branchB)
	run := flow.Stream(context.Background(), "go")
	events, runErr := parallelDrain(t, run, 5*time.Second)
	if runErr == nil || !strings.Contains(runErr.Error(), "a-branch-failed") {
		t.Fatalf("run error = %v, want the branch A error", runErr)
	}
	if got := len(parallelCanceled(events)); got != 0 {
		t.Fatalf("WaitForAll published %d BranchCanceled events, want 0", got)
	}
	if got := bRan.Load(); got != 1 {
		t.Fatalf("branch B tool runs = %d, want 1", got)
	}
	starts := parallelBranchStarts(events, run.Task().ID)
	if len(starts) != 2 {
		t.Fatalf("branch TaskStarted count = %d, want 2", len(starts))
	}
	doneA, ok := parallelTaskDone(events, starts[0].TaskID)
	if !ok || doneA.Status != "failed" {
		t.Fatalf("branch A TaskDone = %+v, want failed", doneA)
	}
	doneB, ok := parallelTaskDone(events, starts[1].TaskID)
	if !ok || doneB.Status != "done" {
		t.Fatalf("branch B TaskDone = %+v, want done after WaitForAll", doneB)
	}
}

func TestParallelRootErrorDeterministic(t *testing.T) {
	bFailed := make(chan struct{})
	branchA, err := agent.New(
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(failAfterSignal{msg: "a-branch-failed", wait: bFailed}),
	)
	if err != nil {
		t.Fatal(err)
	}
	branchB, err := agent.New(
		agent.WithModel(mock.New("b", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(&failAndSignal{msg: "b-branch-failed", signal: bFailed}),
	)
	if err != nil {
		t.Fatal(err)
	}

	flow := agent.Parallel("p", branchA, branchB)
	run := flow.Stream(context.Background(), "go")
	events, runErr := parallelDrain(t, run, 5*time.Second)
	if runErr == nil || !strings.Contains(runErr.Error(), "a-branch-failed") {
		t.Fatalf("run error = %v, want the declaration-first branch A error", runErr)
	}
	if strings.Contains(runErr.Error(), "b-branch-failed") {
		t.Fatalf("run error = %v, branch B's late failure overrode the root cause", runErr)
	}
	starts := parallelBranchStarts(events, run.Task().ID)
	if len(starts) != 2 {
		t.Fatalf("branch TaskStarted count = %d, want 2", len(starts))
	}
	doneA, ok := parallelTaskDone(events, starts[0].TaskID)
	if !ok || doneA.Status != "failed" {
		t.Fatalf("branch A TaskDone = %+v, want failed even after cancellation", doneA)
	}
	doneB, ok := parallelTaskDone(events, starts[1].TaskID)
	if !ok || doneB.Status != "failed" {
		t.Fatalf("branch B TaskDone = %+v, want failed", doneB)
	}
}

func TestParallelCanceledErrorDoesNotOverrideRoot(t *testing.T) {
	aFailed := make(chan struct{})
	branchA, err := agent.New(
		agent.WithModel(mock.New("a", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(&failAndSignal{msg: "a-branch-failed", signal: aFailed}),
	)
	if err != nil {
		t.Fatal(err)
	}
	branchB, err := agent.New(
		agent.WithModel(mock.New("b", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(lateCancel{msg: "b-late", wait: aFailed}),
	)
	if err != nil {
		t.Fatal(err)
	}

	flow := agent.Parallel("p", branchA, branchB)
	run := flow.Stream(context.Background(), "go")
	events, runErr := parallelDrain(t, run, 5*time.Second)
	if runErr == nil || !strings.Contains(runErr.Error(), "a-branch-failed") {
		t.Fatalf("run error = %v, want the branch A error", runErr)
	}
	if strings.Contains(runErr.Error(), "b-late") {
		t.Fatalf("run error = %v, want branch B's cancellation error ignored", runErr)
	}
	starts := parallelBranchStarts(events, run.Task().ID)
	if len(starts) != 2 {
		t.Fatalf("branch TaskStarted count = %d, want 2", len(starts))
	}
	doneB, ok := parallelTaskDone(events, starts[1].TaskID)
	if !ok || doneB.Status != "canceled" {
		t.Fatalf("branch B TaskDone = %+v, want canceled", doneB)
	}
}

func TestParallelPauseDoesNotCancel(t *testing.T) {
	var ran atomic.Int32
	branchA, err := agent.New(
		agent.WithModel(phase1Calls(core.ToolCall{ID: "c1", Name: "danger", Args: json.RawMessage(`{}`)})),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(phase1ApprovalGate{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	branchB, err := agent.New(agent.WithModel(mock.New("b", func(*llm.Request) *llm.Response {
		return mock.Text("b-done")
	})))
	if err != nil {
		t.Fatal(err)
	}

	flow := agent.Parallel("p", branchA, branchB)
	run := flow.Stream(context.Background(), "go")
	events, runErr := parallelDrain(t, run, 5*time.Second)
	if runErr != nil {
		t.Fatalf("paused parallel run returned error: %v", runErr)
	}
	if _, ok := phase1Interrupted(events); !ok {
		t.Fatal("parallel run did not surface the branch pause")
	}
	if got := len(parallelCanceled(events)); got != 0 {
		t.Fatalf("pause published %d BranchCanceled events, want 0", got)
	}
	starts := parallelBranchStarts(events, run.Task().ID)
	if len(starts) != 2 {
		t.Fatalf("branch TaskStarted count = %d, want 2", len(starts))
	}
	doneA, ok := parallelTaskDone(events, starts[0].TaskID)
	if !ok || doneA.Status != "paused" {
		t.Fatalf("branch A TaskDone = %+v, want paused", doneA)
	}
	doneB, ok := parallelTaskDone(events, starts[1].TaskID)
	if !ok || doneB.Status != "done" {
		t.Fatalf("branch B TaskDone = %+v, want done", doneB)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("paused branch ran its tool %d times, want 0", got)
	}
}

// pauseAndSignal interrupts the call and closes hit first, so a sibling can
// fail only after the pause decision exists.
type pauseAndSignal struct {
	agent.BaseMiddleware
	hit  chan struct{}
	once sync.Once
}

func (g *pauseAndSignal) BeforeTool(lc *agent.LoopContext, call *core.ToolCall) (core.Directive, error) {
	g.once.Do(func() { close(g.hit) })
	if lc.IsApproved(call) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

func TestParallelErrorBeatsPause(t *testing.T) {
	hit := make(chan struct{})
	var ran atomic.Int32
	branchA, err := agent.New(
		agent.WithName("a"),
		agent.WithModel(phase1Calls(core.ToolCall{ID: "a-call", Name: "danger", Args: json.RawMessage(`{}`)})),
		agent.WithTools(phase1Tool("danger", &ran)),
		agent.WithMiddleware(&pauseAndSignal{hit: hit}),
	)
	if err != nil {
		t.Fatal(err)
	}
	branchB, err := agent.New(
		agent.WithName("b"),
		agent.WithModel(mock.New("b", func(*llm.Request) *llm.Response { return mock.Text("unreachable") })),
		agent.WithMiddleware(failAfterSignal{msg: "b-branch-failed", wait: hit}),
	)
	if err != nil {
		t.Fatal(err)
	}

	flow := agent.Parallel("p", branchA, branchB)
	run := flow.Stream(context.Background(), "go")
	_, runErr := parallelDrain(t, run, 5*time.Second)
	if runErr == nil || !strings.Contains(runErr.Error(), "b-branch-failed") {
		t.Fatalf("run error = %v, want the branch B failure to win over the pause", runErr)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("approval-gated tool ran %d times, want 0", got)
	}
}

func TestParallelMaxConcurrency(t *testing.T) {
	var active, peak, calls atomic.Int32
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	probe := tool.New("probe", "probe concurrency", func(_ *tool.Context, _ struct{}) (string, error) {
		calls.Add(1)
		n := active.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		return "probe-done", nil
	})
	subs := make([]*agent.Agent, 4)
	for i := range subs {
		a, err := agent.New(
			agent.WithModel(parallelToolCallThenText("probe", fmt.Sprintf("call-%d", i))),
			agent.WithTools(probe),
		)
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = a
	}
	go func() {
		<-entered
		<-entered
		// Give any over-admitted branches time to enter before releasing.
		time.Sleep(30 * time.Millisecond)
		close(release)
	}()

	flow := agent.ParallelWithOptions("p", agent.ParallelOptions{MaxConcurrency: 2}, subs...)
	run := flow.Stream(context.Background(), "go")
	events, runErr := parallelDrain(t, run, 5*time.Second)
	if runErr != nil {
		t.Fatalf("parallel run failed: %v", runErr)
	}
	if got := peak.Load(); got != 2 {
		t.Fatalf("peak concurrent branches = %d, want 2", got)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("probe calls = %d, want 4", got)
	}
	done := 0
	for _, ev := range events {
		if d, ok := ev.(core.TaskDone); ok && d.Status == "done" && d.Kind == string(agent.TaskKindChat) {
			done++
		}
	}
	if done != 4 {
		t.Fatalf("completed branches = %d, want 4", done)
	}
}
