package agent

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func collectTaskEvents(t *testing.T, run *Run) ([]core.Event, error) {
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
	return events, iterErr
}

func eventAt(events []core.Event, match func(core.Event) bool) int {
	for i, ev := range events {
		if match(ev) {
			return i
		}
	}
	return -1
}

func taskStarts(events []core.Event) []core.TaskStarted {
	var out []core.TaskStarted
	for _, ev := range events {
		if e, ok := ev.(core.TaskStarted); ok {
			out = append(out, e)
		}
	}
	return out
}

func taskDones(events []core.Event) []core.TaskDone {
	var out []core.TaskDone
	for _, ev := range events {
		if e, ok := ev.(core.TaskDone); ok {
			out = append(out, e)
		}
	}
	return out
}

func TestTaskEventsRoot(t *testing.T) {
	a, err := New(WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") })))
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "hi")
	events, err := collectTaskEvents(t, run)
	if err != nil {
		t.Fatal(err)
	}

	info := run.Task()
	if info.TurnID == "" {
		t.Fatal("root task has no turn id after the run")
	}
	started := taskStarts(events)
	if len(started) != 1 {
		t.Fatalf("TaskStarted count = %d, want 1", len(started))
	}
	if started[0].TaskID != info.ID {
		t.Fatalf("TaskStarted task id = %q, want run task %q", started[0].TaskID, info.ID)
	}
	if started[0].Depth != 0 || started[0].ParentID != "" {
		t.Fatalf("root TaskStarted = %+v, want depth 0 and no parent", started[0])
	}
	if started[0].Kind != string(TaskKindChat) {
		t.Fatalf("root TaskStarted kind = %q, want chat", started[0].Kind)
	}
	if started[0].ThreadID != info.ThreadID {
		t.Fatalf("root TaskStarted thread = %q, want %q", started[0].ThreadID, info.ThreadID)
	}

	done := taskDones(events)
	if len(done) != 1 {
		t.Fatalf("TaskDone count = %d, want 1", len(done))
	}
	if done[0].TaskID != info.ID || done[0].Status != "done" {
		t.Fatalf("root TaskDone = %+v, want task %q status done", done[0], info.ID)
	}

	startedAt := eventAt(events, func(ev core.Event) bool { _, ok := ev.(core.TaskStarted); return ok })
	turnAt := eventAt(events, func(ev core.Event) bool { _, ok := ev.(core.TurnStarted); return ok })
	doneAt := eventAt(events, func(ev core.Event) bool { _, ok := ev.(core.TaskDone); return ok })
	runDoneAt := eventAt(events, func(ev core.Event) bool { _, ok := ev.(core.RunDone); return ok })
	runStartedAt := eventAt(events, func(ev core.Event) bool { _, ok := ev.(core.RunStarted); return ok })
	if !(runStartedAt >= 0 && runStartedAt < startedAt && startedAt < turnAt && turnAt < doneAt && doneAt < runDoneAt) {
		t.Fatalf("event order = RunStarted:%d TaskStarted:%d TurnStarted:%d TaskDone:%d RunDone:%d", runStartedAt, startedAt, turnAt, doneAt, runDoneAt)
	}
}

// AsTool runs its child through child.Run and never returns the handle, so the
// child's events live on its own ephemeral topic and cannot be subscribed to
// from the tool caller. This drives the same child run (the same withTaskParent
// option AsTool passes) through a Stream handle so those events are observable;
// AsTool's admission path itself is pinned by TestAsToolWithParentAdmitsChild.
func TestTaskEventsAsToolChild(t *testing.T) {
	shared := bus.New()
	parent := &RunContext{
		Context:  context.Background(),
		RunID:    "run_parent",
		ThreadID: "parent-thread",
		Bus:      shared,
		Topic:    bus.Topic("parent-topic"),
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	parent.ensureTask()
	childInfo, err := parent.admitChild(TaskKindChat)
	if err != nil {
		t.Fatal(err)
	}
	parentInfo := parent.taskInfo()

	child, err := New(
		WithModel(mock.New("child", func(*llm.Request) *llm.Response { return mock.Text("child done") })),
		WithBus(shared),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := child.Stream(context.Background(), "task", OnThread("child-thread"), withTaskParent(parent, childInfo))
	events, err := collectTaskEvents(t, run)
	if err != nil {
		t.Fatal(err)
	}

	started := taskStarts(events)
	if len(started) != 1 {
		t.Fatalf("child TaskStarted count = %d, want 1", len(started))
	}
	if started[0].TaskID != childInfo.ID {
		t.Fatalf("child TaskStarted id = %q, want %q", started[0].TaskID, childInfo.ID)
	}
	if started[0].ParentID != parentInfo.ID {
		t.Fatalf("child TaskStarted parent = %q, want parent task %q", started[0].ParentID, parentInfo.ID)
	}
	if started[0].ThreadID != "child-thread" {
		t.Fatalf("child TaskStarted thread = %q, want its own thread", started[0].ThreadID)
	}
	if started[0].Kind != string(TaskKindChat) || started[0].Depth != parentInfo.Depth+1 {
		t.Fatalf("child TaskStarted = %+v, want chat at depth %d", started[0], parentInfo.Depth+1)
	}
	done := taskDones(events)
	if len(done) != 1 {
		t.Fatalf("child TaskDone count = %d, want 1", len(done))
	}
	if done[0].TaskID != childInfo.ID || done[0].Status != "done" {
		t.Fatalf("child TaskDone = %+v, want %q done", done[0], childInfo.ID)
	}
}

func TestTaskEventsParallelBranches(t *testing.T) {
	sayer := func(text string) *Agent {
		a, err := New(WithModel(mock.New(text, func(*llm.Request) *llm.Response { return mock.Text(text) })))
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	flow := Parallel("par", sayer("one"), sayer("two"))
	run := flow.Stream(context.Background(), "go")
	events, err := collectTaskEvents(t, run)
	if err != nil {
		t.Fatal(err)
	}
	root := run.Task()
	started := taskStarts(events)
	if len(started) != 3 {
		t.Fatalf("TaskStarted count = %d, want root + 2 branches", len(started))
	}
	branchIDs := map[string]bool{}
	for _, ev := range started {
		if ev.Depth == 0 {
			if ev.TaskID != root.ID {
				t.Fatalf("root TaskStarted id = %q, want %q", ev.TaskID, root.ID)
			}
			continue
		}
		if ev.ParentID != root.ID || ev.Depth != 1 || ev.Kind != string(TaskKindChat) {
			t.Fatalf("branch TaskStarted = %+v, want parent %q chat depth 1", ev, root.ID)
		}
		branchIDs[ev.TaskID] = true
	}
	if len(branchIDs) != 2 {
		t.Fatalf("branch TaskStarted ids = %v, want 2 distinct branches", branchIDs)
	}
	done := taskDones(events)
	if len(done) != 3 {
		t.Fatalf("TaskDone count = %d, want root + 2 branches", len(done))
	}
	for _, ev := range done {
		if branchIDs[ev.TaskID] && ev.Status != "done" {
			t.Fatalf("branch TaskDone = %+v, want done", ev)
		}
	}
}

func TestTaskEventsPlanNodes(t *testing.T) {
	planner, err := New(WithModel(mock.New("planner", func(*llm.Request) *llm.Response {
		return mock.Text(`{"nodes":[{"id":"a","task":"RA"},{"id":"b","task":"RB"}]}`)
	})))
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(WithModel(mock.New("worker", func(*llm.Request) *llm.Response { return mock.Text("w") })))
	if err != nil {
		t.Fatal(err)
	}
	plan := NewLLMPlan("p", planner, WithWorker(worker))
	run := plan.Stream(context.Background(), "go")
	events, err := collectTaskEvents(t, run)
	if err != nil {
		t.Fatal(err)
	}
	root := run.Task()

	started := taskStarts(events)
	if len(started) != 1+2 {
		t.Fatalf("TaskStarted count = %d, want root + 2 nodes (planner must not emit one)", len(started))
	}
	nodeStarts := map[string]bool{}
	for _, ev := range started {
		if ev.Depth == 0 {
			continue
		}
		if ev.ParentID != root.ID || ev.Depth != 1 || ev.Kind != string(TaskKindChat) {
			t.Fatalf("node TaskStarted = %+v, want parent %q chat depth 1", ev, root.ID)
		}
		nodeStarts[ev.TaskID] = true
	}
	if len(nodeStarts) != 2 {
		t.Fatalf("node TaskStarted ids = %v, want 2 distinct nodes", nodeStarts)
	}
	done := taskDones(events)
	if len(done) != 1+2 {
		t.Fatalf("TaskDone count = %d, want root + 2 nodes", len(done))
	}
	for _, ev := range done {
		if nodeStarts[ev.TaskID] && ev.Status != "done" {
			t.Fatalf("node TaskDone = %+v, want done", ev)
		}
	}
}

func TestTaskEventsNoDuplicateForTransferAndSequential(t *testing.T) {
	sayer := func(text string) *Agent {
		a, err := New(WithModel(mock.New(text, func(*llm.Request) *llm.Response { return mock.Text(text) })))
		if err != nil {
			t.Fatal(err)
		}
		return a
	}

	t.Run("sequential", func(t *testing.T) {
		flow := Sequential("seq", sayer("one"), sayer("two"))
		run := flow.Stream(context.Background(), "go")
		events, err := collectTaskEvents(t, run)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(taskStarts(events)); got != 1 {
			t.Fatalf("Sequential TaskStarted count = %d, want 1", got)
		}
		if got := len(taskDones(events)); got != 1 {
			t.Fatalf("Sequential TaskDone count = %d, want 1", got)
		}
	})

	t.Run("transfer", func(t *testing.T) {
		expert, err := New(
			WithName("expert"),
			WithDescription("answers"),
			WithModel(mock.New("expert", func(*llm.Request) *llm.Response { return mock.Text("42") })),
		)
		if err != nil {
			t.Fatal(err)
		}
		router, err := New(
			WithName("router"),
			WithModel(mock.New("router", func(*llm.Request) *llm.Response {
				return mock.CallTool("t1", "transfer_to_agent", `{"agent":"expert"}`)
			})),
			WithSubAgents(expert),
		)
		if err != nil {
			t.Fatal(err)
		}
		run := router.Stream(context.Background(), "go")
		events, err := collectTaskEvents(t, run)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(taskStarts(events)); got != 1 {
			t.Fatalf("transfer TaskStarted count = %d, want 1", got)
		}
		if got := len(taskDones(events)); got != 1 {
			t.Fatalf("transfer TaskDone count = %d, want 1", got)
		}
	})
}

type taskFailModel struct{}

func (taskFailModel) Name() string { return "fail" }
func (taskFailModel) Generate(context.Context, *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) { yield(nil, errors.New("boom")) }
}

type taskBlockingModel struct{ started chan struct{} }

func (m *taskBlockingModel) Name() string { return "blocking" }
func (m *taskBlockingModel) Generate(ctx context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		close(m.started)
		<-ctx.Done()
		yield(nil, ctx.Err())
	}
}

type taskInterruptGate struct{ BaseMiddleware }

func (taskInterruptGate) BeforeTool(*LoopContext, *core.ToolCall) (core.Directive, error) {
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

func TestTaskDoneStatus(t *testing.T) {
	doneStatus := func(t *testing.T, events []core.Event) core.TaskDone {
		t.Helper()
		done := taskDones(events)
		if len(done) != 1 {
			t.Fatalf("TaskDone count = %d, want 1", len(done))
		}
		return done[0]
	}

	t.Run("done", func(t *testing.T) {
		a, err := New(WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") })))
		if err != nil {
			t.Fatal(err)
		}
		events, err := collectTaskEvents(t, a.Stream(context.Background(), "hi"))
		if err != nil {
			t.Fatal(err)
		}
		if got := doneStatus(t, events).Status; got != "done" {
			t.Fatalf("status = %q, want done", got)
		}
	})

	t.Run("failed", func(t *testing.T) {
		a, err := New(WithModel(taskFailModel{}))
		if err != nil {
			t.Fatal(err)
		}
		events, err := collectTaskEvents(t, a.Stream(context.Background(), "hi"))
		if err == nil {
			t.Fatal("expected the failing model to fail the run")
		}
		if got := doneStatus(t, events).Status; got != "failed" {
			t.Fatalf("status = %q, want failed", got)
		}
	})

	t.Run("paused", func(t *testing.T) {
		noop := tool.New("noop", "noop", func(*tool.Context, struct{}) (string, error) { return "ok", nil })
		a, err := New(
			WithModel(mock.New("m", func(req *llm.Request) *llm.Response {
				if _, ok := mock.LastToolResult(req); ok {
					return mock.Text("done")
				}
				return mock.CallTool("c1", "noop", `{}`)
			})),
			WithTools(noop),
			WithMiddleware(taskInterruptGate{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		events, err := collectTaskEvents(t, a.Stream(context.Background(), "hi"))
		if err != nil {
			t.Fatal(err)
		}
		if got := doneStatus(t, events).Status; got != "paused" {
			t.Fatalf("status = %q, want paused", got)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		model := &taskBlockingModel{started: make(chan struct{})}
		a, err := New(WithModel(model))
		if err != nil {
			t.Fatal(err)
		}
		run := a.Stream(context.Background(), "hi")
		collected := make(chan struct {
			events []core.Event
			err    error
		}, 1)
		go func() {
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
			collected <- struct {
				events []core.Event
				err    error
			}{events, iterErr}
		}()
		<-model.started
		run.Cancel()
		got := <-collected
		if got.err == nil {
			t.Fatal("expected the canceled run to fail")
		}
		if status := doneStatus(t, got.events).Status; status != "canceled" {
			t.Fatalf("status = %q, want canceled", status)
		}
	})
}
