package agent

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestTaskBudgetDefaults(t *testing.T) {
	model := mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") })

	a, err := New(WithModel(model), WithTaskBudget(TaskBudget{}))
	if err != nil {
		t.Fatal(err)
	}
	want := TaskBudget{MaxDepth: 8, MaxUnits: 64}
	if got := a.cfg.taskBudget; got != want {
		t.Fatalf("normalized task budget = %+v, want %+v", got, want)
	}
	run := a.Stream(context.Background(), "hi")
	if got := run.rc.task.budget; got != want {
		t.Fatalf("root task runtime budget = %+v, want %+v", got, want)
	}
	run.Cancel()

	cases := map[string]TaskBudget{
		"max depth": {MaxDepth: -1},
		"max units": {MaxUnits: -1},
		"max turns": {MaxTurns: -1},
	}
	for name, budget := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := New(WithModel(model), WithTaskBudget(budget))
			if err == nil {
				t.Fatalf("New accepted a negative %s budget: %+v", name, budget)
			}
			if !strings.Contains(err.Error(), "task budget") {
				t.Fatalf("error = %v, want a task budget error", err)
			}
		})
	}
}

func TestWithTaskParentCarrier(t *testing.T) {
	parent := &RunContext{
		Context:  context.Background(),
		ThreadID: "parent-thread",
		State:    &core.State{},
		durable:  newDurableRunState(),
	}
	tree := parent.ensureTask()
	child := TaskInfo{ID: "task_child", ParentID: tree.rootID, Kind: TaskKindChat, Depth: 1}

	cfg := resolveRunConfig("hi", "child-thread", withTaskParent(parent, child))
	if cfg.childTask == nil {
		t.Fatal("withTaskParent left childTask unset")
	}
	if *cfg.childTask != child {
		t.Fatalf("childTask = %+v, want %+v", *cfg.childTask, child)
	}
	if cfg.taskTree != tree {
		t.Fatal("withTaskParent did not carry the parent's task runtime")
	}

	carrier := runContextCarrierCtx{Context: keepToolUpdates(context.Background(), parent), rc: parent}
	if got := runContextOf(carrier); got != parent {
		t.Fatalf("runContextOf(carrier) = %p, want the parent %p", got, parent)
	}
	if got := runContextOf(context.Background()); got != nil {
		t.Fatalf("runContextOf(plain context) = %p, want nil", got)
	}
}

// runContextCapture records the first RunContext the model loop runs under.
type runContextCapture struct {
	BaseMiddleware

	mu sync.Mutex
	rc *RunContext
}

func (c *runContextCapture) BeforeModel(lc *LoopContext) (core.Directive, error) {
	c.mu.Lock()
	if c.rc == nil {
		c.rc = lc.RunContext
	}
	c.mu.Unlock()
	return core.Directive{}, nil
}

func (c *runContextCapture) captured() *RunContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rc
}

func TestAsToolWithParentAdmitsChild(t *testing.T) {
	capture := &runContextCapture{}
	child, err := New(
		WithModel(mock.New("child", func(*llm.Request) *llm.Response { return mock.Text("child done") })),
		WithMiddleware(capture),
	)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := New(
		WithModel(mock.New("parent", func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text("parent done")
			}
			return mock.CallTool("c1", "delegate", `{"task":"do the work"}`)
		})),
		WithTools(AsTool(child, "delegate", "delegate work")),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := parent.Stream(context.Background(), "go", OnThread("task-parent-thread"))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	parentTask := run.Task()

	childRC := capture.captured()
	if childRC == nil {
		t.Fatal("the child run never reached its model")
	}
	info := childRC.taskInfo()
	if info.ParentID != parentTask.ID {
		t.Fatalf("child parent = %q, want the parent task %q", info.ParentID, parentTask.ID)
	}
	if info.Depth != parentTask.Depth+1 {
		t.Fatalf("child depth = %d, want %d", info.Depth, parentTask.Depth+1)
	}
	if info.ID == "" || info.ID == parentTask.ID {
		t.Fatalf("child task id = %q, want a new id", info.ID)
	}
	if info.Kind != TaskKindChat {
		t.Fatalf("child kind = %q, want %q", info.Kind, TaskKindChat)
	}
	if info.ThreadID == "" || info.ThreadID == parentTask.ThreadID {
		t.Fatalf("child thread = %q, want its own ephemeral thread (parent %q)", info.ThreadID, parentTask.ThreadID)
	}
	if info.TurnID == "" {
		t.Fatal("child task has no turn id after the run")
	}
	if childRC.task != run.rc.task {
		t.Fatal("the child run did not share the parent's task runtime")
	}
}
