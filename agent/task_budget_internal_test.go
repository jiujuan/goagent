package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// TestTaskBudgetDepth 从根开始连续派生：第 1..8 层成功，第 9 层被深度上限
// 拒绝，且拒绝发生在扣子任务额度之前。
func TestTaskBudgetDepth(t *testing.T) {
	root := &RunContext{Context: context.Background(), ThreadID: "budget-depth-thread"}
	root.ensureTask()

	at := root
	for depth := 1; depth <= 8; depth++ {
		child, tree, err := at.deriveChildTask(TaskKindChat)
		if err != nil {
			t.Fatalf("depth %d: %v", depth, err)
		}
		if child.Depth != depth {
			t.Fatalf("depth %d child depth = %d", depth, child.Depth)
		}
		if child.ParentID != at.taskInfo().ID {
			t.Fatalf("depth %d parent = %q, want %q", depth, child.ParentID, at.taskInfo().ID)
		}
		at = &RunContext{Context: context.Background(), ThreadID: "budget-depth-thread", task: tree, info: child}
	}

	parent := at.taskInfo()
	_, _, err := at.deriveChildTask(TaskKindChat)
	if err == nil {
		t.Fatal("depth 9 was admitted under the default depth limit 8")
	}
	var be *TaskBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("depth 9 error = %v, want *TaskBudgetError", err)
	}
	if be.Dimension != "depth" || be.Limit != 8 || be.Used != 9 {
		t.Fatalf("depth error = %+v, want depth limit 8 used 9", be)
	}
	if be.ParentID != parent.ID || be.Kind != TaskKindChat {
		t.Fatalf("depth error identity = %+v, want parent %q kind %q", be, parent.ID, TaskKindChat)
	}
	if !errors.Is(err, ErrTaskBudgetExceeded) {
		t.Fatalf("errors.Is(depth error, ErrTaskBudgetExceeded) = false")
	}
	if got := at.task.units.Load(); got != 8 {
		t.Fatalf("units after the depth rejection = %d, want 8", got)
	}
}

func TestTaskBudgetUnitsCAS(t *testing.T) {
	tree := &taskRuntime{rootID: "task_root", budget: TaskBudget{MaxDepth: 8, MaxUnits: 5}}
	rc := &RunContext{
		Context: context.Background(),
		task:    tree,
		info:    TaskInfo{ID: "task_root", Kind: TaskKindChat},
	}

	const contenders = 20
	var (
		wg       sync.WaitGroup
		admitted atomic.Int32
		denied   atomic.Int32
		wrongErr atomic.Int32
	)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := rc.admitChild(TaskKindChat); err != nil {
				denied.Add(1)
				var be *TaskBudgetError
				if !errors.As(err, &be) || be.Dimension != "units" {
					wrongErr.Add(1)
				}
				return
			}
			admitted.Add(1)
		}()
	}
	wg.Wait()

	if got := admitted.Load(); got != 5 {
		t.Fatalf("admitted children = %d, want 5", got)
	}
	if got := denied.Load(); got != contenders-5 {
		t.Fatalf("denied children = %d, want %d", got, contenders-5)
	}
	if got := wrongErr.Load(); got != 0 {
		t.Fatalf("%d denials were not units budget errors", got)
	}
	if got := tree.units.Load(); got != 5 {
		t.Fatalf("units = %d, want exactly 5", got)
	}
	if got := tree.units.Load(); got > int64(tree.budget.MaxUnits) {
		t.Fatalf("units = %d exceeded MaxUnits %d", got, tree.budget.MaxUnits)
	}
}

func TestConsumeTurnCountsEveryAttempt(t *testing.T) {
	tree := &taskRuntime{rootID: "task_root", budget: TaskBudget{MaxTurns: 2}}
	rc := &RunContext{
		Context: context.Background(),
		task:    tree,
		info:    TaskInfo{ID: "task_root", Kind: TaskKindChat},
	}

	for i := 0; i < 2; i++ {
		if err := rc.consumeTurn(); err != nil {
			t.Fatalf("turn %d: %v", i+1, err)
		}
	}
	err := rc.consumeTurn()
	if err == nil {
		t.Fatal("turn 3 was admitted under MaxTurns 2")
	}
	var be *TaskBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("turn 3 error = %v, want *TaskBudgetError", err)
	}
	if be.Dimension != "turns" || be.Limit != 2 || be.Used != 2 {
		t.Fatalf("turns error = %+v, want turns limit 2 used 2", be)
	}
	if be.ParentID != "task_root" || be.Kind != TaskKindChat {
		t.Fatalf("turns error identity = %+v, want task_root/chat", be)
	}
	if !errors.Is(err, ErrTaskBudgetExceeded) {
		t.Fatal("errors.Is(turns error, ErrTaskBudgetExceeded) = false")
	}
	if got := tree.turns.Load(); got != 2 {
		t.Fatalf("turns = %d, want 2", got)
	}

	unlimited := &taskRuntime{rootID: "task_unlimited", budget: TaskBudget{}}
	urc := &RunContext{Context: context.Background(), task: unlimited, info: TaskInfo{ID: "task_unlimited", Kind: TaskKindChat}}
	for i := 0; i < 100; i++ {
		if err := urc.consumeTurn(); err != nil {
			t.Fatalf("unlimited turn %d: %v", i+1, err)
		}
	}
	if got := unlimited.turns.Load(); got != 0 {
		t.Fatalf("unlimited tree counted %d turns, want 0", got)
	}
}

func TestParallelBudgetStopsAdmission(t *testing.T) {
	branch := func(name string) *Agent {
		a, err := New(WithModel(mock.New(name, func(*llm.Request) *llm.Response {
			return mock.Text(name)
		})))
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	wf := Parallel("budget-parallel", branch("a"), branch("b"), branch("c"))
	wf.cfg.taskBudget = TaskBudget{MaxUnits: 2}

	run := wf.Stream(context.Background(), "go")
	_, err := run.Wait()
	if err == nil {
		t.Fatal("parallel run succeeded despite a task budget error")
	}
	var be *TaskBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("parallel error = %v, want *TaskBudgetError", err)
	}
	if be.Dimension != "units" || be.Limit != 2 || be.Used != 2 {
		t.Fatalf("parallel budget error = %+v, want units limit 2 used 2", be)
	}
	if got := run.rc.task.units.Load(); got != 2 {
		t.Fatalf("admitted branch units = %d, want 2", got)
	}
	if got := run.rc.task.units.Load(); got > 2 {
		t.Fatalf("admitted branch units = %d exceeded MaxUnits 2", got)
	}
}

type budgetApprovalGate struct{ BaseMiddleware }

func (budgetApprovalGate) BeforeTool(lc *LoopContext, c *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(c) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

func budgetEchoTool() tool.Tool {
	return tool.New("echo", "echo", func(*tool.Context, struct{}) (string, error) {
		return "ok", nil
	})
}

func TestResumeStartsNewTaskTree(t *testing.T) {
	a, err := New(
		WithModel(mock.New("m", func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text("done")
			}
			return mock.CallTool("call-1", "echo", `{}`)
		})),
		WithTools(budgetEchoTool()),
		WithMiddleware(budgetApprovalGate{}),
		WithTaskBudget(TaskBudget{MaxTurns: 4}),
	)
	if err != nil {
		t.Fatal(err)
	}

	threadID := "budget-resume-tree"
	first := a.Stream(context.Background(), "go", OnThread(threadID))
	if _, err := first.Wait(); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if !first.Paused() {
		t.Fatal("first run did not pause")
	}
	root := first.Task()
	pausedTree := first.rc.task
	if pausedTree.turns.Load() == 0 {
		t.Fatal("the paused tree counted no model request")
	}

	resumed, err := a.Resume(context.Background(), threadID, Allow("call-1"))
	if err != nil {
		t.Fatal(err)
	}
	resumedRoot := resumed.Task()
	if resumedRoot.ID == root.ID {
		t.Fatalf("resumed root task id = %q, want a new id", resumedRoot.ID)
	}
	if resumed.rc.task == pausedTree {
		t.Fatal("resume reused the paused task tree")
	}
	if got := resumed.rc.task.turns.Load(); got != 0 {
		t.Fatalf("resumed tree turns = %d before driving, want 0", got)
	}
	if got := resumed.rc.task.units.Load(); got != 0 {
		t.Fatalf("resumed tree units = %d before driving, want 0", got)
	}
	if _, err := resumed.Wait(); err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got := resumed.rc.task.turns.Load(); got == 0 {
		t.Fatal("resumed tree never counted its model request")
	}
}

type budgetFailFirstModel struct {
	BaseMiddleware
	calls atomic.Int32
}

func (m *budgetFailFirstModel) BeforeModel(*LoopContext) (core.Directive, error) {
	if m.calls.Add(1) == 1 {
		return core.Directive{}, errors.New("worker failed")
	}
	return core.Directive{}, nil
}

func TestPlanBudgetErrorNotRetried(t *testing.T) {
	failing := &budgetFailFirstModel{}
	worker, err := New(
		WithModel(mock.New("worker", func(*llm.Request) *llm.Response { return mock.Text("worker done") })),
		WithMiddleware(failing),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := NewPlan("budget-plan", Plan{Nodes: []Node{{
		ID:         "n1",
		Task:       "work",
		Worker:     worker,
		MaxRetries: 2,
	}}})
	plan.cfg.taskBudget = TaskBudget{MaxUnits: 1}

	run := plan.Stream(context.Background(), "go")
	retries, failures := 0, 0
	for ev := range run.Iter() {
		done, ok := ev.(core.PlanNodeDone)
		if !ok || done.NodeID != "n1" {
			continue
		}
		switch done.Status {
		case "retry":
			retries++
		case "failed":
			failures++
		}
	}
	_, err = run.Wait()
	if err == nil {
		t.Fatal("plan run succeeded despite the task budget error")
	}
	if retries != 1 || failures != 1 {
		t.Fatalf("node outcomes = %d retry, %d failed; want 1 retry (the real failure) then 1 failed (the budget error)", retries, failures)
	}
	var be *TaskBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("plan error = %v, want *TaskBudgetError", err)
	}
	if be.Dimension != "units" || be.Limit != 1 || be.Used != 1 {
		t.Fatalf("plan budget error = %+v, want units limit 1 used 1", be)
	}
	if !errors.Is(err, ErrTaskBudgetExceeded) {
		t.Fatal("errors.Is(plan error, ErrTaskBudgetExceeded) = false")
	}
	if got := failing.calls.Load(); got != 1 {
		t.Fatalf("worker model calls = %d, want 1 (a budget error must not retry)", got)
	}
	if got := run.Task().Kind; got != TaskKindPlan {
		t.Fatalf("plan root kind = %q, want %q", got, TaskKindPlan)
	}
}
