package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
)

func derivedParent() *RunContext {
	rc := testRunContext(&core.State{})
	rc.ThreadID = "thread-parent"
	rc.ensureTask()
	rc.info = TaskInfo{
		ID:       "task_parent",
		ParentID: "task_grandparent",
		ThreadID: "thread-parent",
		TurnID:   "turn-parent",
		Kind:     TaskKindWorkflow,
		Depth:    2,
	}
	return rc
}

func TestSubRunDerivesChildTask(t *testing.T) {
	rc := derivedParent()
	tree := rc.task

	child, err := rc.subRun(core.UserText("task"), TaskKindPlan)
	if err != nil {
		t.Fatal(err)
	}
	if child.task != tree {
		t.Fatal("subRun did not share the parent's task runtime")
	}
	info := child.taskInfo()
	if info.ID == "" || !strings.HasPrefix(info.ID, "task_") {
		t.Fatalf("child task id = %q, want a new task_ id", info.ID)
	}
	if info.ID == "task_parent" {
		t.Fatal("subRun reused the parent task id")
	}
	if info.ParentID != "task_parent" {
		t.Fatalf("child parent = %q, want task_parent", info.ParentID)
	}
	if info.Depth != 3 {
		t.Fatalf("child depth = %d, want 3", info.Depth)
	}
	if info.ThreadID != "thread-parent" || info.TurnID != "turn-parent" {
		t.Fatalf("child thread/turn = %q/%q, want the parent's", info.ThreadID, info.TurnID)
	}
	if info.Kind != TaskKindPlan {
		t.Fatalf("child kind = %q, want %q", info.Kind, TaskKindPlan)
	}
}

func TestForBranchDerivesChildTask(t *testing.T) {
	rc := derivedParent()
	tree := rc.task

	child, err := rc.forBranch("branch-a", TaskKindChat)
	if err != nil {
		t.Fatal(err)
	}
	if child.task != tree {
		t.Fatal("forBranch did not share the parent's task runtime")
	}
	if child.branch != "branch-a" {
		t.Fatalf("branch label = %q, want branch-a", child.branch)
	}
	info := child.taskInfo()
	if info.ID == "" || info.ID == "task_parent" || !strings.HasPrefix(info.ID, "task_") {
		t.Fatalf("branch task id = %q, want a new task_ id", info.ID)
	}
	if info.ParentID != "task_parent" {
		t.Fatalf("branch parent = %q, want task_parent", info.ParentID)
	}
	if info.Depth != 3 {
		t.Fatalf("branch depth = %d, want 3", info.Depth)
	}
	if info.ThreadID != "thread-parent" || info.TurnID != "turn-parent" {
		t.Fatalf("branch thread/turn = %q/%q, want the parent's", info.ThreadID, info.TurnID)
	}
	if info.Kind != TaskKindChat {
		t.Fatalf("branch kind = %q, want %q", info.Kind, TaskKindChat)
	}
}

func TestTransferKeepsTaskIdentity(t *testing.T) {
	rc := derivedParent()
	deep := rc.deeper()

	if deep.task != rc.task {
		t.Fatal("deeper did not share the parent's task runtime")
	}
	info := deep.taskInfo()
	if info.ID != "task_parent" || info.ParentID != "task_grandparent" || info.Depth != 2 {
		t.Fatalf("transfer task = %+v, want the parent's identity unchanged", info)
	}
	if deep.transferDepth != rc.transferDepth+1 {
		t.Fatalf("transfer depth = %d, want %d", deep.transferDepth, rc.transferDepth+1)
	}
}

func TestSubRunWithoutTaskCreatesRoot(t *testing.T) {
	rc := &RunContext{
		Context:  context.Background(),
		ThreadID: "thread-rootless",
		State:    &core.State{},
		durable:  newDurableRunState(),
	}

	child, err := rc.subRun(core.UserText("task"), TaskKindChat)
	if err != nil {
		t.Fatal(err)
	}
	parent := rc.taskInfo()
	if parent.ID == "" || !strings.HasPrefix(parent.ID, "task_") {
		t.Fatalf("root task id = %q, want a generated task_ id", parent.ID)
	}
	if parent.Kind != TaskKindChat {
		t.Fatalf("root task kind = %q, want %q", parent.Kind, TaskKindChat)
	}
	if parent.Depth != 0 {
		t.Fatalf("root task depth = %d, want 0", parent.Depth)
	}
	if child.task == nil || child.task != rc.task {
		t.Fatal("derived child did not share the generated root task runtime")
	}
	info := child.taskInfo()
	if info.ParentID != parent.ID {
		t.Fatalf("child parent = %q, want the generated root %q", info.ParentID, parent.ID)
	}
	if info.Depth != 1 {
		t.Fatalf("child depth = %d, want 1", info.Depth)
	}
	if info.ThreadID != "thread-rootless" {
		t.Fatalf("child thread = %q, want thread-rootless", info.ThreadID)
	}
}
