package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestRunTaskRootIdentity(t *testing.T) {
	store := checkpoint.NewMemory()
	a, err := agent.New(
		agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") })),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	const threadID = "task-root-identity"
	run := a.Stream(context.Background(), "hi", agent.OnThread(threadID))
	before := run.Task()
	if !strings.HasPrefix(before.ID, "task_") {
		t.Fatalf("root task id = %q, want a task_ prefix", before.ID)
	}
	if before.ParentID != "" {
		t.Fatalf("root task parent = %q, want empty", before.ParentID)
	}
	if before.ThreadID != threadID {
		t.Fatalf("root task thread = %q, want %q", before.ThreadID, threadID)
	}
	if before.Kind != agent.TaskKindChat {
		t.Fatalf("root task kind = %q, want %q", before.Kind, agent.TaskKindChat)
	}
	if before.Depth != 0 {
		t.Fatalf("root task depth = %d, want 0", before.Depth)
	}

	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	after := run.Task()
	if after.ID != before.ID {
		t.Fatalf("root task id changed after the run: %q -> %q", before.ID, after.ID)
	}
	if after.TurnID == "" {
		t.Fatal("root task turn id is empty after the run")
	}
	cp, err := store.Latest(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || cp.TurnID == "" {
		t.Fatalf("head checkpoint = %+v, want one carrying a turn id", cp)
	}
	if after.TurnID != cp.TurnID {
		t.Fatalf("root task turn = %q, want the head checkpoint turn %q", after.TurnID, cp.TurnID)
	}
}

func TestTaskKindForWorkflowAndPlan(t *testing.T) {
	t.Run("workflow", func(t *testing.T) {
		flow := agent.Parallel("wf-kind", sayer(t, "branch done"))
		run := flow.Stream(context.Background(), "go", agent.OnThread("task-kind-workflow"))
		if got := run.Task().Kind; got != agent.TaskKindWorkflow {
			t.Fatalf("workflow root kind = %q, want %q", got, agent.TaskKindWorkflow)
		}
		if _, err := run.Wait(); err != nil {
			t.Fatal(err)
		}
		if got := run.Task().Kind; got != agent.TaskKindWorkflow {
			t.Fatalf("workflow root kind after run = %q, want %q", got, agent.TaskKindWorkflow)
		}
	})

	t.Run("plan", func(t *testing.T) {
		worker := sayer(t, "node done")
		plan := agent.NewPlan("plan-kind", agent.Plan{Nodes: []agent.Node{
			{ID: "n1", Task: "do the work", Worker: worker},
		}})
		run := plan.Stream(context.Background(), "go", agent.OnThread("task-kind-plan"))
		if got := run.Task().Kind; got != agent.TaskKindPlan {
			t.Fatalf("plan root kind = %q, want %q", got, agent.TaskKindPlan)
		}
		if _, err := run.Wait(); err != nil {
			t.Fatal(err)
		}
		if got := run.Task().Kind; got != agent.TaskKindPlan {
			t.Fatalf("plan root kind after run = %q, want %q", got, agent.TaskKindPlan)
		}
	})
}

func TestTaskBudgetDoesNotAffectRunWithoutExplicitOption(t *testing.T) {
	a := sayer(t, "plain answer")
	out, err := a.Run(context.Background(), "hi")
	if err != nil {
		t.Fatal(err)
	}
	if out != "plain answer" {
		t.Fatalf("run result = %q, want %q", out, "plain answer")
	}
}
