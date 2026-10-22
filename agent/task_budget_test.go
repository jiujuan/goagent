package agent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

func budgetTestEchoTool() tool.Tool {
	return tool.New("echo", "echo", func(*tool.Context, struct{}) (string, error) {
		return "ok", nil
	})
}

func TestAsToolBudgetReturnsToolResult(t *testing.T) {
	var childCalls atomic.Int32
	child, err := agent.New(agent.WithModel(mock.New("child", func(*llm.Request) *llm.Response {
		childCalls.Add(1)
		return mock.Text("child done")
	})))
	if err != nil {
		t.Fatal(err)
	}

	var parentCalls atomic.Int32
	parent, err := agent.New(
		agent.WithModel(mock.New("parent", func(req *llm.Request) *llm.Response {
			n := parentCalls.Add(1)
			if n <= 2 {
				return mock.CallTool(fmt.Sprintf("call-%d", n), "delegate", `{"task":"work"}`)
			}
			tr, ok := mock.LastToolResult(req)
			if !ok {
				return mock.Text("parent saw no tool result")
			}
			return mock.Text("parent saw: " + toolResultText(tr))
		})),
		agent.WithTools(agent.AsTool(child, "delegate", "delegate work")),
		agent.WithTaskBudget(agent.TaskBudget{MaxUnits: 1}),
	)
	if err != nil {
		t.Fatal(err)
	}

	answer, err := parent.Run(context.Background(), "go")
	if err != nil {
		t.Fatalf("parent run failed: %v", err)
	}
	if got := childCalls.Load(); got != 1 {
		t.Fatalf("child model calls = %d, want 1", got)
	}
	if !strings.Contains(answer, "task budget exceeded") {
		t.Fatalf("parent answer = %q, want the budget error text", answer)
	}
	if !strings.Contains(answer, "units") {
		t.Fatalf("parent answer = %q, want the units dimension", answer)
	}
}

func TestTaskBudgetTurns(t *testing.T) {
	var calls atomic.Int32
	a, err := agent.New(
		agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
			n := calls.Add(1)
			return mock.CallTool(fmt.Sprintf("call-%d", n), "echo", `{}`)
		})),
		agent.WithTools(budgetTestEchoTool()),
		agent.WithTaskBudget(agent.TaskBudget{MaxTurns: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "go")
	if _, err := run.Wait(); err == nil {
		t.Fatal("run succeeded despite the task turn budget")
	} else {
		var be *agent.TaskBudgetError
		if !errors.As(err, &be) {
			t.Fatalf("run error = %v, want *agent.TaskBudgetError", err)
		}
		if be.Dimension != "turns" || be.Limit != 2 || be.Used != 2 {
			t.Fatalf("turn budget error = %+v, want turns limit 2 used 2", be)
		}
		if be.ParentID != run.Task().ID || be.Kind != agent.TaskKindChat {
			t.Fatalf("turn budget error identity = %+v, want task %q kind %q", be, run.Task().ID, agent.TaskKindChat)
		}
		if !errors.Is(err, agent.ErrTaskBudgetExceeded) {
			t.Fatal("errors.Is(run error, agent.ErrTaskBudgetExceeded) = false")
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("model calls = %d, want 2", got)
	}
}

func TestTaskBudgetVsRunBudget(t *testing.T) {
	t.Run("tree cap is a hard error", func(t *testing.T) {
		var calls atomic.Int32
		a, err := agent.New(
			agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
				calls.Add(1)
				return mock.CallTool("call-1", "echo", `{}`)
			})),
			agent.WithTools(budgetTestEchoTool()),
			agent.WithTaskBudget(agent.TaskBudget{MaxTurns: 1}),
			agent.WithMiddleware(middleware.RunBudget(middleware.RunBudgetOptions{MaxTurns: 5})),
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = a.Run(context.Background(), "go")
		if err == nil {
			t.Fatal("run succeeded despite the tree turn cap")
		}
		var be *agent.TaskBudgetError
		if !errors.As(err, &be) || be.Dimension != "turns" {
			t.Fatalf("run error = %v, want a turns *agent.TaskBudgetError", err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("model calls = %d, want 1", got)
		}
	})

	t.Run("run budget wraps up", func(t *testing.T) {
		var calls atomic.Int32
		a, err := agent.New(
			agent.WithModel(mock.New("m", func(req *llm.Request) *llm.Response {
				calls.Add(1)
				if len(req.Tools) == 0 {
					return mock.Text("wrap-up answer")
				}
				return mock.CallTool("call-1", "echo", `{}`)
			})),
			agent.WithTools(budgetTestEchoTool()),
			agent.WithTaskBudget(agent.TaskBudget{MaxTurns: 5}),
			agent.WithMiddleware(middleware.RunBudget(middleware.RunBudgetOptions{MaxTurns: 1})),
		)
		if err != nil {
			t.Fatal(err)
		}

		answer, err := a.Run(context.Background(), "go")
		if err != nil {
			t.Fatalf("run failed instead of wrapping up: %v", err)
		}
		if answer != "wrap-up answer" {
			t.Fatalf("answer = %q, want the wrap-up text", answer)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("model calls = %d, want 2", got)
		}
	})
}
