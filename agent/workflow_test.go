package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// sayer is an agent that always replies with a fixed phrase.
func sayer(t *testing.T, phrase string) *agent.Agent {
	t.Helper()
	a, err := agent.New(agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
		return mock.Text(phrase)
	})))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestSequentialRunsInOrder(t *testing.T) {
	flow := agent.Sequential("seq", sayer(t, "alpha"), sayer(t, "beta"), sayer(t, "gamma"))
	out, err := flow.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	// Sequential returns the last stage's result.
	if out != "gamma" {
		t.Fatalf("sequential result = %q, want gamma", out)
	}
}

func TestParallelMergesBranches(t *testing.T) {
	flow := agent.Parallel("par", sayer(t, "one"), sayer(t, "two"), sayer(t, "three"))
	out, err := flow.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"one", "two", "three"} {
		if !strings.Contains(out, want) {
			t.Fatalf("parallel result %q missing %q", out, want)
		}
	}
}

func TestLoopStopsOnEscalate(t *testing.T) {
	// A critic that calls exit_loop on the 2nd pass; counts passes via history.
	calls := 0
	critic, err := agent.New(
		agent.WithModel(mock.New("m", func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text("revised")
			}
			calls++
			if calls >= 2 {
				return mock.CallTool("c1", "exit_loop", `{"reason":"good enough"}`)
			}
			return mock.Text("needs work")
		})),
		agent.WithTools(agent.ExitLoopTool()),
	)
	if err != nil {
		t.Fatal(err)
	}
	flow := agent.Loop("refine", 10, critic)
	if _, err := flow.Run(context.Background(), "draft"); err != nil {
		t.Fatal(err)
	}
	if calls < 2 {
		t.Fatalf("loop exited too early (calls=%d)", calls)
	}
	if calls > 3 {
		t.Fatalf("loop did not stop on escalate (calls=%d)", calls)
	}
}

func TestPipelineBuilds(t *testing.T) {
	pipe := agent.NewPipeline("p").
		Then(sayer(t, "plan")).
		ThenParallel("gather", sayer(t, "web"), sayer(t, "papers")).
		Then(sayer(t, "final")).
		Build()
	out, err := pipe.Run(context.Background(), "topic")
	if err != nil {
		t.Fatal(err)
	}
	if out != "final" {
		t.Fatalf("pipeline result = %q, want final", out)
	}
}

// ensure tool import is used (ExitLoopTool returns tool.Tool).
var _ tool.Tool = agent.ExitLoopTool()
var _ = core.Continue
