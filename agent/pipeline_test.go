package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
)

// replier builds a leaf agent whose model always answers with a fixed string.
func replier(name, answer string) *agent.LLMAgent {
	return agent.New(agent.Config{
		Name:            name,
		DisableTransfer: true,
		Model: mock.New(name, func(*llm.Request) *llm.Response {
			return mock.Text(answer)
		}),
	})
}

// authorsOf runs the agent and collects the authors of the final assistant
// messages, in order.
func authorsOf(t *testing.T, root agent.Agent) []string {
	t.Helper()
	r := runner.New(runner.Config{Root: root})
	var authors []string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("go")) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.Message == nil || ev.Message.Role != core.RoleAssistant || ev.Partial {
			continue
		}
		authors = append(authors, ev.Author)
	}
	return authors
}

// TestPipelineRunsStagesInOrder verifies the builder produces an agent that runs
// its stages sequentially in the order they were added.
func TestPipelineRunsStagesInOrder(t *testing.T) {
	p := agent.Pipeline("etl").
		Then(replier("ingest", "a")).
		Then(replier("transform", "b")).
		Then(replier("load", "c")).
		Build()

	if got := p.Name(); got != "etl" {
		t.Fatalf("Name() = %q", got)
	}
	if got := len(p.SubAgents()); got != 3 {
		t.Fatalf("SubAgents() len = %d", got)
	}

	got := authorsOf(t, p)
	want := []string{"ingest", "transform", "load"}
	if len(got) != len(want) {
		t.Fatalf("authors = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("authors = %v, want %v", got, want)
		}
	}
}

// TestPipelineCompositeStages verifies ThenParallel and ThenLoop wrap their
// sub-agents in the right composite agents and run within the pipeline.
func TestPipelineCompositeStages(t *testing.T) {
	// A loop critic that escalates immediately, so the loop runs exactly once.
	approve := func() *agent.LLMAgent {
		return agent.New(agent.Config{
			Name:            "critic",
			DisableTransfer: true,
			Model: mock.New("critic", func(*llm.Request) *llm.Response {
				return mock.Text("ok") // no escalation needed; maxIter=1 bounds it
			}),
		})
	}

	p := agent.Pipeline("flow").
		Then(replier("plan", "x")).
		ThenParallel("gather", replier("g1", "1"), replier("g2", "2")).
		ThenLoop("review", 1, approve()).
		Build()

	stages := p.SubAgents()
	if len(stages) != 3 {
		t.Fatalf("stages = %d, want 3", len(stages))
	}
	if _, ok := stages[1].(*agent.ParallelAgent); !ok {
		t.Fatalf("stage 2 = %T, want *agent.ParallelAgent", stages[1])
	}
	if _, ok := stages[2].(*agent.LoopAgent); !ok {
		t.Fatalf("stage 3 = %T, want *agent.LoopAgent", stages[2])
	}

	// The parallel branch authors are nondeterministic in order, but all five
	// authors (plan, g1, g2, critic) must appear, with plan first.
	got := authorsOf(t, p)
	if len(got) == 0 || got[0] != "plan" {
		t.Fatalf("authors = %v, want plan first", got)
	}
	seen := map[string]bool{}
	for _, a := range got {
		seen[a] = true
	}
	for _, want := range []string{"plan", "g1", "g2", "critic"} {
		if !seen[want] {
			t.Fatalf("authors %v missing %q", got, want)
		}
	}
}
