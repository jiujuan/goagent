package agent_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestPlanReplanAddsAndExecutes(t *testing.T) {
	w := echoWorker(t)

	// Replanner: round 1 adds an "extra" node fed by base; round 2 says done.
	calls := 0
	replanner, err := agent.New(agent.WithModel(mock.New("rp", func(*llm.Request) *llm.Response {
		calls++
		if calls == 1 {
			// fenced JSON to also exercise the tolerant parser
			return mock.Text("```json\n{\"nodes\":[{\"id\":\"extra\",\"task\":\"E from {{base}}\",\"depends_on\":[\"base\"]}],\"done\":false}\n```")
		}
		return mock.Text(`{"done":true}`)
	})))
	if err != nil {
		t.Fatal(err)
	}

	plan := agent.Plan{Nodes: []agent.Node{{ID: "base", Task: "B"}}}
	pa := agent.NewPlan("replan", plan, agent.WithWorker(w), agent.WithReplanner(replanner))

	out, err := pa.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	// extra is the new leaf; it ran with base's output threaded via {{base}}.
	if out != "E from B" {
		t.Fatalf("replanned result = %q, want %q", out, "E from B")
	}
	if calls != 2 {
		t.Fatalf("replanner called %d times, want 2 (extend then done)", calls)
	}
}

func TestPlanReplanBounded(t *testing.T) {
	w := echoWorker(t)
	// Replanner never says done — must be bounded by WithMaxReplanRounds.
	calls := 0
	replanner, _ := agent.New(agent.WithModel(mock.New("rp", func(*llm.Request) *llm.Response {
		calls++
		return mock.Text(fmt.Sprintf(`{"nodes":[{"id":"n%d","task":"x"}],"done":false}`, calls))
	})))
	plan := agent.Plan{Nodes: []agent.Node{{ID: "base", Task: "B"}}}
	pa := agent.NewPlan("bound", plan,
		agent.WithWorker(w), agent.WithReplanner(replanner), agent.WithMaxReplanRounds(2))

	if _, err := pa.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("replanner called %d times, want exactly 2 (bounded)", calls)
	}
}

func TestPlanReplanInvalidIgnored(t *testing.T) {
	w := echoWorker(t)
	// Replanner proposes a node depending on a non-existent node → invalid → ignored.
	replanner, _ := agent.New(agent.WithModel(mock.New("rp", func(*llm.Request) *llm.Response {
		return mock.Text(`{"nodes":[{"id":"x","task":"t","depends_on":["ghost"]}],"done":false}`)
	})))
	plan := agent.Plan{Nodes: []agent.Node{{ID: "base", Task: "B"}}}
	pa := agent.NewPlan("inv", plan, agent.WithWorker(w), agent.WithReplanner(replanner))

	out, err := pa.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	if out != "B" { // invalid delta ignored → finishes with base only
		t.Fatalf("result = %q, want B (invalid replan ignored)", out)
	}
}
