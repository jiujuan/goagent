package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestLLMPlanGeneratesAndExecutes(t *testing.T) {
	w := echoWorker(t)
	// Planner returns a 3-node DAG (a, b parallel → c).
	planner, err := agent.New(agent.WithModel(mock.New("planner", func(*llm.Request) *llm.Response {
		return mock.Text(`{"nodes":[
			{"id":"a","task":"RA"},
			{"id":"b","task":"RB"},
			{"id":"c","task":"merge {{a}} {{b}}","depends_on":["a","b"]}
		]}`)
	})))
	if err != nil {
		t.Fatal(err)
	}

	pa := agent.NewLLMPlan("auto", planner, agent.WithWorker(w))
	out, err := pa.Run(context.Background(), "anything")
	if err != nil {
		t.Fatal(err)
	}
	if out != "merge RA RB" {
		t.Fatalf("LLM-planned result = %q, want %q", out, "merge RA RB")
	}
}

func TestLLMPlanRetriesOnInvalidThenSucceeds(t *testing.T) {
	w := echoWorker(t)
	calls := 0
	planner, _ := agent.New(agent.WithModel(mock.New("planner", func(*llm.Request) *llm.Response {
		calls++
		if calls == 1 {
			// invalid: depends on a non-existent node
			return mock.Text(`{"nodes":[{"id":"x","task":"t","depends_on":["ghost"]}]}`)
		}
		return mock.Text(`{"nodes":[{"id":"x","task":"OK"}]}`)
	})))
	pa := agent.NewLLMPlan("retry", planner, agent.WithWorker(w))
	out, err := pa.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	if out != "OK" || calls != 2 {
		t.Fatalf("out=%q calls=%d, want OK/2 (retried after invalid)", out, calls)
	}
}

func TestLLMPlanFailsAfterMaxAttempts(t *testing.T) {
	w := echoWorker(t)
	planner, _ := agent.New(agent.WithModel(mock.New("planner", func(*llm.Request) *llm.Response {
		return mock.Text("not json at all")
	})))
	pa := agent.NewLLMPlan("bad", planner, agent.WithWorker(w), agent.WithMaxPlanAttempts(2))
	if _, err := pa.Run(context.Background(), "go"); err == nil {
		t.Fatal("expected error when planner never produces a valid DAG")
	}
}

func TestLLMPlanEmptyFallsBackToSingleNode(t *testing.T) {
	w := echoWorker(t)
	// Planner says no decomposition needed → executor runs the worker on {{input}}.
	planner, _ := agent.New(agent.WithModel(mock.New("planner", func(*llm.Request) *llm.Response {
		return mock.Text(`{"nodes":[]}`)
	})))
	pa := agent.NewLLMPlan("single", planner, agent.WithWorker(w))
	out, err := pa.Run(context.Background(), "直接回答这个")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "直接回答这个") { // echo worker returns its input ({{input}})
		t.Fatalf("single-node fallback result = %q", out)
	}
}
