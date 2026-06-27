package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestTransferDelegatesToSubAgent(t *testing.T) {
	// Expert answers directly.
	expert, err := agent.New(
		agent.WithName("math_expert"),
		agent.WithDescription("solves math"),
		agent.WithModel(mock.New("e", func(*llm.Request) *llm.Response {
			return mock.Text("42")
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Router: on first turn it transfers; the delegate's reply becomes the run's.
	router, err := agent.New(
		agent.WithName("router"),
		agent.WithModel(mock.New("r", func(req *llm.Request) *llm.Response {
			// Only the router should ever be asked to transfer; the expert's
			// model never calls the tool.
			return mock.CallTool("t1", "transfer_to_agent", `{"agent":"math_expert"}`)
		})),
		agent.WithSubAgents(expert),
	)
	if err != nil {
		t.Fatal(err)
	}

	out, err := router.Run(context.Background(), "what is 6*7?")
	if err != nil {
		t.Fatal(err)
	}
	if out != "42" {
		t.Fatalf("delegated answer = %q, want 42", out)
	}
}

func TestTransferToolAdvertisedOnlyWithSubAgents(t *testing.T) {
	// Without sub-agents: model sees no transfer tool, answers directly.
	plain, _ := agent.New(agent.WithModel(mock.New("m", func(req *llm.Request) *llm.Response {
		for _, ts := range req.Tools {
			if ts.Name == "transfer_to_agent" {
				t.Fatal("transfer tool advertised without sub-agents")
			}
		}
		return mock.Text("ok")
	})))
	if _, err := plain.Run(context.Background(), "hi"); err != nil {
		t.Fatal(err)
	}

	// With a sub-agent: the tool is advertised and enumerates the target.
	child, _ := agent.New(agent.WithName("child"), agent.WithModel(mock.New("c", func(*llm.Request) *llm.Response {
		return mock.Text("child")
	})))
	parent, _ := agent.New(
		agent.WithModel(mock.New("p", func(req *llm.Request) *llm.Response {
			var found bool
			for _, ts := range req.Tools {
				if ts.Name == "transfer_to_agent" {
					found = true
					if !strings.Contains(string(ts.Parameters), "child") {
						t.Fatalf("transfer schema missing target: %s", ts.Parameters)
					}
				}
			}
			if !found {
				t.Fatal("transfer tool not advertised with sub-agents")
			}
			return mock.Text("done")
		})),
		agent.WithSubAgents(child),
	)
	if _, err := parent.Run(context.Background(), "hi"); err != nil {
		t.Fatal(err)
	}
}
