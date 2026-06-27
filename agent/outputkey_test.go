package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestOutputKeyFlowsToNextStage(t *testing.T) {
	// Stage 1 produces a "plan" and stores it under the output key.
	planner, err := agent.New(
		agent.WithModel(mock.New("p1", func(*llm.Request) *llm.Response {
			return mock.Text("STEP-ALPHA")
		})),
		agent.WithOutputKey("plan"),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Stage 2's instruction references {{plan}}; assert the rendered system
	// prompt contains stage 1's output.
	var sawPlan bool
	writer, err := agent.New(
		agent.WithInstruction("Write based on this plan: {{plan}}"),
		agent.WithModel(mock.New("p2", func(req *llm.Request) *llm.Response {
			if strings.Contains(req.System, "STEP-ALPHA") {
				sawPlan = true
			}
			return mock.Text("written")
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	flow := agent.Sequential("pipe", planner, writer)
	if _, err := flow.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if !sawPlan {
		t.Fatal("stage 2 did not receive stage 1's output via {{plan}}")
	}
}
