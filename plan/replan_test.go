package plan_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/plan"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

func TestReplanOnFailure(t *testing.T) {
	badTool := tool.New("bad", "always fails", func(ctx *tool.Context, _ struct{}) (string, error) {
		return "", errors.New("boom")
	})
	goodTool := tool.New("good", "succeeds", func(ctx *tool.Context, _ struct{}) (string, error) {
		return "ok", nil
	})

	// Round 1 plan uses the failing tool; round 2 (replan) uses the good tool.
	bad := `{"id":"rp","goal":"replan","steps":[{"id":"a","name":"A","executor":{"type":"tool","name":"bad","args":{}}}]}`
	good := `{"id":"rp","goal":"replan","steps":[{"id":"a","name":"A","executor":{"type":"tool","name":"good","args":{}}}]}`

	// Each planner invocation makes exactly two model calls: one to call set_plan,
	// one to reply after the tool result. Drive off a call counter so the decision
	// is independent of the planner's (persistent) message history. Round 1 emits
	// a plan using the failing tool; the replan (round 2) uses the good tool.
	var calls atomic.Int32
	planner := agent.New(agent.Config{
		Name: "planner", Description: "replanning planner",
		Tools:           []tool.Tool{plan.SetPlanTool()},
		DisableTransfer: true,
		Model: mock.New("planner", func(req *llm.Request) *llm.Response {
			switch calls.Add(1) {
			case 1:
				return mock.CallTool("p1", "set_plan", bad)
			case 3:
				return mock.CallTool("p2", "set_plan", good)
			default:
				return mock.Text("已登记")
			}
		}),
	})

	pa := plan.New(plan.Config{
		Name:       "rp-agent",
		Planner:    planner,
		Tools:      []tool.Tool{badTool, goodTool},
		MaxReplans: 1,
	})
	r := runner.New(runner.Config{AppName: "t", Root: pa, Store: session.InMemory()})

	var lastA string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("go")) {
		if err != nil {
			t.Fatalf("run error: %v", err)
		}
		if ev != nil && !ev.Partial && ev.Progress != nil && ev.Progress.JobID == "a" {
			lastA = ev.Progress.Status
		}
	}

	if lastA != string(plan.Done) {
		t.Fatalf("final status of step a = %q, want completed (replan should have fixed it)", lastA)
	}
	if calls.Load() != 4 {
		t.Errorf("planner made %d model calls, want 4 (2 per planning round × 2 rounds)", calls.Load())
	}
}
