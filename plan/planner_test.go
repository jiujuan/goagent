package plan_test

import (
	"context"
	"sync"
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

func TestDynamicPlanFromPlanner(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	mkTool := func(name string) tool.Tool {
		return tool.New(name, "capability "+name, func(ctx *tool.Context, _ struct{}) (string, error) {
			mu.Lock()
			ran = append(ran, name)
			mu.Unlock()
			return name + "-out", nil
		})
	}
	toolA, toolB := mkTool("tool_a"), mkTool("tool_b")

	// The planner emits a 2-step DAG: b depends on a.
	draftJSON := `{"id":"dyn","goal":"动态计划","steps":[
		{"id":"a","name":"A","depends_on":[],"executor":{"type":"tool","name":"tool_a","args":{}}},
		{"id":"b","name":"B","depends_on":["a"],"executor":{"type":"tool","name":"tool_b","args":{}}}
	]}`

	planner := agent.New(agent.Config{
		Name: "planner", Description: "produce a DAG",
		Tools:           []tool.Tool{plan.SetPlanTool()},
		DisableTransfer: true,
		Model: mock.New("planner", func(req *llm.Request) *llm.Response {
			if res, ok := mock.LastToolResult(req); ok && res.Name == "set_plan" {
				return mock.Text("✅ 计划已登记")
			}
			return mock.CallTool("p1", "set_plan", draftJSON)
		}),
	})

	pa := plan.New(plan.Config{
		Name:    "dyn-plan",
		Planner: planner,
		Tools:   []tool.Tool{toolA, toolB},
	})
	r := runner.New(runner.Config{AppName: "t", Root: pa, Store: session.InMemory()})

	status := map[string]string{}
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("plan it")) {
		if err != nil {
			t.Fatalf("run error: %v", err)
		}
		if ev != nil && !ev.Partial && ev.Progress != nil && ev.Progress.Kind == "plan_step" {
			status[ev.Progress.JobID] = ev.Progress.Status
		}
	}

	if status["a"] != string(plan.Done) || status["b"] != string(plan.Done) {
		t.Fatalf("statuses = %v, want a,b completed", status)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 2 || ran[0] != "tool_a" || ran[1] != "tool_b" {
		t.Fatalf("execution order = %v, want [tool_a tool_b]", ran)
	}
}

func TestParseUnknownTool(t *testing.T) {
	raw := []byte(`{"id":"x","goal":"g","steps":[{"id":"a","name":"A","executor":{"type":"tool","name":"ghost"}}]}`)
	if _, err := plan.Parse(raw, nil, nil); err == nil {
		t.Fatal("Parse(unknown tool) = nil, want error")
	}
}
