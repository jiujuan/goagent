package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// buildDangerAgent makes an agent that calls a gated "danger" tool, wired to the
// given (file) checkpointer. ran counts how many times the tool actually ran.
func buildDangerAgent(store checkpoint.Checkpointer, ran *int) *agent.Agent {
	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		*ran++
		return "done", nil
	})
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("ok:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "danger", "{}")
	})
	a, _ := agent.New(
		agent.WithModel(model),
		agent.WithTools(danger),
		agent.WithMiddleware(gate{}), // interrupts before any tool (defined in run_test.go)
		agent.WithCheckpointer(store),
	)
	return a
}

func TestDurableResumeAcrossInstances(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var ran int

	// "Process 1": pauses for approval, checkpoints to disk.
	s1, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	run := buildDangerAgent(s1, &ran).Stream(ctx, "go", agent.OnThread("t1"))
	var pending []core.ApprovalRequest
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if it, ok := ev.(core.Interrupted); ok {
			pending = it.Pending
		}
	}
	if len(pending) != 1 {
		t.Fatalf("expected a pause, got %v", pending)
	}
	if ran != 0 {
		t.Fatal("tool ran before approval")
	}

	// "Process 2": a brand-new agent + a brand-new file checkpointer over the
	// same dir resumes and completes — durable, cross-process HITL resume.
	s2, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	cont, err := buildDangerAgent(s2, &ran).Resume(ctx, "t1", agent.Allow(pending[0].CallID))
	if err != nil {
		t.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if ran != 1 || res.Message.Text() != "ok:done" {
		t.Fatalf("durable resume failed: ran=%d result=%q", ran, res.Message.Text())
	}
}
