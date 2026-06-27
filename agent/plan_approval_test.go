package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
)

// drainPlan iterates a run to its terminal event, returning the per-node final
// statuses seen and any pending approvals at the pause.
func drainPlan(t *testing.T, run *agent.Run) (map[string]string, []core.ApprovalRequest) {
	t.Helper()
	status := map[string]string{}
	var pending []core.ApprovalRequest
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case core.PlanNodeDone:
			status[e.NodeID] = e.Status
		case core.Interrupted:
			pending = e.Pending
		}
	}
	return status, pending
}

func TestPlanPerNodeApproveResume(t *testing.T) {
	ctx := context.Background()
	w := echoWorker(t)
	// "gated" needs approval; "free" is independent and should run while gated waits.
	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "gated", Task: "G", Approve: true},
		{ID: "free", Task: "F"},
		{ID: "after", Task: "{{gated}}+{{free}}", DependsOn: []string{"gated", "free"}},
	}}
	pa := agent.NewPlan("pernode", plan, agent.WithWorker(w))

	run := pa.Stream(ctx, "go", agent.OnThread("t1"))
	status, pending := drainPlan(t, run)

	// Paused for the gated node; the independent "free" node already ran.
	if len(pending) != 1 || pending[0].CallID != "gated" {
		t.Fatalf("expected pause on 'gated', pending=%v", pending)
	}
	if status["free"] != "done" {
		t.Fatalf("'free' should run while 'gated' awaits approval; status=%v", status)
	}
	if status["gated"] != "" || status["after"] != "" {
		t.Fatalf("'gated'/'after' should not have run yet; status=%v", status)
	}

	run.Decide(agent.Allow("gated"))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if out.Message.Text() != "G+F" {
		t.Fatalf("after approval, result = %q, want G+F", out.Message.Text())
	}
}

func TestPlanPerNodeRejectCascades(t *testing.T) {
	ctx := context.Background()
	w := echoWorker(t)
	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "gated", Task: "G", Approve: true},
		{ID: "dep", Task: "needs {{gated}}", DependsOn: []string{"gated"}},
		{ID: "free", Task: "F"},
	}}
	pa := agent.NewPlan("reject", plan, agent.WithWorker(w))

	run := pa.Stream(ctx, "go", agent.OnThread("t1"))
	_, pending := drainPlan(t, run)
	if len(pending) != 1 {
		t.Fatalf("expected one pending approval, got %v", pending)
	}

	run.Decide(agent.Reject("gated", "not allowed"))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, _ := drainPlan(t, cont)
	if status["gated"] != "rejected" {
		t.Fatalf("'gated' = %q, want rejected", status["gated"])
	}
	if status["dep"] != "skipped" {
		t.Fatalf("'dep' = %q, want skipped (cascade from rejected dep)", status["dep"])
	}
}
