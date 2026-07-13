package agent_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// echoWorker returns an agent whose reply is its input task verbatim — handy for
// asserting dependency ordering and data flow.
func echoWorker(t *testing.T) *agent.Agent {
	t.Helper()
	a, err := agent.New(agent.WithModel(mock.New("echo", func(req *llm.Request) *llm.Response {
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role == core.RoleUser {
				return mock.Text(req.Messages[i].Text())
			}
		}
		return mock.Text("")
	})))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestPlanDependencyDataFlow(t *testing.T) {
	w := echoWorker(t)
	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "a", Task: "RA"},
		{ID: "b", Task: "RB"},
		{ID: "d", Task: "combine {{a}} {{b}}", DependsOn: []string{"a", "b"}},
	}}
	pa := agent.NewPlan("dag", plan, agent.WithWorker(w))

	out, err := pa.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	// d is the leaf; it ran after a and b and saw their outputs via {{a}}/{{b}}.
	if out != "combine RA RB" {
		t.Fatalf("plan output = %q, want %q", out, "combine RA RB")
	}
}

func TestPlanCycleRejected(t *testing.T) {
	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "a", Task: "x", DependsOn: []string{"b"}},
		{ID: "b", Task: "y", DependsOn: []string{"a"}},
	}}
	pa := agent.NewPlan("cycle", plan, agent.WithWorker(echoWorker(t)))
	if _, err := pa.Run(context.Background(), "go"); err == nil {
		t.Fatal("expected a cycle error")
	}
}

func TestPlanUnknownDepRejected(t *testing.T) {
	plan := agent.Plan{Nodes: []agent.Node{{ID: "a", Task: "x", DependsOn: []string{"ghost"}}}}
	pa := agent.NewPlan("bad", plan, agent.WithWorker(echoWorker(t)))
	if _, err := pa.Run(context.Background(), "go"); err == nil {
		t.Fatal("expected an unknown-dependency error")
	}
}

// errModel always errors, to drive failure paths.
type errModel struct{}

func (errModel) Name() string { return "err" }
func (errModel) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) { yield(nil, errors.New("boom")) }
}

func TestPlanFailFast(t *testing.T) {
	bad, _ := agent.New(agent.WithModel(errModel{}))
	plan := agent.Plan{Nodes: []agent.Node{{ID: "a", Task: "x", Worker: bad}}}
	pa := agent.NewPlan("ff", plan, agent.WithWorker(echoWorker(t)))
	if _, err := pa.Run(context.Background(), "go"); err == nil {
		t.Fatal("FailFast should surface the node error")
	}
}

func TestPlanContinueOnErrorSkipsDependents(t *testing.T) {
	bad, _ := agent.New(agent.WithModel(errModel{}))
	w := echoWorker(t)
	plan := agent.Plan{Nodes: []agent.Node{
		{ID: "x", Task: "fails", Worker: bad},
		{ID: "y", Task: "needs x", DependsOn: []string{"x"}}, // should be skipped
		{ID: "z", Task: "ZZ"},                                // independent → done
	}}
	pa := agent.NewPlan("coe", plan, agent.WithWorker(w), agent.WithErrorPolicy(agent.ContinueOnError))

	status := map[string]string{}
	for ev, err := range pa.Stream(context.Background(), "go").Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if d, ok := ev.(core.PlanNodeDone); ok {
			status[d.NodeID] = d.Status
		}
	}
	if status["x"] != "failed" || status["y"] != "skipped" || status["z"] != "done" {
		t.Fatalf("statuses = %v, want x:failed y:skipped z:done", status)
	}
}

func TestPlanFinalApprovalPauseResume(t *testing.T) {
	ctx := context.Background()
	w := echoWorker(t)
	plan := agent.Plan{Nodes: []agent.Node{{ID: "only", Task: "RESULT"}}}
	// The plan agent has its own checkpointer; Stream pauses into it and Resume
	// reads from it (same agent).
	pa := agent.NewPlan("appr", plan, agent.WithWorker(w), agent.WithFinalApproval())

	run := pa.Stream(ctx, "go", agent.OnThread("t1"))
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
		t.Fatalf("expected a final-approval pause, pending=%v", pending)
	}

	run.Decide(agent.Allow(pending[0].CallID))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := cont.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "RESULT" {
		t.Fatalf("approved result = %q, want RESULT", res.Message.Text())
	}
}
