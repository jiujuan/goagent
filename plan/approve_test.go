package plan_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/plan"
)

func TestStepApprovalDeniedSkipsAndContinues(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	// b requires approval and will be denied; c depends on b. a is independent.
	b := okStep("b", nil, &ran, &mu)
	b.NeedApproval = true
	p := &plan.Plan{ID: "ap1", Goal: "approval", Steps: []*plan.Step{
		okStep("a", nil, &ran, &mu),
		b,
		okStep("c", []string{"b"}, &ran, &mu),
	}}

	approver := plan.Funcs{
		Step: func(_ context.Context, s *plan.Step) (middleware.Decision, error) {
			if s.ID == "b" {
				return middleware.Deny("不允许执行 b"), nil
			}
			return middleware.Approve(), nil
		},
	}
	status, _ := drive(t, p, plan.Config{Approver: approver})

	if status["a"] != string(plan.Done) {
		t.Errorf("a status=%q want completed", status["a"])
	}
	if status["b"] != string(plan.Skipped) {
		t.Errorf("b status=%q want skipped", status["b"])
	}
	// c depends on a skipped (non-blocking) upstream, so it still runs.
	if status["c"] != string(plan.Done) {
		t.Errorf("c status=%q want completed", status["c"])
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range ran {
		if id == "b" {
			t.Error("b should not have executed after denial")
		}
	}
}

func TestPlanApprovalDeniedAbortsAll(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	p := &plan.Plan{ID: "ap2", Goal: "plan-deny", Steps: []*plan.Step{
		okStep("a", nil, &ran, &mu),
	}}
	approver := plan.Funcs{
		Plan: func(_ context.Context, _ *plan.Plan) (middleware.Decision, error) {
			return middleware.Deny("整个计划不批准"), nil
		},
	}
	status, _ := drive(t, p, plan.Config{Approver: approver})

	if len(status) != 0 {
		t.Errorf("no step should have run, got statuses=%v", status)
	}
	if len(ran) != 0 {
		t.Errorf("no step should have executed, ran=%v", ran)
	}
}
