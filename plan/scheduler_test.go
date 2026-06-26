package plan_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/plan"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

// drive runs a PlanAgent to completion through a Runner and returns the terminal
// status of every step (keyed by step ID), collected from the step events'
// Progress, plus the final session state.
func drive(t *testing.T, p *plan.Plan, cfg plan.Config) (map[string]string, session.State) {
	t.Helper()
	cfg.Name = "planner-test"
	cfg.Plan = p
	store := session.InMemory()
	pa := plan.New(cfg)
	r := runner.New(runner.Config{AppName: "t", Root: pa, Store: store})

	status := map[string]string{}
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("go")) {
		if err != nil {
			t.Fatalf("run error: %v", err)
		}
		if ev != nil && !ev.Partial && ev.Progress != nil && ev.Progress.Kind == "plan_step" {
			status[ev.Progress.JobID] = ev.Progress.Status
		}
	}
	sess, _ := store.GetOrCreate(context.Background(), "t", "u", "s")
	return status, sess.State()
}

// okStep is a FuncExecutor step that records it ran and returns a fixed output.
func okStep(id string, deps []string, ran *[]string, mu *sync.Mutex) *plan.Step {
	return &plan.Step{
		ID: id, Name: id, DependsOn: deps,
		Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
			mu.Lock()
			*ran = append(*ran, id)
			mu.Unlock()
			return &plan.StepResult{StepID: id, Output: id + "-out"}, nil
		}),
	}
}

func TestSerialDependencyOrder(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	p := &plan.Plan{ID: "p1", Goal: "linear", Steps: []*plan.Step{
		okStep("a", nil, &ran, &mu),
		okStep("b", []string{"a"}, &ran, &mu),
		okStep("c", []string{"b"}, &ran, &mu),
	}}
	status, st := drive(t, p, plan.Config{})

	for _, id := range []string{"a", "b", "c"} {
		if status[id] != string(plan.Done) {
			t.Errorf("step %s: status=%q want completed", id, status[id])
		}
	}
	if len(ran) != 3 || ran[0] != "a" || ran[1] != "b" || ran[2] != "c" {
		t.Fatalf("execution order = %v, want [a b c]", ran)
	}
	if v, _ := st.Get(plan.StepResultKey("c")); v != "c-out" {
		t.Errorf("state[step:c:result] = %v, want c-out", v)
	}
}

func TestDiamondParallelism(t *testing.T) {
	// a -> {b, c} -> d. b and c must overlap in time.
	var inFlight, maxInFlight atomic.Int32
	overlap := func(id string, deps []string) *plan.Step {
		return &plan.Step{
			ID: id, Name: id, DependsOn: deps,
			Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
				n := inFlight.Add(1)
				for {
					m := maxInFlight.Load()
					if n <= m || maxInFlight.CompareAndSwap(m, n) {
						break
					}
				}
				time.Sleep(30 * time.Millisecond)
				inFlight.Add(-1)
				return &plan.StepResult{StepID: id, Output: id}, nil
			}),
		}
	}
	p := &plan.Plan{ID: "p2", Goal: "diamond", Steps: []*plan.Step{
		overlap("a", nil),
		overlap("b", []string{"a"}),
		overlap("c", []string{"a"}),
		overlap("d", []string{"b", "c"}),
	}}
	status, _ := drive(t, p, plan.Config{MaxConc: 4})

	for _, id := range []string{"a", "b", "c", "d"} {
		if status[id] != string(plan.Done) {
			t.Errorf("step %s status=%q want completed", id, status[id])
		}
	}
	if maxInFlight.Load() < 2 {
		t.Errorf("max concurrency = %d, want >= 2 (b and c should overlap)", maxInFlight.Load())
	}
}

func failStep(id string, deps []string, policy plan.ErrorPolicy) *plan.Step {
	return &plan.Step{
		ID: id, Name: id, DependsOn: deps, OnError: policy,
		Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
			return nil, errors.New("boom")
		}),
	}
}

func TestPolicyFailBlocksDependents(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	p := &plan.Plan{ID: "p3", Goal: "fail-abort", Steps: []*plan.Step{
		failStep("a", nil, plan.PolicyFail),
		okStep("b", []string{"a"}, &ran, &mu),
	}}
	status, _ := drive(t, p, plan.Config{})

	if status["a"] != string(plan.Failed) {
		t.Errorf("a status=%q want failed", status["a"])
	}
	if status["b"] != string(plan.Blocked) {
		t.Errorf("b status=%q want blocked", status["b"])
	}
	if len(ran) != 0 {
		t.Errorf("b should not have run, ran=%v", ran)
	}
}

func TestPolicySkipContinues(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	p := &plan.Plan{ID: "p4", Goal: "skip-continue", Steps: []*plan.Step{
		failStep("a", nil, plan.PolicySkip),
		okStep("b", []string{"a"}, &ran, &mu),
	}}
	status, _ := drive(t, p, plan.Config{})

	if status["a"] != string(plan.Skipped) {
		t.Errorf("a status=%q want skipped", status["a"])
	}
	if status["b"] != string(plan.Done) {
		t.Errorf("b status=%q want completed", status["b"])
	}
	if len(ran) != 1 || ran[0] != "b" {
		t.Errorf("b should have run, ran=%v", ran)
	}
}

func TestRetrySucceedsEventually(t *testing.T) {
	var attempts atomic.Int32
	p := &plan.Plan{ID: "p5", Goal: "retry", Steps: []*plan.Step{{
		ID: "a", Name: "a", Retry: plan.RetryPolicy{Max: 3},
		Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
			if attempts.Add(1) < 3 {
				return nil, errors.New("transient")
			}
			return &plan.StepResult{StepID: "a", Output: "ok"}, nil
		}),
	}}}
	status, _ := drive(t, p, plan.Config{})

	if status["a"] != string(plan.Done) {
		t.Errorf("a status=%q want completed", status["a"])
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}
