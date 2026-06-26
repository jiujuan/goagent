package plan_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/plan"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

// TestResumeFromFailure runs a plan whose first step fails on the first attempt
// and succeeds on a retry-in-a-fresh-run. Across a simulated restart (two
// FileStore instances over the same dir), completed steps are skipped and the
// run resumes from the failure frontier.
func TestResumeFromFailure(t *testing.T) {
	dir := t.TempDir()

	var aExec, bExec atomic.Int32 // count real executions of each step
	var aShouldFail atomic.Bool
	aShouldFail.Store(true)

	build := func() *plan.Plan {
		return &plan.Plan{ID: "resume", Goal: "resume test", Steps: []*plan.Step{
			{ID: "a", Name: "a", OnError: plan.PolicyFail,
				Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
					aExec.Add(1)
					if aShouldFail.Load() {
						return nil, context.DeadlineExceeded
					}
					return &plan.StepResult{StepID: "a", Output: "a-out"}, nil
				})},
			{ID: "b", Name: "b", DependsOn: []string{"a"},
				Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
					bExec.Add(1)
					up, _ := sc.State.Get(plan.StepResultKey("a"))
					return &plan.StepResult{StepID: "b", Output: "saw:" + toStr(up)}, nil
				})},
		}}
	}

	runOnce := func() map[string]string {
		store, err := session.NewFileStore(dir)
		if err != nil {
			t.Fatalf("NewFileStore: %v", err)
		}
		pa := plan.New(plan.Config{Name: "p", Plan: build()})
		r := runner.New(runner.Config{AppName: "app", Root: pa, Store: store})
		status := map[string]string{}
		for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("go")) {
			if err != nil {
				t.Fatalf("run error: %v", err)
			}
			if ev != nil && !ev.Partial && ev.Progress != nil && ev.Progress.Kind == "plan_step" {
				status[ev.Progress.JobID] = ev.Progress.Status
			}
		}
		return status
	}

	// Run 1: a fails → b blocked.
	s1 := runOnce()
	if s1["a"] != string(plan.Failed) {
		t.Fatalf("run1 a=%q want failed", s1["a"])
	}
	if s1["b"] != string(plan.Blocked) {
		t.Fatalf("run1 b=%q want blocked", s1["b"])
	}

	// Run 2 (simulated restart): a now succeeds; b runs and sees a's output.
	aShouldFail.Store(false)
	s2 := runOnce()
	if s2["a"] != string(plan.Done) {
		t.Fatalf("run2 a=%q want completed", s2["a"])
	}
	if s2["b"] != string(plan.Done) {
		t.Fatalf("run2 b=%q want completed", s2["b"])
	}

	// a executed twice (failed once, then re-run); b executed once (only after
	// a finally succeeded).
	if aExec.Load() != 2 {
		t.Errorf("a executed %d times, want 2", aExec.Load())
	}
	if bExec.Load() != 1 {
		t.Errorf("b executed %d times, want 1", bExec.Load())
	}
}

// TestResumeSkipsCompleted verifies a step completed in run 1 is not re-executed
// when a later step forces a second run.
func TestResumeSkipsCompleted(t *testing.T) {
	dir := t.TempDir()
	var aExec atomic.Int32
	var bShouldFail atomic.Bool
	bShouldFail.Store(true)

	build := func() *plan.Plan {
		return &plan.Plan{ID: "skip", Goal: "skip completed", Steps: []*plan.Step{
			{ID: "a", Name: "a",
				Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
					aExec.Add(1)
					return &plan.StepResult{StepID: "a", Output: "a"}, nil
				})},
			{ID: "b", Name: "b", DependsOn: []string{"a"}, OnError: plan.PolicyFail,
				Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
					if bShouldFail.Load() {
						return nil, context.DeadlineExceeded
					}
					return &plan.StepResult{StepID: "b", Output: "b"}, nil
				})},
		}}
	}
	runOnce := func() {
		store, _ := session.NewFileStore(dir)
		pa := plan.New(plan.Config{Name: "p", Plan: build()})
		r := runner.New(runner.Config{AppName: "app", Root: pa, Store: store})
		for _, err := range r.Run(context.Background(), "u", "s", core.UserText("go")) {
			if err != nil {
				t.Fatalf("run error: %v", err)
			}
		}
	}
	runOnce() // a done, b fails
	bShouldFail.Store(false)
	runOnce() // a skipped (already done), b re-runs and succeeds

	if aExec.Load() != 1 {
		t.Errorf("a executed %d times, want 1 (should be skipped on resume)", aExec.Load())
	}
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
