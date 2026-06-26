package plan_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/plan"
)

var errBoom = errors.New("boom")

// backends is the matrix every backend-agnostic behavior is verified against, so
// the queue.Worker pool and the default goroutine pool stay observably identical.
var backends = []struct {
	name    string
	backend plan.Backend
}{
	{"goroutines", plan.BackendGoroutines},
	{"queue", plan.BackendQueue},
}

func TestBackendsDependencyOrder(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			var mu sync.Mutex
			var ran []string
			p := &plan.Plan{ID: "bo", Goal: "order", Steps: []*plan.Step{
				okStep("a", nil, &ran, &mu),
				okStep("b", []string{"a"}, &ran, &mu),
				okStep("c", []string{"b"}, &ran, &mu),
			}}
			status, _ := drive(t, p, plan.Config{Backend: b.backend})
			for _, id := range []string{"a", "b", "c"} {
				if status[id] != string(plan.Done) {
					t.Errorf("step %s status=%q want completed", id, status[id])
				}
			}
			if len(ran) != 3 || ran[0] != "a" || ran[2] != "c" {
				t.Fatalf("order = %v, want a..c", ran)
			}
		})
	}
}

func TestBackendsParallelism(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			var inFlight, maxInFlight atomic.Int32
			overlap := func(id string, deps []string) *plan.Step {
				return &plan.Step{ID: id, Name: id, DependsOn: deps,
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
					})}
			}
			p := &plan.Plan{ID: "bp", Goal: "diamond", Steps: []*plan.Step{
				overlap("a", nil),
				overlap("b", []string{"a"}),
				overlap("c", []string{"a"}),
				overlap("d", []string{"b", "c"}),
			}}
			status, _ := drive(t, p, plan.Config{Backend: b.backend, MaxConc: 4})
			for _, id := range []string{"a", "b", "c", "d"} {
				if status[id] != string(plan.Done) {
					t.Errorf("step %s status=%q want completed", id, status[id])
				}
			}
			if maxInFlight.Load() < 2 {
				t.Errorf("max concurrency = %d, want >= 2", maxInFlight.Load())
			}
		})
	}
}

func TestBackendsPolicyFailBlocks(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			var mu sync.Mutex
			var ran []string
			p := &plan.Plan{ID: "bf", Goal: "fail", Steps: []*plan.Step{
				failStep("a", nil, plan.PolicyFail),
				okStep("b", []string{"a"}, &ran, &mu),
			}}
			status, _ := drive(t, p, plan.Config{Backend: b.backend})
			if status["a"] != string(plan.Failed) {
				t.Errorf("a status=%q want failed", status["a"])
			}
			if status["b"] != string(plan.Blocked) {
				t.Errorf("b status=%q want blocked", status["b"])
			}
			if len(ran) != 0 {
				t.Errorf("b should not have run, ran=%v", ran)
			}
		})
	}
}

func TestBackendsRetry(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			var attempts atomic.Int32
			p := &plan.Plan{ID: "br", Goal: "retry", Steps: []*plan.Step{{
				ID: "a", Name: "a", Retry: plan.RetryPolicy{Max: 3},
				Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
					if attempts.Add(1) < 3 {
						return nil, errBoom
					}
					return &plan.StepResult{StepID: "a", Output: "ok"}, nil
				}),
			}}}
			status, _ := drive(t, p, plan.Config{Backend: b.backend})
			if status["a"] != string(plan.Done) {
				t.Errorf("a status=%q want completed", status["a"])
			}
			if attempts.Load() != 3 {
				t.Errorf("attempts = %d, want 3", attempts.Load())
			}
		})
	}
}
