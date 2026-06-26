// Package plan adds execution plans to goagent: a first-class Plan/Step data
// model plus a dependency-aware DAG scheduler. Where the workflow agents
// (Sequential/Parallel/Loop) express a total order or a flat fan-out, a plan
// expresses an arbitrary dependency graph — independent steps run concurrently,
// dependents wait for their upstreams, and each step carries its own error
// policy, retry, timeout, and approval gate.
//
// A plan is driven by a PlanAgent, which implements agent.Agent, so it plugs
// straight into the Runner, composes inside a Pipeline, and can even nest (a
// step's executor may itself be another PlanAgent). Step state transitions
// stream as core.Events and persist via the Runner, which makes a plan
// resumable: a re-run skips completed steps and re-enters from the failure
// frontier.
package plan

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// errNoPlan is returned when a PlanAgent has neither a static Plan nor a
// planner-produced one to run.
var errNoPlan = errors.New("plan: no plan to run (set Config.Plan or Config.Planner)")

// errNoDraft is returned when a planner ran but produced no plan via set_plan.
var errNoDraft = errors.New("plan: planner produced no plan (set_plan was not called)")

// StepStatus is the lifecycle state of one step. Pending/Ready/Running are
// transient; Done/Failed/Skipped/Blocked are terminal. Waiting marks a step
// parked on its approval gate.
type StepStatus string

const (
	Pending StepStatus = "pending" // created, dependencies not yet satisfied
	Ready   StepStatus = "ready"   // dependencies satisfied, eligible to run
	Waiting StepStatus = "waiting_approval"
	Running StepStatus = "running"
	Done    StepStatus = "completed"
	Failed  StepStatus = "failed"  // ran and errored under PolicyContinue
	Skipped StepStatus = "skipped" // PolicySkip on error, or approval denied
	Blocked StepStatus = "blocked" // an upstream broke, so this never ran
)

// terminal reports whether a status is final (the scheduler will not advance it
// further, and a resume keeps it as-is unless it is a retryable failure).
func (s StepStatus) terminal() bool {
	switch s {
	case Done, Failed, Skipped, Blocked:
		return true
	default:
		return false
	}
}

// ErrorPolicy decides what happens to the plan when a step errors.
type ErrorPolicy string

const (
	// PolicyFail (default) aborts the plan: the failing step is marked Failed,
	// and every not-yet-started step is marked Blocked.
	PolicyFail ErrorPolicy = ""
	// PolicySkip marks the step Skipped and lets the plan continue; dependents
	// treat a skipped upstream as "completed without a result".
	PolicySkip ErrorPolicy = "skip"
	// PolicyContinue records the failure (status Failed) but, like Skip, does not
	// block dependents.
	PolicyContinue ErrorPolicy = "continue"
)

// blocksDependents reports whether a broken upstream under this policy should
// block its dependents. Only the abort policy does.
func (p ErrorPolicy) blocksDependents() bool { return p == PolicyFail }

// RetryPolicy controls per-step retries on error. Max is the number of retries
// after the first attempt (0 = no retry); Backoff is the base delay, doubled
// each attempt.
type RetryPolicy struct {
	Max     int
	Backoff time.Duration
}

// Step is one node in the plan DAG: what to do (Exec), what it waits on
// (DependsOn), and the policies governing its execution. The fields below the
// divider are runtime state the scheduler writes; they serialize into the
// snapshot that makes a plan resumable.
type Step struct {
	ID          string
	Name        string
	Description string
	DependsOn   []string // IDs of upstream steps; the edges of the DAG
	Exec        Executor // how this step runs (tool / agent / func)

	OnError      ErrorPolicy
	Retry        RetryPolicy
	NeedApproval bool
	Timeout      time.Duration // per-attempt deadline; 0 = none

	// --- runtime state (written by the scheduler, serialized for resume) ---
	Status   StepStatus
	Attempts int
	Result   *StepResult
}

// StepResult is what executing a step produced. On success Output holds the
// step's textual output; on failure Err holds the error message.
type StepResult struct {
	StepID string `json:"step_id"`
	Title  string `json:"title,omitempty"`
	Output string `json:"output,omitempty"`
	Err    string `json:"err,omitempty"`
}

// Plan is a goal plus a set of steps forming a DAG.
type Plan struct {
	ID    string
	Goal  string
	Steps []*Step
}

// byID indexes the plan's steps by ID. It assumes IDs are unique (Validate
// enforces this).
func (p *Plan) byID() map[string]*Step {
	m := make(map[string]*Step, len(p.Steps))
	for _, s := range p.Steps {
		m[s.ID] = s
	}
	return m
}

// Validate checks the plan is well-formed before scheduling: non-empty unique
// step IDs, every DependsOn referencing an existing step, and no cycles. A
// cyclic or dangling graph would otherwise deadlock the scheduler.
func Validate(p *Plan) error {
	if p == nil || len(p.Steps) == 0 {
		return errors.New("plan: empty plan")
	}
	index := make(map[string]*Step, len(p.Steps))
	for _, s := range p.Steps {
		if s.ID == "" {
			return errors.New("plan: step with empty ID")
		}
		if _, dup := index[s.ID]; dup {
			return fmt.Errorf("plan: duplicate step ID %q", s.ID)
		}
		index[s.ID] = s
	}
	for _, s := range p.Steps {
		for _, dep := range s.DependsOn {
			if dep == s.ID {
				return fmt.Errorf("plan: step %q depends on itself", s.ID)
			}
			if _, ok := index[dep]; !ok {
				return fmt.Errorf("plan: step %q depends on unknown step %q", s.ID, dep)
			}
		}
	}
	return detectCycle(p, index)
}

// detectCycle runs a three-color DFS over the dependency graph; a back edge
// (encountering a gray node) is a cycle.
func detectCycle(p *Plan, index map[string]*Step) error {
	const (
		white = 0 // unvisited
		gray  = 1 // on the current DFS stack
		black = 2 // fully explored
	)
	color := make(map[string]int, len(p.Steps))
	var visit func(id string) error
	visit = func(id string) error {
		color[id] = gray
		for _, dep := range index[id].DependsOn {
			switch color[dep] {
			case gray:
				return fmt.Errorf("plan: dependency cycle through step %q", dep)
			case white:
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}
	for _, s := range p.Steps {
		if color[s.ID] == white {
			if err := visit(s.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// clonePlan returns a deep copy of a plan's structure with runtime state reset,
// so each Run operates on its own copy and the caller's template is never
// mutated. Executors are shared by reference (they are stateless capabilities).
func clonePlan(p *Plan) *Plan {
	out := &Plan{ID: p.ID, Goal: p.Goal, Steps: make([]*Step, len(p.Steps))}
	for i, s := range p.Steps {
		cp := *s
		cp.DependsOn = append([]string(nil), s.DependsOn...)
		cp.Status = Pending
		cp.Attempts = 0
		cp.Result = nil
		out.Steps[i] = &cp
	}
	return out
}

// summarize renders a multi-line human summary of a settled plan: one line per
// step plus any outputs.
func summarize(p *Plan) string {
	var b strings.Builder
	fmt.Fprintf(&b, "📋 计划「%s」执行结果：\n", p.Goal)
	for _, s := range p.Steps {
		b.WriteString("   • ")
		b.WriteString(stepLine(s, s.Status))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// hasFailure reports whether the plan aborted: at least one step failed under a
// policy that blocks dependents (PolicyFail). Continue/Skip failures do not
// count, since by design they let the plan finish.
func hasFailure(p *Plan) bool {
	for _, s := range p.Steps {
		if s.Status == Failed && s.OnError.blocksDependents() {
			return true
		}
	}
	return false
}
