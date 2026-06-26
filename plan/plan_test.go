package plan_test

import (
	"strings"
	"testing"

	"github.com/jiujuan/goagent/plan"
)

func noop(id string, deps ...string) *plan.Step {
	return &plan.Step{ID: id, Name: id, DependsOn: deps,
		Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
			return &plan.StepResult{StepID: id}, nil
		})}
}

func TestValidateOK(t *testing.T) {
	p := &plan.Plan{ID: "p", Goal: "g", Steps: []*plan.Step{
		noop("a"), noop("b", "a"), noop("c", "a", "b"),
	}}
	if err := plan.Validate(p); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidateEmpty(t *testing.T) {
	if err := plan.Validate(&plan.Plan{ID: "p"}); err == nil {
		t.Fatal("Validate(empty) = nil, want error")
	}
}

func TestValidateDuplicateID(t *testing.T) {
	p := &plan.Plan{ID: "p", Steps: []*plan.Step{noop("a"), noop("a")}}
	if err := plan.Validate(p); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Validate(dup) = %v, want duplicate error", err)
	}
}

func TestValidateDanglingDependency(t *testing.T) {
	p := &plan.Plan{ID: "p", Steps: []*plan.Step{noop("a", "ghost")}}
	if err := plan.Validate(p); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("Validate(dangling) = %v, want unknown-step error", err)
	}
}

func TestValidateCycle(t *testing.T) {
	p := &plan.Plan{ID: "p", Steps: []*plan.Step{
		noop("a", "c"), noop("b", "a"), noop("c", "b"),
	}}
	if err := plan.Validate(p); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Validate(cycle) = %v, want cycle error", err)
	}
}

func TestValidateSelfDependency(t *testing.T) {
	p := &plan.Plan{ID: "p", Steps: []*plan.Step{noop("a", "a")}}
	if err := plan.Validate(p); err == nil {
		t.Fatal("Validate(self-dep) = nil, want error")
	}
}
