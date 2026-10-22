package agent

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/core"
)

type attemptTestKey struct{}

func TestBeginEndModelAttempt(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := &RunContext{Context: parent, preemptedStep: -1}
	base := context.WithValue(parent, attemptTestKey{}, "value")

	ctx, attempt := rc.beginModelAttempt(base, 3)
	if got := ctx.Value(attemptTestKey{}); got != "value" {
		t.Fatalf("attempt context value = %v, want the base context's value", got)
	}
	if attempt.step != 3 {
		t.Fatalf("attempt step = %d, want 3", attempt.step)
	}
	if !rc.requestPreemption("user") {
		t.Fatal("requestPreemption returned false for the active attempt")
	}
	if ctx.Err() == nil {
		t.Fatal("requestPreemption did not cancel the attempt context")
	}
	if parent.Err() != nil {
		t.Fatal("requestPreemption canceled the parent context")
	}
	if got := attempt.reason; got != "user" {
		t.Fatalf("attempt reason = %q, want user", got)
	}
	if !rc.endModelAttempt(attempt) {
		t.Fatal("endModelAttempt lost the preemption")
	}
	if !attempt.done {
		t.Fatal("endModelAttempt did not mark the attempt done")
	}
	if rc.requestPreemption("late") {
		t.Fatal("requestPreemption succeeded after the attempt ended")
	}
}

func TestRequestPreemptionOncePerStep(t *testing.T) {
	rc := &RunContext{Context: context.Background(), preemptedStep: -1}

	_, first := rc.beginModelAttempt(rc.Context, 2)
	if !rc.requestPreemption("first") {
		t.Fatal("first preemption in step 2 was refused")
	}
	if rc.requestPreemption("second") {
		t.Fatal("second preemption in the same attempt was accepted")
	}
	if !rc.endModelAttempt(first) {
		t.Fatal("endModelAttempt lost the first preemption")
	}

	_, retry := rc.beginModelAttempt(rc.Context, 2)
	if rc.requestPreemption("same step") {
		t.Fatal("a retry in an already preempted step was preempted again")
	}
	if rc.endModelAttempt(retry) {
		t.Fatal("the retry was reported as preempted")
	}

	_, next := rc.beginModelAttempt(rc.Context, 3)
	if !rc.requestPreemption("next step") {
		t.Fatal("preemption in a later step was refused")
	}
	if !rc.endModelAttempt(next) {
		t.Fatal("endModelAttempt lost the later step's preemption")
	}
}

func TestRunContextDerivedContextsDoNotShareAttempt(t *testing.T) {
	parent := &RunContext{
		Context:       context.Background(),
		State:         &core.State{},
		preemptedStep: -1,
	}
	_, attempt := parent.beginModelAttempt(parent.Context, 0)
	defer parent.endModelAttempt(attempt)

	deeper := parent.deeper()
	if deeper.requestPreemption("deeper") {
		t.Fatal("deeper inherited the parent's active attempt")
	}
	if deeper.preemptedStep != -1 {
		t.Fatalf("deeper preemptedStep = %d, want -1", deeper.preemptedStep)
	}

	branch, err := parent.forBranch("b")
	if err != nil {
		t.Fatal(err)
	}
	if branch.requestPreemption("branch") {
		t.Fatal("forBranch inherited the parent's active attempt")
	}
	if branch.preemptedStep != -1 {
		t.Fatalf("forBranch preemptedStep = %d, want -1", branch.preemptedStep)
	}

	if !parent.requestPreemption("parent") {
		t.Fatal("the parent's own attempt was not preemptible")
	}
}
