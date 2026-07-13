package middleware_test

import (
	"errors"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/middleware"
)

// switchableModel flips between failing and succeeding based on *mode, and
// counts how many times the inner Generate body actually runs.
func switchableModel(calls *int, mode *string, failErr error) llm.Model {
	return fakeModel{
		name:  "primary",
		calls: calls,
		behavior: func(yield func(*llm.Response, error) bool) {
			if *mode == "fail" {
				yield(nil, failErr)
				return
			}
			yield(&llm.Response{Message: core.AssistantText("ok"), StopReason: llm.StopEnd}, nil)
		},
	}
}

func TestCircuitOpensAndFailsFast(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	var calls int
	mode := "fail"
	var transitions []string

	cb := middleware.CircuitBreaker(
		switchableModel(&calls, &mode, &llm.StatusError{Provider: "p", Code: 503}),
		middleware.CircuitOptions{
			FailureThreshold: 3,
			OpenTimeout:      time.Minute,
			Now:              clock,
			OnStateChange: func(from, to middleware.CircuitState) {
				transitions = append(transitions, from.String()+"->"+to.String())
			},
		},
	)

	// 3 consecutive failures trip the breaker.
	for i := 0; i < 3; i++ {
		if _, err := drain(t, cb); err == nil {
			t.Fatalf("call %d: expected failure", i)
		}
	}
	if calls != 3 {
		t.Fatalf("inner calls = %d, want 3", calls)
	}

	// Now open: next call fails fast WITHOUT touching the inner model.
	_, err := drain(t, cb)
	if !errors.Is(err, middleware.ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}
	if calls != 3 {
		t.Errorf("inner called while open: calls = %d, want 3", calls)
	}

	// After the cooldown, a half-open probe is allowed; success closes it.
	now = now.Add(time.Minute + time.Second)
	mode = "ok"
	if _, err := drain(t, cb); err != nil {
		t.Fatalf("half-open probe failed: %v", err)
	}
	if calls != 4 {
		t.Errorf("probe did not reach inner: calls = %d, want 4", calls)
	}

	// Breaker is closed again: normal call passes through.
	if _, err := drain(t, cb); err != nil {
		t.Fatalf("post-recovery call failed: %v", err)
	}

	want := []string{"closed->open", "open->half-open", "half-open->closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Errorf("transition[%d] = %q, want %q", i, transitions[i], want[i])
		}
	}
}

func TestCircuitIgnoresNonFailureErrors(t *testing.T) {
	now := time.Unix(2000, 0)
	var calls int
	mode := "fail"
	// 400 is not retryable, so llm.IsRetryable (default IsFailure) does not
	// count it against the breaker.
	cb := middleware.CircuitBreaker(
		switchableModel(&calls, &mode, &llm.StatusError{Provider: "p", Code: 400}),
		middleware.CircuitOptions{FailureThreshold: 2, Now: func() time.Time { return now }},
	)

	for i := 0; i < 5; i++ {
		if _, err := drain(t, cb); err == nil {
			t.Fatalf("call %d: expected the 400 error", i)
		}
	}
	// Never tripped: all 5 calls reached the inner model.
	if calls != 5 {
		t.Errorf("inner calls = %d, want 5 (breaker should not trip on 400)", calls)
	}
}

func TestCircuitHalfOpenFailureReopens(t *testing.T) {
	now := time.Unix(3000, 0)
	clock := func() time.Time { return now }
	var calls int
	mode := "fail"

	cb := middleware.CircuitBreaker(
		switchableModel(&calls, &mode, &llm.StatusError{Provider: "p", Code: 500}),
		middleware.CircuitOptions{FailureThreshold: 1, OpenTimeout: time.Minute, Now: clock},
	)

	// Trip immediately (threshold 1).
	if _, err := drain(t, cb); err == nil {
		t.Fatal("expected failure")
	}
	// Cooldown elapses → half-open probe, which fails → reopen.
	now = now.Add(2 * time.Minute)
	if _, err := drain(t, cb); err == nil {
		t.Fatal("expected probe failure")
	}
	callsAfterProbe := calls
	// Immediately after the failed probe the breaker is open again: fail fast.
	_, err := drain(t, cb)
	if !errors.Is(err, middleware.ErrCircuitOpen) {
		t.Errorf("err = %v, want ErrCircuitOpen after failed probe", err)
	}
	if calls != callsAfterProbe {
		t.Errorf("inner called while reopened: calls = %d, want %d", calls, callsAfterProbe)
	}
}
