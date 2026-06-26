package middleware_test

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
)

// flakyModel fails the first failUntil attempts (error before yielding), then
// succeeds. It counts attempts.
type flakyModel struct {
	failUntil int
	attempts  int
}

func (m *flakyModel) Name() string { return "flaky" }
func (m *flakyModel) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.attempts++
		if m.attempts <= m.failUntil {
			yield(nil, errors.New("transient"))
			return
		}
		yield(mock.Text("ok"), nil)
	}
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	flaky := &flakyModel{failUntil: 2}
	model := middleware.Chain(flaky, middleware.Retry(&middleware.RetryOptions{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
	}))

	var final string
	for resp, err := range model.Generate(context.Background(), &llm.Request{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		final = resp.Message.Text()
	}
	if final != "ok" {
		t.Fatalf("final = %q", final)
	}
	if flaky.attempts != 3 {
		t.Fatalf("expected 3 attempts (2 fail + 1 ok), got %d", flaky.attempts)
	}
}

func TestRetryGivesUp(t *testing.T) {
	flaky := &flakyModel{failUntil: 100}
	model := middleware.Chain(flaky, middleware.Retry(&middleware.RetryOptions{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
	}))

	var gotErr error
	for _, err := range model.Generate(context.Background(), &llm.Request{}) {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if flaky.attempts != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", flaky.attempts)
	}
}

// partialThenFail yields one partial, then an error — must NOT be retried.
type partialThenFail struct{ attempts int }

func (m *partialThenFail) Name() string { return "partial" }
func (m *partialThenFail) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.attempts++
		if !yield(mock.Partial("He"), nil) {
			return
		}
		yield(nil, errors.New("mid-stream failure"))
	}
}

func TestRetryDoesNotRetryAfterPartial(t *testing.T) {
	m := &partialThenFail{}
	model := middleware.Chain(m, middleware.Retry(&middleware.RetryOptions{MaxAttempts: 5, BaseDelay: time.Millisecond}))

	var partials int
	var gotErr error
	for resp, err := range model.Generate(context.Background(), &llm.Request{}) {
		if err != nil {
			gotErr = err
			continue
		}
		if resp.Partial {
			partials++
		}
	}
	if gotErr == nil {
		t.Fatal("expected the mid-stream error to propagate")
	}
	if m.attempts != 1 {
		t.Fatalf("must not retry after partial output, got %d attempts", m.attempts)
	}
	if partials != 1 {
		t.Fatalf("expected the partial to be delivered once, got %d", partials)
	}
}

func TestRetryRespectsContextCancel(t *testing.T) {
	flaky := &flakyModel{failUntil: 100}
	model := middleware.Chain(flaky, middleware.Retry(&middleware.RetryOptions{MaxAttempts: 100, BaseDelay: 50 * time.Millisecond}))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	for _, err := range model.Generate(ctx, &llm.Request{}) {
		_ = err
	}
	// Should stop well before 100 attempts due to context timeout during backoff.
	if flaky.attempts > 5 {
		t.Fatalf("expected few attempts before ctx cancel, got %d", flaky.attempts)
	}
}
