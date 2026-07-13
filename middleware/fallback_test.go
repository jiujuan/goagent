package middleware_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/middleware"
)

// fakeModel is a scriptable llm.Model for resilience tests. behavior is invoked
// per Generate call; calls counts invocations.
type fakeModel struct {
	name     string
	calls    *int
	behavior func(yield func(*llm.Response, error) bool)
}

func (m fakeModel) Name() string { return m.name }

func (m fakeModel) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		if m.calls != nil {
			*m.calls++
		}
		m.behavior(yield)
	}
}

func okText(s string) func(yield func(*llm.Response, error) bool) {
	return func(yield func(*llm.Response, error) bool) {
		yield(&llm.Response{Message: core.AssistantText(s), StopReason: llm.StopEnd}, nil)
	}
}

func failPreStream(err error) func(yield func(*llm.Response, error) bool) {
	return func(yield func(*llm.Response, error) bool) { yield(nil, err) }
}

func failMidStream(partial string, err error) func(yield func(*llm.Response, error) bool) {
	return func(yield func(*llm.Response, error) bool) {
		if !yield(&llm.Response{Message: core.AssistantText(partial), Partial: true}, nil) {
			return
		}
		yield(nil, err)
	}
}

func drain(t *testing.T, m llm.Model) (string, error) {
	t.Helper()
	var last core.Message
	for resp, err := range m.Generate(context.Background(), &llm.Request{}) {
		if err != nil {
			return "", err
		}
		last = resp.Message
	}
	return last.Text(), nil
}

func TestFallbackPreStreamFailsOver(t *testing.T) {
	var primaryCalls, backupCalls int
	var fellOver bool
	primary := fakeModel{name: "primary", calls: &primaryCalls, behavior: failPreStream(&llm.StatusError{Provider: "primary", Code: 503})}
	backup := fakeModel{name: "backup", calls: &backupCalls, behavior: okText("backup answer")}

	model := middleware.FallbackModel(middleware.FallbackOptions{
		OnFallback: func(from, to string, err error) { fellOver = true },
	}, primary, backup)

	got, err := drain(t, model)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "backup answer" {
		t.Errorf("answer = %q, want backup answer", got)
	}
	if primaryCalls != 1 || backupCalls != 1 {
		t.Errorf("calls primary=%d backup=%d, want 1/1", primaryCalls, backupCalls)
	}
	if !fellOver {
		t.Error("OnFallback was not called")
	}
}

func TestFallbackMidStreamDoesNotSwitch(t *testing.T) {
	var backupCalls int
	boom := errors.New("mid-stream boom")
	primary := fakeModel{name: "primary", behavior: failMidStream("half ", boom)}
	backup := fakeModel{name: "backup", calls: &backupCalls, behavior: okText("backup")}

	model := middleware.FallbackModel(middleware.FallbackOptions{}, primary, backup)

	_, err := drain(t, model)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want mid-stream boom", err)
	}
	if backupCalls != 0 {
		t.Errorf("backup called %d times after mid-stream failure, want 0", backupCalls)
	}
}

func TestFallbackPredicateRejects(t *testing.T) {
	var backupCalls int
	// 400 is not retryable per llm.IsRetryable, so no failover.
	primary := fakeModel{name: "primary", behavior: failPreStream(&llm.StatusError{Provider: "primary", Code: 400})}
	backup := fakeModel{name: "backup", calls: &backupCalls, behavior: okText("backup")}

	model := middleware.FallbackModel(middleware.FallbackOptions{}, primary, backup)

	_, err := drain(t, model)
	var se *llm.StatusError
	if !errors.As(err, &se) || se.Code != 400 {
		t.Errorf("err = %v, want status 400", err)
	}
	if backupCalls != 0 {
		t.Errorf("backup called %d times for non-retryable error, want 0", backupCalls)
	}
}

func TestFallbackAllFailReturnsLastError(t *testing.T) {
	last := &llm.StatusError{Provider: "backup", Code: 502}
	primary := fakeModel{name: "primary", behavior: failPreStream(&llm.StatusError{Provider: "primary", Code: 503})}
	backup := fakeModel{name: "backup", behavior: failPreStream(last)}

	model := middleware.FallbackModel(middleware.FallbackOptions{}, primary, backup)

	_, err := drain(t, model)
	var se *llm.StatusError
	if !errors.As(err, &se) || se.Code != 502 {
		t.Errorf("err = %v, want last error status 502", err)
	}
}
