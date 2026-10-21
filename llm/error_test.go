package llm_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jiujuan/goagent/llm"
)

func TestStatusErrorRetryable(t *testing.T) {
	cases := []struct {
		code int
		want bool
	}{
		{400, false}, {401, false}, {403, false}, {404, false},
		{408, true}, {429, true},
		{500, true}, {502, true}, {503, true},
	}
	for _, c := range cases {
		e := &llm.StatusError{Provider: "x", Code: c.code}
		if got := e.Retryable(); got != c.want {
			t.Errorf("status %d Retryable() = %v, want %v", c.code, got, c.want)
		}
	}
}

func TestStatusErrorMessageStable(t *testing.T) {
	e := &llm.StatusError{Provider: "anthropic", Code: 429, Body: "rate limited"}
	want := "anthropic: status 429: rate limited"
	if e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
}

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"status 429", &llm.StatusError{Code: 429}, true},
		{"status 400", &llm.StatusError{Code: 400}, false},
		{"wrapped 503", fmt.Errorf("call: %w", &llm.StatusError{Code: 503}), true},
		{"plain network error", errors.New("connection reset"), true},
	}
	for _, c := range cases {
		if got := llm.IsRetryable(c.err); got != c.want {
			t.Errorf("%s: IsRetryable = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestNewStatusErrorClassifiesContextLength(t *testing.T) {
	cases := []struct {
		name      string
		provider  string
		code      int
		body      string
		wantTyped bool
		limit     int
		requested int
	}{
		{
			name:     "openai limit and requested",
			provider: "openaicompat", code: 400,
			body:      "This model's maximum context length is 8192 tokens, however you requested 9000 tokens",
			wantTyped: true, limit: 8192, requested: 9000,
		},
		{
			name:     "openai resulted in",
			provider: "openaicompat", code: 400,
			body:      "context_length_exceeded: your messages resulted in 9000 tokens",
			wantTyped: true, requested: 9000,
		},
		{
			name:     "openai requested only",
			provider: "openaicompat", code: 400,
			body:      "context_length_exceeded: you requested 9000 tokens",
			wantTyped: true, requested: 9000,
		},
		{
			name:     "anthropic prompt too long",
			provider: "anthropic", code: 400,
			body:      "prompt is too long: 210000 tokens > 200000 maximum",
			wantTyped: true, limit: 200000, requested: 210000,
		},
		{
			name:     "anthropic exceed context limit",
			provider: "anthropic", code: 413,
			body:      "exceed context limit: 100 + 50 > 120",
			wantTyped: true, limit: 120, requested: 150,
		},
		{
			name:     "generic input too long",
			provider: "gateway", code: 400,
			body:      "input is too long",
			wantTyped: true,
		},
		{
			name:     "generic request too large",
			provider: "gateway", code: 413,
			body:      "request too large",
			wantTyped: true,
		},
		{
			name:     "400 without marker",
			provider: "test", code: 400,
			body: "invalid model",
		},
		{
			name:     "429 with marker stays plain",
			provider: "test", code: 429,
			body: "maximum context length is 8192 tokens",
		},
		{
			name:     "500 with marker stays plain",
			provider: "test", code: 500,
			body: "prompt is too long: 210000 tokens > 200000 maximum",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := llm.NewStatusError(c.provider, c.code, c.body)
			var cle *llm.ContextLengthError
			gotTyped := errors.As(err, &cle)
			if gotTyped != c.wantTyped {
				t.Fatalf("typed = %v, want %v (err = %v)", gotTyped, c.wantTyped, err)
			}
			if !c.wantTyped {
				var se *llm.StatusError
				if !errors.As(err, &se) {
					t.Fatalf("want *llm.StatusError, got %T", err)
				}
				if se.Provider != c.provider || se.Code != c.code || se.Body != c.body {
					t.Fatalf("status error = %+v", se)
				}
				return
			}
			if cle.Limit != c.limit || cle.Requested != c.requested {
				t.Fatalf("limit/requested = %d/%d, want %d/%d", cle.Limit, cle.Requested, c.limit, c.requested)
			}
			if cle.Provider != c.provider || cle.Code != c.code || cle.Body != c.body {
				t.Fatalf("embedded status error = %+v", cle.StatusError)
			}
		})
	}
}

func TestContextLengthErrorUnwrapsStatusError(t *testing.T) {
	err := llm.NewStatusError("openaicompat", 400, "maximum context length is 100 tokens, however you requested 200 tokens")
	var cle *llm.ContextLengthError
	if !errors.As(err, &cle) {
		t.Fatalf("want *llm.ContextLengthError, got %T", err)
	}
	var se *llm.StatusError
	if !errors.As(err, &se) {
		t.Fatal("errors.As should find the embedded *llm.StatusError")
	}
	want := "openaicompat: status 400: maximum context length is 100 tokens, however you requested 200 tokens"
	if err.Error() != want || se.Error() != want {
		t.Fatalf("Error() = %q, StatusError.Error() = %q, want %q", err.Error(), se.Error(), want)
	}
	if cle.Unwrap() != se {
		t.Fatal("Unwrap should return the embedded StatusError")
	}
}

func TestIsRetryableExcludesContextLength(t *testing.T) {
	cle := &llm.ContextLengthError{
		StatusError: &llm.StatusError{Provider: "test", Code: 429, Body: "maximum context length is 10 tokens"},
		Limit:       10,
	}
	if llm.IsRetryable(cle) {
		t.Fatal("context length error must not be retryable")
	}
	if llm.IsRetryable(fmt.Errorf("wrap: %w", cle)) {
		t.Fatal("wrapped context length error must not be retryable")
	}
}
