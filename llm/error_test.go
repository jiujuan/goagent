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
