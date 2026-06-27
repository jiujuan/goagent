package middleware

import (
	"context"
	"iter"
	"time"

	"github.com/jiujuan/goagent/llm"
)

// RetryOptions configures RetryModel.
type RetryOptions struct {
	// MaxAttempts is the total number of tries (default 3).
	MaxAttempts int
	// BaseDelay is the first backoff delay; it doubles each retry (default 200ms).
	BaseDelay time.Duration
}

// RetryModel wraps an llm.Model so transient failures are retried with
// exponential backoff. It is a MODEL DECORATOR, not a loop hook — retry belongs
// around the bare model call. Use it via WithModel:
//
//	agent.New(agent.WithModel(middleware.RetryModel(real, middleware.RetryOptions{MaxAttempts: 3})))
//
// It only retries when the call fails BEFORE yielding any response in an
// attempt; once tokens have streamed out, a later error is returned as-is
// (re-running would duplicate output).
func RetryModel(m llm.Model, o RetryOptions) llm.Model {
	if o.MaxAttempts < 1 {
		o.MaxAttempts = 3
	}
	if o.BaseDelay <= 0 {
		o.BaseDelay = 200 * time.Millisecond
	}
	return &retryModel{inner: m, max: o.MaxAttempts, base: o.BaseDelay}
}

type retryModel struct {
	inner llm.Model
	max   int
	base  time.Duration
}

func (r *retryModel) Name() string { return r.inner.Name() }

func (r *retryModel) Generate(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		for attempt := 0; ; attempt++ {
			yielded := false
			var failed error
			for resp, err := range r.inner.Generate(ctx, req) {
				if err != nil {
					failed = err
					break
				}
				yielded = true
				if !yield(resp, nil) {
					return
				}
			}
			if failed == nil {
				return // success
			}
			if yielded || attempt >= r.max-1 {
				yield(nil, failed) // mid-stream failure or out of attempts
				return
			}
			delay := r.base << attempt
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			}
		}
	}
}
