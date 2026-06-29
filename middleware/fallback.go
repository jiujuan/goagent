package middleware

import (
	"context"
	"iter"

	"github.com/jiujuan/goagent/llm"
)

// FallbackOptions configures FallbackModel.
type FallbackOptions struct {
	// ShouldFallback decides whether a pre-stream error from one model should
	// trigger trying the next. Defaults to llm.IsRetryable (5xx/429/408 and
	// network errors fail over; 4xx and context cancellation do not).
	ShouldFallback func(error) bool
	// OnFallback, if set, is called each time the decorator gives up on one
	// model and moves to the next — useful for metrics/logging.
	OnFallback func(from, to string, err error)
}

// FallbackModel wraps an ordered list of models for provider failover: the
// first is primary, the rest are backups tried in order. It is a MODEL
// DECORATOR (like RetryModel), not a loop hook. Use it via WithModel:
//
//	model := middleware.FallbackModel(middleware.FallbackOptions{}, primary, backup)
//	agent.New(agent.WithModel(model))
//
// Failover is only possible BEFORE the chosen model streams any token: once
// output has been emitted, a later error is returned as-is (switching providers
// would duplicate output) — the same streaming constraint as RetryModel. Pair
// it with CircuitBreaker so a tripped primary fails fast and this decorator
// jumps to a backup without waiting on timeouts.
func FallbackModel(o FallbackOptions, models ...llm.Model) llm.Model {
	if len(models) == 0 {
		panic("middleware.FallbackModel requires at least one model")
	}
	if o.ShouldFallback == nil {
		o.ShouldFallback = llm.IsRetryable
	}
	return &fallbackModel{models: models, opts: o}
}

type fallbackModel struct {
	models []llm.Model
	opts   FallbackOptions
}

func (f *fallbackModel) Name() string { return f.models[0].Name() }

func (f *fallbackModel) Generate(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		for i, mdl := range f.models {
			yielded := false
			var failed error
			for resp, err := range mdl.Generate(ctx, req) {
				if err != nil {
					failed = err
					break
				}
				yielded = true
				if !yield(resp, nil) {
					return // consumer stopped
				}
			}
			if failed == nil {
				return // this model succeeded
			}
			// Mid-stream failure: output already emitted, cannot fail over.
			if yielded {
				yield(nil, failed)
				return
			}
			// Pre-stream failure: fail over only if there is a backup left and
			// the predicate says this error is worth it.
			last := i == len(f.models)-1
			if last || !f.opts.ShouldFallback(failed) {
				yield(nil, failed)
				return
			}
			if f.opts.OnFallback != nil {
				f.opts.OnFallback(mdl.Name(), f.models[i+1].Name(), failed)
			}
		}
	}
}
