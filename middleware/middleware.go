// Package middleware provides composable cross-cutting capabilities for the
// turn engine, expressed uniformly as model decorators. A Middleware wraps an
// llm.Model, intercepting its Generate call: request transformers (compaction,
// RAG) rewrite the request before delegating; call controllers (retry, rate
// limit) govern how and when the call runs. The agent applies them with Chain.
//
// This is the "capability as middleware" idea: features layer onto the model
// rather than being hardcoded into the loop.
package middleware

import (
	"context"
	"iter"

	"github.com/jiujuan/goagent/llm"
)

// Middleware decorates an llm.Model.
type Middleware func(next llm.Model) llm.Model

// Chain wraps base with the given middlewares. The FIRST listed is the
// outermost (it runs first on the way in, last on the way out). Recommended
// ordering, outer→inner: RateLimit, Compaction, RAG, Retry — so rate limiting
// gates everything and retry re-runs only the bare model call.
func Chain(base llm.Model, mws ...Middleware) llm.Model {
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] != nil {
			base = mws[i](base)
		}
	}
	return base
}

// Wrap builds an llm.Model from a custom generate function, inheriting next's
// name. It is the building block for writing middlewares.
func Wrap(next llm.Model, gen func(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error]) llm.Model {
	return wrapped{name: next.Name(), gen: gen}
}

type wrapped struct {
	name string
	gen  func(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error]
}

func (w wrapped) Name() string { return w.name }
func (w wrapped) Generate(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return w.gen(ctx, req)
}

// BeforeModel builds a Middleware from a function that rewrites the request
// before each model call. If fn returns an error, the call fails without
// reaching the model. Compaction and RAG are built on this.
func BeforeModel(fn func(ctx context.Context, req *llm.Request) error) Middleware {
	return func(next llm.Model) llm.Model {
		return Wrap(next, func(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
			if err := fn(ctx, req); err != nil {
				return FailStream(err)
			}
			return next.Generate(ctx, req)
		})
	}
}

// FailStream returns a response stream that yields a single error.
func FailStream(err error) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		yield(nil, err)
	}
}
