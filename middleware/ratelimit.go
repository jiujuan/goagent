package middleware

import (
	"context"
	"iter"
	"sync"
	"time"

	"github.com/jiujuan/goagent/llm"
)

// RateLimitOptions configures the RateLimit middleware.
type RateLimitOptions struct {
	// RPS caps requests per second (a minimum interval between calls). 0 = no
	// rate cap.
	RPS float64
	// MaxConcurrent caps simultaneous in-flight calls. 0 = unlimited.
	MaxConcurrent int
}

// RateLimit throttles model calls: it enforces a minimum interval between
// calls (RPS) and/or a cap on concurrent in-flight calls. Both bounds honor
// context cancellation while waiting. The concurrency slot is held for the
// whole streamed response and released when the stream ends.
func RateLimit(opts *RateLimitOptions) Middleware {
	lim := &limiter{}
	if opts != nil {
		if opts.RPS > 0 {
			lim.interval = time.Duration(float64(time.Second) / opts.RPS)
		}
		if opts.MaxConcurrent > 0 {
			lim.sem = make(chan struct{}, opts.MaxConcurrent)
		}
	}

	return func(next llm.Model) llm.Model {
		return Wrap(next, func(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
			return func(yield func(*llm.Response, error) bool) {
				release, err := lim.acquire(ctx)
				if err != nil {
					yield(nil, err)
					return
				}
				defer release()
				for resp, err := range next.Generate(ctx, req) {
					if !yield(resp, err) {
						return
					}
				}
			}
		})
	}
}

// limiter combines an optional minimum-interval rate cap with an optional
// concurrency semaphore.
type limiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
	sem      chan struct{}
}

// acquire blocks until a slot and the rate window are available, returning a
// release function. It returns ctx.Err() if cancelled while waiting.
func (l *limiter) acquire(ctx context.Context) (func(), error) {
	if l.sem != nil {
		select {
		case l.sem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	release := func() {
		if l.sem != nil {
			<-l.sem
		}
	}

	if l.interval > 0 {
		l.mu.Lock()
		now := time.Now()
		if l.next.Before(now) {
			l.next = now
		}
		wait := l.next.Sub(now)
		l.next = l.next.Add(l.interval)
		l.mu.Unlock()

		if wait > 0 {
			t := time.NewTimer(wait)
			defer t.Stop()
			select {
			case <-t.C:
			case <-ctx.Done():
				release()
				return nil, ctx.Err()
			}
		}
	}
	return release, nil
}
