package middleware

import (
	"context"
	"sync"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
)

// RateLimitOptions configures RateLimit.
type RateLimitOptions struct {
	// RPS is the sustained model-calls-per-second (default 1).
	RPS float64
	// Burst is the bucket capacity — how many calls may happen back-to-back
	// before throttling kicks in (default 1).
	Burst int
}

// RateLimit throttles model calls with a token bucket: BeforeModel blocks until
// a token is available (respecting context cancellation). Leak-free because it
// holds no resource past the hook.
func RateLimit(o RateLimitOptions) agent.Middleware {
	if o.RPS <= 0 {
		o.RPS = 1
	}
	burst := float64(o.Burst)
	if burst < 1 {
		burst = 1
	}
	return &rateLimit{b: &bucket{rps: o.RPS, max: burst, tokens: burst, last: time.Now()}}
}

type rateLimit struct {
	agent.BaseMiddleware
	b *bucket
}

func (r *rateLimit) BeforeModel(lc *agent.LoopContext) (core.Directive, error) {
	return core.Directive{}, r.b.wait(lc.Context)
}

type bucket struct {
	mu     sync.Mutex
	tokens float64
	rps    float64
	max    float64
	last   time.Time
}

func (b *bucket) wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		now := time.Now()
		b.tokens = min(b.max, b.tokens+now.Sub(b.last).Seconds()*b.rps)
		b.last = now
		if b.tokens >= 1 {
			b.tokens -= 1
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - b.tokens) / b.rps * float64(time.Second))
		b.mu.Unlock()

		if ctx == nil {
			time.Sleep(wait)
			continue
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
