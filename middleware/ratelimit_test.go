package middleware_test

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
)

func TestRateLimitMinInterval(t *testing.T) {
	base := mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") })
	model := middleware.Chain(base, middleware.RateLimit(&middleware.RateLimitOptions{RPS: 100})) // 10ms apart

	start := time.Now()
	const n = 5
	for range n {
		for _, err := range model.Generate(context.Background(), &llm.Request{}) {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	// 5 calls at >=10ms spacing → at least ~40ms total (4 gaps).
	if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
		t.Fatalf("rate limit not enforced: %v elapsed for %d calls", elapsed, n)
	}
}

func TestRateLimitMaxConcurrent(t *testing.T) {
	var inflight, peak int32
	base := mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("ok") })
	// Wrap base with a tracker that records concurrent in-flight calls inside
	// the rate-limited critical section.
	tracked := middleware.Wrap(base, func(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
		return func(yield func(*llm.Response, error) bool) {
			cur := atomic.AddInt32(&inflight, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&inflight, -1)
			for resp, err := range base.Generate(ctx, req) {
				if !yield(resp, err) {
					return
				}
			}
		}
	})
	model := middleware.Chain(tracked, middleware.RateLimit(&middleware.RateLimitOptions{MaxConcurrent: 2}))

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, err := range model.Generate(context.Background(), &llm.Request{}) {
				_ = err
			}
		}()
	}
	wg.Wait()

	if peak > 2 {
		t.Fatalf("concurrency cap exceeded: peak %d > 2", peak)
	}
	if peak == 0 {
		t.Fatal("tracker never ran")
	}
}
