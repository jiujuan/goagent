package scheduler_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/scheduler"
)

func TestPoolRunsAllJobsBounded(t *testing.T) {
	q := scheduler.NewMemQueue(64)
	var ran, peak, cur int64

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		_ = q.Enqueue(context.Background(), scheduler.Job{
			Run: func(context.Context) error {
				c := atomic.AddInt64(&cur, 1)
				for { // track peak concurrency
					p := atomic.LoadInt64(&peak)
					if c <= p || atomic.CompareAndSwapInt64(&peak, p, c) {
						break
					}
				}
				time.Sleep(2 * time.Millisecond)
				atomic.AddInt64(&cur, -1)
				atomic.AddInt64(&ran, 1)
				wg.Done()
				return nil
			},
		})
	}
	q.Close()

	pool := scheduler.NewPool(q, 4)
	go pool.Run(context.Background())
	wg.Wait()

	if atomic.LoadInt64(&ran) != n {
		t.Fatalf("ran %d jobs, want %d", ran, n)
	}
	if p := atomic.LoadInt64(&peak); p > 4 {
		t.Fatalf("peak concurrency %d exceeded limit 4", p)
	}
}

func TestEnqueueAgentBackgroundRun(t *testing.T) {
	ctx := context.Background()
	a, err := agent.New(agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
		return mock.Text("背景任务完成")
	})))
	if err != nil {
		t.Fatal(err)
	}

	q := scheduler.NewMemQueue(8)
	h, err := scheduler.EnqueueAgent(ctx, q, a, "go", agent.OnThread("t1"))
	if err != nil {
		t.Fatal(err)
	}
	q.Close()

	pool := scheduler.NewPool(q, 2)
	go pool.Run(ctx)

	res, err := h.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "背景任务完成" {
		t.Fatalf("background result = %q", res.Message.Text())
	}
}
