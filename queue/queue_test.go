package queue_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/queue"
)

func TestPoolRunsAllJobsBounded(t *testing.T) {
	q := queue.NewMemQueue(64)
	var ran, peak, cur int64

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		_ = q.Enqueue(context.Background(), queue.Job{
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

	pool := queue.NewPool(q, 4)
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

	q := queue.NewMemQueue(8)
	h, err := queue.EnqueueAgent(ctx, q, a, "go", agent.OnThread("t1"))
	if err != nil {
		t.Fatal(err)
	}
	q.Close()

	pool := queue.NewPool(q, 2)
	go pool.Run(ctx)

	res, err := h.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "背景任务完成" {
		t.Fatalf("background result = %q", res.Message.Text())
	}
}

func TestRegistryHandlerForSerializableJob(t *testing.T) {
	// A Type+Payload job (the shape Redis uses) is run by the pool's Registry.
	got := make(chan string, 1)
	q := queue.NewMemQueue(4)
	_ = q.Enqueue(context.Background(), queue.Job{
		ID: "j1", Type: "echo", Payload: []byte("hello"),
	})
	q.Close()

	pool := queue.NewPool(q, 1).WithRegistry(queue.Registry{
		"echo": func(_ context.Context, payload []byte) error {
			got <- string(payload)
			return nil
		},
	})
	go pool.Run(context.Background())

	select {
	case v := <-got:
		if v != "hello" {
			t.Fatalf("handler got %q, want hello", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestNewInProcessBackend(t *testing.T) {
	q, c, err := queue.New() // no WithRedis -> MemQueue
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	_ = q.Enqueue(context.Background(), queue.Job{Run: func(context.Context) error {
		close(done)
		return nil
	}})
	if mq, ok := c.(*queue.MemQueue); ok {
		mq.Close()
	}
	go queue.NewPool(c, 1).Run(context.Background())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("in-process job not run")
	}
}
