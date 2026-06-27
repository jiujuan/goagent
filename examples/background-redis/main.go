// Command background-redis demonstrates the durable, cross-process scheduler
// backend: a Redis Streams job queue with a worker pool. Because a Run closure
// cannot cross a process boundary, Redis jobs carry Type+Payload and the worker
// rebuilds them from a Registry of Handlers.
//
// Needs a Redis server:
//
//	export REDIS_URL=redis://localhost:6379/0
//	go run ./examples/background-redis
//
// In production the producer (Enqueue) and the worker (Pool.Run) are usually
// SEPARATE processes sharing the same stream; this single program does both so
// it is runnable as one demo.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/scheduler"
)

func main() {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		fmt.Println("请设置 REDIS_URL(如 redis://localhost:6379/0)再运行。")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Durable Redis-backed queue (same value is producer + consumer here).
	q, c, err := scheduler.New(scheduler.WithRedis(url), scheduler.WithGroup("demo"))
	if err != nil {
		log.Fatal(err)
	}

	// Producer: enqueue serializable jobs (Type + Payload). Survives restarts.
	for _, name := range []string{"小明", "小红", "小刚"} {
		if err := q.Enqueue(ctx, scheduler.Job{
			ID: core.NewID("job"), Type: "greet", Payload: []byte(name),
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("📥 入队 greet:", name)
	}

	// Worker: a Registry rebuilds each Type's work. Here "greet" runs a mock agent.
	pool := scheduler.NewPool(c, 2).WithRegistry(scheduler.Registry{
		"greet": func(ctx context.Context, payload []byte) error {
			a, _ := agent.New(agent.WithModel(mock.New("m", func(req *llm.Request) *llm.Response {
				return mock.Text("你好," + lastUser(req) + "!")
			})))
			ans, err := a.Run(ctx, string(payload))
			if err != nil {
				return err
			}
			fmt.Println("✅", ans)
			return nil
		},
	})
	fmt.Println("--- worker 处理中(并发 2)---")
	go pool.Run(ctx)

	<-ctx.Done() // let the worker drain (Ctrl-C or timeout to stop)
}

func lastUser(req *llm.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == core.RoleUser {
			return req.Messages[i].Text()
		}
	}
	return ""
}
