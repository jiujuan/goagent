// Command background is a tutorial for the scheduler: fire-and-forget agent runs
// processed by a bounded worker pool, off the caller's goroutine. Uses the mock
// provider, so it runs offline.
//
//	go run ./examples/background
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/scheduler"
)

func main() {
	ctx := context.Background()

	// A worker that echoes its input (so we can tell jobs apart).
	a, err := agent.New(agent.WithModel(mock.New("m", func(req *llm.Request) *llm.Response {
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role == "user" {
				return mock.Text("处理完成:" + req.Messages[i].Text())
			}
		}
		return mock.Text("done")
	})))
	if err != nil {
		log.Fatal(err)
	}

	// Queue + bounded pool (2 at a time).
	q := scheduler.NewMemQueue(16)
	pool := scheduler.NewPool(q, 2)

	// Fire-and-forget several agent runs; each returns a Handle immediately.
	tasks := []string{"任务A", "任务B", "任务C", "任务D", "任务E"}
	var handles []*scheduler.Handle
	for i, task := range tasks {
		h, err := scheduler.EnqueueAgent(ctx, q, a, task, agent.OnThread(fmt.Sprintf("job-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
		handles = append(handles, h)
		fmt.Printf("📥 已入队 %s(job %s)\n", task, h.ID)
	}
	q.Close() // no more jobs; pool returns once drained

	// Run the pool in the background; collect results.
	go pool.Run(ctx)

	fmt.Println("--- 后台处理中(并发 2)---")
	for _, h := range handles {
		res, err := h.Wait()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("✅", res.Message.Text())
	}
}
