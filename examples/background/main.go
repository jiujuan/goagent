// Command background demonstrates offline queue execution with a bounded Pool.
// Handle.Wait only awaits execution; WaitRecorded also awaits the worker's
// terminal-record attempt. Neither method starts a job. The mock makes no
// network calls, and the example waits for workers to exit before returning.
//
//	go run ./examples/background
//
// See examples/background-status for file-backed terminal records and RunStatus,
// or examples/lazy-run-lifecycle for channel-controlled lifecycle assertions.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/queue"
)

func main() {
	if err := demonstrate(); err != nil {
		log.Fatal(err)
	}
}

func demonstrate() error {
	ctx := context.Background()
	a, err := agent.New(agent.WithModel(mock.New("offline-echo", func(req *llm.Request) *llm.Response {
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role == "user" {
				return mock.Text("处理完成:" + req.Messages[i].Text())
			}
		}
		return mock.Text("done")
	})))
	if err != nil {
		return err
	}

	q := queue.NewMemQueue(16)
	pool := queue.NewPool(q, 2)
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		pool.Run(ctx)
	}()
	// On every return, close the queue and join the workers. Wait alone does
	// not confirm a terminal record, nor does it guarantee the Pool has exited.
	defer func() {
		q.Close()
		<-workersDone
	}()

	tasks := []string{"任务A", "任务B", "任务C", "任务D", "任务E"}
	var handles []*queue.Handle
	for i, task := range tasks {
		h, err := queue.EnqueueAgent(ctx, q, a, task, agent.OnThread(fmt.Sprintf("job-%d", i)))
		if err != nil {
			return err
		}
		handles = append(handles, h)
		fmt.Printf("已入队 %s (job %s)\n", task, h.ID)
	}
	q.Close()

	fmt.Println("--- 后台处理中（并发 2）---")
	for _, h := range handles {
		res, err := h.Wait() // Pure wait; the Pool above owns starting the work.
		if err != nil {
			return fmt.Errorf("execution %s: %w", h.ID, err)
		}
		fmt.Println("执行已结算:", res.Message.Text())
		if _, err := h.WaitRecorded(); err != nil {
			// Includes both runErr and recordErr; a completed run with nil
			// error here confirms the record in this example's memory store.
			return fmt.Errorf("record %s: %w", h.ID, err)
		}
		fmt.Println("终态记录已确认（内存存储）:", h.ID)
	}
	return nil
}
