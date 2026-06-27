// Command redis collects the Redis-backed scheduler demos. All need a Redis
// server; pick a demo with the first argument:
//
//	export REDIS_URL=redis://localhost:6379/0
//	go run ./examples/redis queue   # durable job queue (producer + worker pool)
//	go run ./examples/redis bus     # cross-process progress bus (publish/subscribe)
//	go run ./examples/redis full    # agent run in a "worker", progress bridged to a "frontend"
//
// In production each side is a SEPARATE process sharing the same Redis; these
// programs run both sides in one process so each is a single runnable demo.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/scheduler"
)

func main() {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		fmt.Println("请先设置 REDIS_URL,如:")
		fmt.Println("  export REDIS_URL=redis://localhost:6379/0")
		usage()
		return
	}
	cmd := "full"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "queue":
		demoQueue(url)
	case "bus":
		demoBus(url)
	case "full":
		demoFull(url)
	default:
		usage()
	}
}

func usage() {
	fmt.Println("用法: go run ./examples/redis [queue|bus|full]")
}

// --- demo 1: 持久任务队列 ----------------------------------------------------
//
// 生产者把可序列化任务(Type+Payload)写入 Redis Stream;工作池用 Registry 重建
// 并执行。任务在 Redis 里,崩溃可恢复,多 worker 进程可共享同一队列。
func demoQueue(url string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q, c, err := scheduler.New(scheduler.WithRedis(url), scheduler.WithGroup("demo-queue"))
	if err != nil {
		log.Fatal(err)
	}

	section("demo: 持久任务队列(Redis Streams)")
	for _, name := range []string{"小明", "小红", "小刚"} {
		if err := q.Enqueue(ctx, scheduler.Job{
			ID: core.NewID("job"), Type: "greet", Payload: []byte(name),
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("📥 入队 greet:", name)
	}

	done := make(chan struct{}, 3)
	pool := scheduler.NewPool(c, 2).WithRegistry(scheduler.Registry{
		"greet": func(ctx context.Context, payload []byte) error {
			a, _ := agent.New(agent.WithModel(greetModel()))
			ans, err := a.Run(ctx, string(payload))
			if err == nil {
				fmt.Println("✅", ans)
				done <- struct{}{}
			}
			return err
		},
	})
	go pool.Run(ctx)

	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
}

// --- demo 2: 跨进程进度总线 --------------------------------------------------
//
// 一端订阅某 key 的事件,另一端发布。事件经 core.MarshalEvent 序列化走 Redis
// Pub/Sub,所以发布方和订阅方可以是不同进程。
func demoBus(url string) {
	b, err := scheduler.NewBus(scheduler.WithRedis(url))
	if err != nil {
		log.Fatal(err)
	}
	section("demo: 跨进程进度总线(Redis Pub/Sub)")

	const key = "session-1"
	ch, cancel := b.Subscribe(key) // 模拟前端进程
	defer cancel()
	time.Sleep(200 * time.Millisecond) // 确保订阅已建立(pub/sub 不回放历史)

	// 模拟 worker 进程发布事件。
	b.Publish(key, core.RunStarted{RunID: "r1", ThreadID: key})
	b.Publish(key, core.MessageDone{Message: core.AssistantText("处理中…")})
	b.Publish(key, core.RunDone{Result: core.Result{Message: core.AssistantText("完成")}})

	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev := <-ch:
			fmt.Printf("📡 收到事件: %T\n", ev)
			if _, ok := ev.(core.RunDone); ok {
				return
			}
		case <-timeout:
			return
		}
	}
}

// --- demo 3: agent 跑在 worker,进度桥接给 frontend --------------------------
//
// 完整故事:worker 跑一个 agent,用 scheduler.Bridge 把它的事件镜像到 Redis 总线;
// frontend(另一进程)订阅该 key,实时看到 agent 的流式进度。
func demoFull(url string) {
	b, err := scheduler.NewBus(scheduler.WithRedis(url))
	if err != nil {
		log.Fatal(err)
	}
	section("demo: agent 运行 + 跨进程进度桥接")
	const key = "job-xyz"

	// frontend:订阅进度。
	ch, cancel := b.Subscribe(key)
	defer cancel()
	frontendDone := make(chan struct{})
	go func() {
		defer close(frontendDone)
		for ev := range ch {
			switch e := ev.(type) {
			case core.MessageDelta:
				fmt.Print(e.Delta.Text())
			case core.RunDone:
				fmt.Println("\n[frontend] 收到完成事件")
				return
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)

	// worker:跑 agent,把它的事件桥接到 Redis 总线。
	a, _ := agent.New(
		agent.WithModel(streamingModel()),
		agent.WithModelOptions(llm.WithStream(true)),
	)
	run := a.Stream(context.Background(), "讲个一句话的小故事")
	evch, ecancel := run.Events(bus.Lossy)
	defer ecancel()
	go scheduler.Bridge(b, key, evch)

	if _, err := run.Wait(); err != nil {
		log.Fatal(err)
	}
	select {
	case <-frontendDone:
	case <-time.After(3 * time.Second):
	}
}

// --- mock models ------------------------------------------------------------

func greetModel() llm.Model {
	return mock.New("m", func(req *llm.Request) *llm.Response {
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role == core.RoleUser {
				return mock.Text("你好," + req.Messages[i].Text() + "!")
			}
		}
		return mock.Text("你好!")
	})
}

func streamingModel() llm.Model {
	// Emits a few partial chunks then a final message.
	return mock.NewStream("m", func(*llm.Request) []*llm.Response {
		return []*llm.Response{
			mock.Partial("从前"),
			mock.Partial("有座山,"),
			mock.Partial("山里讲着故事。"),
			mock.Text("从前有座山,山里讲着故事。"),
		}
	})
}

func section(title string) { fmt.Printf("\n========== %s ==========\n", title) }
