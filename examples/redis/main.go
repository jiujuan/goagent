// Command redis collects the Redis-backed queue demos. All need a Redis
// server; pick a demo with the first argument:
//
//	export REDIS_URL=redis://localhost:6379/0   # 或 GOAGENT_REDIS_URL,或写进 config.yaml
//	go run ./examples/redis queue   # durable job queue (producer + worker pool)
//	go run ./examples/redis bus     # cross-process progress bus (publish/subscribe)
//	go run ./examples/redis full    # agent run in a "worker", progress bridged to a "frontend"
//
// 配置经 config 包加载:Redis URL 与 queue 旋钮(stream/group/重试阈值…)都来自
// config(默认值 < config.yaml < 环境变量),再由本文件的 queueOpts 映射到 queue
// 的函数式 Option。config 不 import queue——值单向流入,DAG 不变。
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
	"github.com/jiujuan/goagent/config"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/queue"
)

func main() {
	// 一次加载。Redis URL 与 queue 旋钮都从这里来(默认 redis://localhost:6379,
	// 可被 GOAGENT_REDIS_URL / 旧 REDIS_URL / config.yaml 覆盖)。
	cfg := config.MustLoad()
	fmt.Printf("使用 Redis: %s  (覆盖: REDIS_URL / GOAGENT_REDIS_URL / config.yaml)\n", cfg.Redis.URL)

	cmd := "full"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "queue":
		demoQueue(cfg)
	case "bus":
		demoBus(cfg)
	case "full":
		demoFull(cfg)
	default:
		usage()
	}
}

func usage() {
	fmt.Println("用法: go run ./examples/redis [queue|bus|full]")
}

// queueOpts 把 config 里的 queue 旋钮映射成 queue 的函数式 Option。这一步刻意放在
// 调用方(main 包):config 只产出强类型值,不 import queue;由这里把"值"装配成
// "构造参数"。只有非零字段才下发,空值留给 queue 自己的默认(如 DeadStream)。
func queueOpts(cfg *config.Config) []queue.Option {
	opts := []queue.Option{queue.WithRedis(cfg.Redis.URL)}
	q := cfg.Queue
	if q.Stream != "" {
		opts = append(opts, queue.WithStream(q.Stream))
	}
	if q.Group != "" {
		opts = append(opts, queue.WithGroup(q.Group))
	}
	if q.DeadStream != "" {
		opts = append(opts, queue.WithDeadStream(q.DeadStream))
	}
	if q.IdleThreshold > 0 {
		opts = append(opts, queue.WithIdleThreshold(q.IdleThreshold))
	}
	if q.MaxDeliveries > 0 {
		opts = append(opts, queue.WithMaxDeliveries(q.MaxDeliveries))
	}
	if q.MaxLen > 0 {
		opts = append(opts, queue.WithMaxLen(q.MaxLen))
	}
	return opts
}

// --- demo 1: 持久任务队列 ----------------------------------------------------
//
// 生产者把可序列化任务(Type+Payload)写入 Redis Stream;工作池用 Registry 重建
// 并执行。任务在 Redis 里,崩溃可恢复,多 worker 进程可共享同一队列。
func demoQueue(cfg *config.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// config 提供的旋钮 + 本 demo 专属覆盖(WithGroup 后置 → 覆盖 config 默认组)。
	q, c, err := queue.New(append(queueOpts(cfg), queue.WithGroup("demo-queue"))...)
	if err != nil {
		log.Fatal(err)
	}

	section("demo: 持久任务队列(Redis Streams)")
	for _, name := range []string{"小明", "小红", "小刚"} {
		if err := q.Enqueue(ctx, queue.Job{
			ID: core.NewID("job"), Type: "greet", Payload: []byte(name),
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("📥 入队 greet:", name)
	}

	done := make(chan struct{}, 3)
	pool := queue.NewPool(c, 2).WithRegistry(queue.Registry{
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
func demoBus(cfg *config.Config) {
	b, err := queue.NewBus(queue.WithRedis(cfg.Redis.URL))
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
// 完整故事:worker 跑一个 agent,用 queue.Bridge 把它的事件镜像到 Redis 总线;
// frontend(另一进程)订阅该 key,实时看到 agent 的流式进度。
func demoFull(cfg *config.Config) {
	b, err := queue.NewBus(queue.WithRedis(cfg.Redis.URL))
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
	go queue.Bridge(b, key, evch)

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
