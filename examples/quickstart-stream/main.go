// Command quickstart-stream is the smallest real-model starter that STREAMS the
// answer token by token. Open SSE streaming with WithModelOptions, then range
// the run's events and print each MessageDelta. Creating Stream is lazy; consuming
// Iter subscribes before starting. This real-provider template is not an offline
// lifecycle check; use examples/lazy-run-lifecycle for that.
//
//	export AGNES_API_KEY=sk-...
//	go run ./examples/quickstart-stream
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/openaicompat"
)

func main() {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		fmt.Println("请先设置 AGNES_API_KEY(和可选 AGNES_MODEL)。")
		return
	}
	model := openaicompat.Agnes(envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1"),
		envOr("AGNES_MODEL", "gemini-2.5-flash"), key)

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是一个讲故事的人。"),
		agent.WithModelOptions(llm.WithStream(true)), // 打开 SSE 流式
	)
	if err != nil {
		log.Fatal(err)
	}

	run := a.Stream(context.Background(), "用三句话讲个温暖的小故事。")
	defer run.Cancel()
	// Iter 消费时先订阅再 Start。提前 break 会自动退订，但不会取消执行；
	// 如果也要停止工作，调用 run.Cancel()，再 Await 等实际结算。
	// 原始 Events 只订阅，必须自行 Start，并立即 defer unsubscribe()。
	for ev, err := range run.Iter() {
		if err != nil {
			log.Print(err) // 返回前执行上面的 defer。
			return
		}
		if d, ok := ev.(core.MessageDelta); ok {
			fmt.Print(d.Delta.Text()) // 逐 token 打印
		}
	}
	if _, err := run.Await(context.Background()); err != nil {
		log.Print(err)
		return
	}
	fmt.Println()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
