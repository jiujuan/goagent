// Command quickstart-chat is the smallest real-model starter: build an agent
// and ask it one question. Copy this file and swap in your model.
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/quickstart-chat
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
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
		agent.WithInstruction("你是一个乐于助人的中文助手。"),
	)
	if err != nil {
		log.Fatal(err)
	}

	answer, err := a.Run(context.Background(), "用一句话介绍 Go 语言。")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(answer)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
