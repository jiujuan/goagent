// Command pipeline is a tutorial for passing structured output between workflow
// stages with WithOutputKey + {{key}} templating. Where examples/workflow lets
// stages share the conversation implicitly, this shows EXPLICIT hand-off: a
// stage writes its final answer into State.KV under a key, and a later stage
// references it in its instruction via a {{key}} placeholder.
//
// Driven by a real Agnes chat model (OpenAI-compatible).
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/pipeline
//
// 没设 AGNES_API_KEY 时,程序只打印用法后退出。
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
	model, ok := buildModel()
	if !ok {
		fmt.Println("请先设置 AGNES_API_KEY(和可选 AGNES_MODEL / AGNES_BASE_URL)再运行。")
		return
	}
	ctx := context.Background()

	// 阶段 1:策划 → 把要点写进 state["plan"]。
	planner, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是策划。把用户主题拆成 3 个要点,用分号分隔成一行。"),
		agent.WithOutputKey("plan"),
	)

	// 阶段 2:据 {{plan}} 成文 → 写进 state["draft"]。
	writer, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是作者。严格依据以下要点写一段 80 字简介:\n要点:{{plan}}"),
		agent.WithOutputKey("draft"),
	)

	// 阶段 3:据 {{draft}} 润色为成品。
	polisher, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是润色编辑。把下面的草稿改得更简洁有力,直接输出成品:\n草稿:{{draft}}"),
	)

	pipe := agent.Sequential("report", planner, writer, polisher)

	fmt.Println("主题:Go 的接口设计")
	fmt.Println()
	for ev, err := range pipe.Stream(ctx, "主题:Go 的接口设计").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		if e, ok := ev.(core.MessageDone); ok {
			if t := e.Message.Text(); t != "" {
				fmt.Println("· 阶段产出:", t)
			}
		}
	}
}

func buildModel() (llm.Model, bool) {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		return nil, false
	}
	base := envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1")
	model := envOr("AGNES_MODEL", "gemini-2.5-flash")
	return openaicompat.Agnes(base, model, key), true
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
