// Command subagent is a tutorial for sub-agent context isolation (the
// deep-agents "quarantine" model). agent.AsTool wraps a child agent as a tool:
// the child runs in a FRESH isolated context (only the task as input) and
// returns ONLY its final text — its intermediate reasoning and tool calls never
// pollute the orchestrator's context. Contrast with transfer (examples/
// multiagent), which hands over the whole conversation.
//
// Driven by a real Agnes chat model (OpenAI-compatible).
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/subagent
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

	// 两个专才子 agent。它们各自可能多步思考/用工具,但对外只交付一段最终文本。
	researcher, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是调研员。就给定主题列出 3 条关键事实,每条一句话。"),
	)
	critic, _ := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是审稿人。指出给定文字的一个最重要改进点,一句话。"),
	)

	// 编排者:把两个子 agent 当作隔离工具挂上。它自己决定先调研、再让审稿、最后成文;
	// 子 agent 的中间过程不会进入编排者的上下文(隔离),只回最终结论。
	orchestrator, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是主笔。先用 research 工具调研主题,据此写一段 80 字简介,"+
			"再用 critique 工具征求改进意见并据此润色,最后输出成品。"),
		agent.WithTools(
			agent.AsTool(researcher, "research", "把一个调研任务交给隔离的调研子 agent,返回要点"),
			agent.AsTool(critic, "critique", "把一段文字交给隔离的审稿子 agent,返回改进意见"),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	section("主笔编排两个隔离子 agent 完成一篇简介")
	for ev, err := range orchestrator.Stream(ctx, "主题:Go 的垃圾回收").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			// 每个子 agent 调用回来的,只是它的最终文本(隔离边界)。
			fmt.Printf("   [子 agent %s] %s\n", e.Result.Name, firstLine(e.Result.Content[0].(core.Text).Text))
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("🤖", t)
			}
		}
	}
}

func firstLine(s string) string {
	for i, r := range s {
		if r == '\n' {
			return s[:i] + " …"
		}
	}
	return s
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

func section(title string) { fmt.Printf("\n========== %s ==========\n", title) }
