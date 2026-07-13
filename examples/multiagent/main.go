// Command multiagent is a tutorial for LLM-driven delegation: a router agent
// with sub-experts. When you give an agent sub-agents (WithSubAgents), goagent
// auto-injects a transfer_to_agent tool; the model decides whom to hand off to,
// and the delegate's reply becomes the run's answer (it continues the same
// conversation State).
//
// Driven by a real Agnes chat model (OpenAI-compatible).
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/multiagent
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

	// 两个领域专家(子 agent)。Name/Description 会被 transfer 工具用来让模型挑选。
	weather := expert(model, "weather_expert", "回答天气相关问题",
		"你是天气专家,只回答天气问题,简洁作答。")
	math := expert(model, "math_expert", "做数学计算与推理",
		"你是数学专家,只回答数学问题,给出步骤与结果。")

	// 路由 agent:有 SubAgents,于是自动获得 transfer_to_agent 工具。系统提示让它
	// 先判断领域、再转交,不自己回答。
	router, err := agent.New(
		agent.WithName("router"),
		agent.WithModel(model),
		agent.WithInstruction("你是一个分诊路由。判断用户问题属于天气还是数学,"+
			"调用 transfer_to_agent 转交给对应专家;不要自己回答专业问题。"),
		agent.WithSubAgents(weather, math),
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, q := range []string{
		"北京明天会下雨吗?",
		"123 乘以 456 等于多少?",
	} {
		section("用户:" + q)
		// 流式打印,能看到「→ 转交给 X」再看到专家的回答。
		for ev, err := range router.Stream(ctx, q).Iter() {
			if err != nil {
				log.Fatal(err)
			}
			switch e := ev.(type) {
			case core.ToolDone:
				if e.Result.Name == "transfer_to_agent" {
					fmt.Println("   [delegation]", e.Result.Content[0].(core.Text).Text)
				}
			case core.MessageDone:
				if t := e.Message.Text(); t != "" {
					fmt.Println("🤖", t)
				}
			}
		}
	}
}

func expert(model llm.Model, name, desc, instruction string) *agent.Agent {
	a, err := agent.New(
		agent.WithName(name),
		agent.WithDescription(desc),
		agent.WithModel(model),
		agent.WithInstruction(instruction),
	)
	if err != nil {
		log.Fatal(err)
	}
	return a
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
