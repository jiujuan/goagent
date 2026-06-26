// Command multiagent demonstrates LLM-driven delegation: a router agent
// auto-advertises a transfer_to_agent tool (because it has sub-agents) and
// hands the request to the specialist best suited to answer. Uses the mock
// provider, so no API key is required.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
)

func main() {
	weatherExpert := agent.New(agent.Config{
		Name:        "weather_expert",
		Description: "回答天气相关问题",
		Model: mock.New("weather", func(*llm.Request) *llm.Response {
			return mock.Text("今天北京晴，25°C，适合出门。")
		}),
	})

	mathExpert := agent.New(agent.Config{
		Name:        "math_expert",
		Description: "解决数学计算问题",
		Model: mock.New("math", func(*llm.Request) *llm.Response {
			return mock.Text("42。")
		}),
	})

	router := agent.New(agent.Config{
		Name:        "router",
		Description: "把用户问题路由到合适的专家",
		Instruction: "根据问题类型转交给合适的专家。",
		SubAgents:   []agent.Agent{weatherExpert, mathExpert},
		Model: mock.New("router", func(*llm.Request) *llm.Response {
			// 一个真实模型会读 user 问题决定目标；这里固定转交天气专家。
			return mock.CallTool("t1", "transfer_to_agent", `{"agent_name":"weather_expert"}`)
		}),
	})

	r := runner.New(runner.Config{Root: router})
	for ev, err := range r.Run(context.Background(), "u1", "s1", core.UserText("北京天气怎么样？")) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.Message == nil {
			continue
		}
		switch ev.Message.Role {
		case core.RoleUser:
			fmt.Printf("👤 user: %s\n", ev.Message.Text())
		case core.RoleAssistant:
			if calls := ev.Message.ToolCalls(); len(calls) > 0 {
				fmt.Printf("🧭 %s: 转交 → %s\n", ev.Author, string(calls[0].Args))
				continue
			}
			fmt.Printf("🤖 %s: %s\n", ev.Author, ev.Message.Text())
		case core.RoleTool:
			fmt.Printf("🔧 (transfer 已执行)\n")
		}
	}
}
