// Command prompt is a tutorial for the composable system-prompt builder
// (WithPrompt): instead of one static instruction string, assemble the system
// prompt from ordered, reusable Sections — Identity, Environment, ToolGuidance,
// SessionState, and your own.
//
//	export AGNES_API_KEY=sk-...
//	export AGNES_MODEL=gemini-2.5-flash
//	go run ./examples/prompt
//
// 没设 AGNES_API_KEY 时,程序只打印用法后退出。
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm/openaicompat"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		fmt.Println("请先设置 AGNES_API_KEY(和可选 AGNES_MODEL)。")
		return
	}
	model := openaicompat.Agnes(envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1"),
		envOr("AGNES_MODEL", "gemini-2.5-flash"), key)

	weather := tool.New("get_weather", "查询某城市天气",
		func(_ *tool.Context, in struct {
			City string `json:"city" desc:"城市名"`
		}) (string, error) {
			return in.City + ":晴 25°C", nil
		})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(weather),
		// system prompt 由有序 Section 组合而成(可丢空、可按名覆盖)。
		agent.WithPrompt(prompt.New().
			Add(prompt.Identity("你是一个友好的天气助手,回答简洁。")). // 100
			Add(prompt.Environment()).                 // 200:日期/OS/cwd
			Add(prompt.ToolGuidance()).                // 300:自动列出工具
			Add(prompt.SectionFunc{                    // 自定义 Section,插在 250
				SecName: "policy", SecOrder: 250,
				RenderFn: func(prompt.Context) (string, error) {
					return "# 规则\n只回答天气相关问题。", nil
				},
			})),
	)
	if err != nil {
		log.Fatal(err)
	}

	answer, err := a.Run(context.Background(), "北京天气怎么样?")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("🤖", answer)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
