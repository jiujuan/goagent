// Command prompt demonstrates the prompt.Builder: a system prompt assembled
// from four composable sections (Identity, Environment, ToolGuidance,
// SessionState) instead of a single static Instruction string. It runs one
// closed loop against the mock provider (no API key required).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

type weatherArgs struct {
	City string `json:"city" desc:"城市名"`
}

func main() {
	weather := tool.New("get_weather", "查询某城市的当前天气",
		func(_ *tool.Context, in weatherArgs) (string, error) {
			return in.City + "：晴，25°C", nil
		})

	// The model echoes the system prompt it received on its first call so the
	// composed prompt is visible, then drives a normal tool loop.
	model := mock.New("mock-opus", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("查到了，" + partsText(tr.Content) + "。")
		}
		fmt.Println("=== composed system prompt ===")
		fmt.Println(req.System)
		fmt.Println("==============================")
		return mock.CallTool("call_1", "get_weather", `{"city":"北京"}`)
	})

	// Compose the system prompt from sections instead of a single string.
	// Environment uses an injected clock so this example is deterministic.
	fixedClock := func() time.Time { return time.Date(2026, 6, 25, 9, 0, 0, 0, time.UTC) }
	p := prompt.New().
		Add(prompt.Identity("你是一个友好的天气助手。")).
		Add(prompt.Environment(prompt.WithNow(fixedClock))).
		Add(prompt.ToolGuidance()).
		Add(prompt.SessionState("user_pref"))

	assistant := agent.New(agent.Config{
		Name:        "assistant",
		Description: "天气助手",
		Model:       model,
		Prompt:      p, // wins over Instruction
		Tools:       []tool.Tool{weather},
	})

	// Seed a session-state key so the SessionState section has something to show.
	store := session.InMemory()
	s, err := store.GetOrCreate(context.Background(), "demo", "user-1", "session-1")
	if err != nil {
		log.Fatal(err)
	}
	s.State().Set("user_pref", "偏好摄氏度")

	r := runner.New(runner.Config{Root: assistant, Store: store, AppName: "demo"})

	ctx := context.Background()
	for ev, err := range r.Run(ctx, "user-1", "session-1", core.UserText("北京天气怎么样？")) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.Message != nil && ev.IsFinalResponse() {
			fmt.Println("assistant:", ev.Message.Text())
		}
	}
}

func partsText(parts []core.Part) string {
	var s string
	for _, p := range parts {
		if tx, ok := p.(core.Text); ok {
			s += tx.Text
		}
	}
	return s
}
