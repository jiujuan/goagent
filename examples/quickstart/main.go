// Command quickstart runs a minimal end-to-end goagent loop using the mock
// provider (no API key required): the model asks to call a weather tool, the
// turn engine runs it, and the model produces a final answer from the result.
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
	"github.com/jiujuan/goagent/tool"
)

type weatherArgs struct {
	City string `json:"city" desc:"城市名"`
}

func main() {
	// 1. A typed tool. Its JSON Schema is derived from weatherArgs by reflection.
	weather := tool.New("get_weather", "查询某城市的当前天气",
		func(_ *tool.Context, in weatherArgs) (string, error) {
			return in.City + "：晴，25°C", nil
		})

	// 2. A scripted mock model: first call requests the tool, second call (now
	//    that a tool result is in history) writes the final answer.
	model := mock.New("mock-opus", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("查到了，" + partsText(tr.Content) + "。需要我帮你看看其他城市吗？")
		}
		return mock.CallTool("call_1", "get_weather", `{"city":"北京"}`)
	})

	// 3. An LLM agent wiring model + instruction + tools.
	assistant := agent.New(agent.Config{
		Name:        "assistant",
		Description: "天气助手",
		Model:       model,
		Instruction: "你是一个友好的天气助手。",
		Tools:       []tool.Tool{weather},
	})

	// 4. A runner ties the agent to a session store and streams events.
	r := runner.New(runner.Config{Root: assistant})

	ctx := context.Background()
	for ev, err := range r.Run(ctx, "user-1", "session-1", core.UserText("北京天气怎么样？")) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}
}

func printEvent(ev *core.Event) {
	if ev == nil || ev.Message == nil {
		return
	}
	switch ev.Message.Role {
	case core.RoleUser:
		fmt.Printf("👤 user:      %s\n", ev.Message.Text())
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			for _, c := range calls {
				fmt.Printf("🤖 assistant: →调用工具 %s(%s)\n", c.Name, string(c.Args))
			}
			return
		}
		fmt.Printf("🤖 assistant: %s\n", ev.Message.Text())
	case core.RoleTool:
		for _, p := range ev.Message.Parts {
			if tr, ok := p.(core.ToolResult); ok {
				fmt.Printf("🔧 tool:      %s -> %s\n", tr.Name, partsText(tr.Content))
			}
		}
	}
}

func partsText(parts []core.Part) string {
	var s string
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			s += t.Text
		}
	}
	return s
}
