// Command quickstart is the v2 minimal closed loop: Spec → Runtime.Compile →
// Agent.Start → Run.Iter. It uses the mock provider (no API key, no network) so
// it runs deterministically.
//
//	go run ./examples/quickstart
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runtime"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	// 1. A typed tool: the JSON schema is derived from the parameter struct.
	weather := tool.New("get_weather", "Look up the weather in a city",
		func(_ *tool.Context, in struct {
			City string `json:"city" desc:"city name"`
		}) (string, error) {
			return in.City + ": sunny, 25°C", nil
		})

	// 2. A mock model that calls the tool once, then answers from its result.
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("Done — " + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "get_weather", `{"city":"Beijing"}`)
	})

	// 3. Declarative spec.
	spec := agent.Spec{
		Name:        "assistant",
		Model:       model,
		Instruction: "You are a friendly weather assistant.",
		Tools:       []tool.Tool{weather},
	}

	// 4. Compile on the runtime and run, streaming events.
	rt := runtime.New(runtime.Config{})
	ag := rt.Compile(spec)
	run, err := ag.Start(context.Background(), runtime.RunRequest{
		ThreadID: "session-1",
		Message:  core.UserText("What's the weather in Beijing?"),
	})
	if err != nil {
		log.Fatal(err)
	}

	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("🤖 assistant:", t)
			}
		case core.ToolDone:
			fmt.Printf("🔧 tool:      %s -> %s\n", e.Result.Name, e.Result.Content[0].(core.Text).Text)
		}
	}
}
