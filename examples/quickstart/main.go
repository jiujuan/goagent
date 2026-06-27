// Command quickstart is the v2 minimal closed loop with the merged agent API:
// agent.New(...options) → agent.Run(...). It uses the mock provider (no API key,
// no network) so it runs deterministically.
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
	"github.com/jiujuan/goagent/tool"
)

func main() {
	// A typed tool: the JSON schema is derived from the parameter struct.
	weather := tool.New("get_weather", "Look up the weather in a city",
		func(_ *tool.Context, in struct {
			City string `json:"city" desc:"city name"`
		}) (string, error) {
			return in.City + ": sunny, 25°C", nil
		})

	// A mock model that calls the tool once, then answers from its result.
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("Done — " + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "get_weather", `{"city":"Beijing"}`)
	})

	// Build the agent with functional options.
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("You are a friendly weather assistant."),
		agent.WithTools(weather),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// A) One line to the answer: New + Run drives the LLM<->tool loop.
	answer, err := a.Run(ctx, "What's the weather in Beijing?")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("answer:", answer)

	// B) Or stream the events of a run.
	fmt.Println("--- streamed ---")
	for ev, err := range a.Stream(ctx, "And in Shanghai?").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			fmt.Printf("🔧 %s -> %s\n", e.Result.Name, e.Result.Content[0].(core.Text).Text)
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("🤖", t)
			}
		}
	}
}
