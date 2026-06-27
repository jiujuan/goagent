// Command quickstart is the offline smoke test: the v2 closed loop on the mock
// provider (no API key, no network), so `go run ./examples/quickstart` always
// works. For a copy-paste starter against a real model, see the sibling
// quickstart-chat / quickstart-tool / quickstart-stream examples.
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

	// A mock model: on the first turn it calls the tool (echoing the city from
	// the user's message), then answers from the tool result.
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("Done — " + tr.Content[0].(core.Text).Text)
		}
		city := lastUserText(req)
		return mock.CallTool("c1", "get_weather", fmt.Sprintf(`{"city":%q}`, city))
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("You are a friendly weather assistant."),
		agent.WithTools(weather),
		agent.WithMaxTurns(8), // safety cap on the model<->tool loop
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// A) One line to the answer.
	answer, err := a.Run(ctx, "Beijing")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("answer:", answer)

	// B) Or stream the same run's events.
	fmt.Println("--- streamed ---")
	for ev, err := range a.Stream(ctx, "Shanghai").Iter() {
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

// lastUserText returns the text of the most recent user message.
func lastUserText(req *llm.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == core.RoleUser {
			return req.Messages[i].Text()
		}
	}
	return ""
}
