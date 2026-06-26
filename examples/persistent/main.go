// Command persistent demonstrates JSONL session persistence: each run appends
// one user turn to a session stored on disk, and the recovered history grows
// across separate process invocations.
//
// Usage:
//
//	go run ./examples/persistent            # writes to ./goagent-sessions
//	go run ./examples/persistent again      # resumes the same session
//
// Run it several times and watch the message count climb — the conversation is
// reloaded from disk each time.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

func main() {
	prompt := "你好"
	if len(os.Args) > 1 {
		prompt = os.Args[1]
	}

	store, err := session.NewFileStore("./goagent-sessions")
	if err != nil {
		log.Fatal(err)
	}

	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		// Reply with the running turn count to make growth visible.
		turns := 0
		for _, m := range req.Messages {
			if m.Role == core.RoleUser {
				turns++
			}
		}
		return mock.Text(fmt.Sprintf("收到第 %d 条消息。", turns))
	})

	ag := agent.New(agent.Config{Name: "assistant", Model: model})
	r := runner.New(runner.Config{AppName: "demo", Root: ag, Store: store})

	ctx := context.Background()
	for ev, err := range r.Run(ctx, "user-1", "chat-1", core.UserText(prompt)) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.Message != nil && ev.IsFinalResponse() {
			fmt.Printf("🤖 %s\n", ev.Message.Text())
		}
	}

	// Show the full recovered history length.
	s, _ := store.GetOrCreate(ctx, "demo", "user-1", "chat-1")
	fmt.Printf("📜 会话现有 %d 条消息（落盘于 ./goagent-sessions/demo/user-1/chat-1.jsonl）\n", len(s.Messages()))
}
