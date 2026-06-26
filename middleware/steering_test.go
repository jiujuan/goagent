package middleware_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
)

// TestSteeringInjectsMidRun verifies a steering message queued during a
// multi-step run is injected before a later model call and influences the
// outcome. The tool callback enqueues a steering message the first time it
// runs, simulating an external nudge arriving mid-task.
func TestSteeringInjectsMidRun(t *testing.T) {
	steer := middleware.NewSteering()

	noop := tool.New("noop", "do nothing",
		func(_ *tool.Context, _ struct{}) (string, error) {
			steer.SteerText("STEER: 改用简洁风格")
			return "done", nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		// If the steering message has arrived in context, finish accordingly.
		for _, m := range req.Messages {
			if strings.Contains(m.Text(), "STEER") {
				return mock.Text("已按指示调整：简洁。")
			}
		}
		// First turn: call the tool (which will enqueue a steering message).
		if _, ok := mock.LastToolResult(req); !ok {
			return mock.CallTool("c1", "noop", `{}`)
		}
		// Tool ran but steering not yet visible — should not happen because the
		// middleware drains before this call.
		return mock.Text("未收到指示。")
	})

	ag := agent.New(agent.Config{
		Name:       "a",
		Model:      model,
		Tools:      []tool.Tool{noop},
		Middleware: []middleware.Middleware{steer.Middleware()},
	})
	r := runner.New(runner.Config{Root: ag})

	var final string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("开始")) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.IsFinalResponse() {
			final = ev.Message.Text()
		}
	}

	if !strings.Contains(final, "简洁") {
		t.Fatalf("steering message was not applied; final = %q", final)
	}
	if steer.Pending() != 0 {
		t.Fatalf("steering queue should be drained, %d pending", steer.Pending())
	}
}
