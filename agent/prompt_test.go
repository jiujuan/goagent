package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
)

func TestWithPromptBuildsSystem(t *testing.T) {
	weather := tool.New("get_weather", "查询天气", func(_ *tool.Context, _ struct{}) (string, error) {
		return "", nil
	})
	var sys string
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		sys = req.System
		return mock.Text("ok")
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("IGNORED"), // WithPrompt wins
		agent.WithTools(weather),
		agent.WithPrompt(prompt.New().
			Add(prompt.Identity("你是天气助手。")).
			Add(prompt.ToolGuidance()).
			Add(prompt.SessionState("user_pref"))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "hi"); err != nil {
		t.Fatal(err)
	}

	if strings.Contains(sys, "IGNORED") {
		t.Fatal("WithPrompt should take precedence over WithInstruction")
	}
	if !strings.Contains(sys, "你是天气助手") {
		t.Fatalf("identity missing from prompt: %q", sys)
	}
	if !strings.Contains(sys, "get_weather") {
		t.Fatalf("ToolGuidance did not list the tool: %q", sys)
	}
}
