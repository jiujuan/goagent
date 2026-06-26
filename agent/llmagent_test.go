package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
)

type echoArgs struct {
	Text string `json:"text"`
}

// TestToolLoop verifies the full closed loop: the model requests a tool, the
// engine runs it, and the model produces a final answer from the tool result.
func TestToolLoop(t *testing.T) {
	var toolCalled bool
	echo := tool.New("echo", "echo the input back",
		func(_ *tool.Context, in echoArgs) (string, error) {
			toolCalled = true
			return "echo:" + in.Text, nil
		})

	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("done with " + partsText(tr.Content))
		}
		return mock.CallTool("c1", "echo", `{"text":"hi"}`)
	})

	a := agent.New(agent.Config{Name: "a", Model: model, Tools: []tool.Tool{echo}})
	r := runner.New(runner.Config{Root: a})

	var roles []core.Role
	var finalText string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("say hi")) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ev.Message != nil {
			roles = append(roles, ev.Message.Role)
			if ev.IsFinalResponse() {
				finalText = ev.Message.Text()
			}
		}
	}

	if !toolCalled {
		t.Fatal("tool was never called")
	}
	if finalText != "done with echo:hi" {
		t.Fatalf("unexpected final text: %q", finalText)
	}
	// Expect: user, assistant(tool_call), tool, assistant(final)
	want := []core.Role{core.RoleUser, core.RoleAssistant, core.RoleTool, core.RoleAssistant}
	if len(roles) != len(want) {
		t.Fatalf("role sequence = %v, want %v", roles, want)
	}
	for i := range want {
		if roles[i] != want[i] {
			t.Fatalf("role[%d] = %v, want %v (full: %v)", i, roles[i], want[i], roles)
		}
	}
}

// TestSchemaReflection checks that a tool's parameter schema is derived from
// its argument type.
func TestSchemaReflection(t *testing.T) {
	echo := tool.New("echo", "d", func(_ *tool.Context, in echoArgs) (string, error) { return "", nil })
	got := string(echo.Schema())
	want := `{"properties":{"text":{"type":"string"}},"required":["text"],"type":"object"}`
	if got != want {
		t.Fatalf("schema = %s, want %s", got, want)
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
