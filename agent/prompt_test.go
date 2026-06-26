package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
)

// TestPromptOverridesInstruction asserts a configured Prompt builder wins over
// the static Instruction, and that it is rendered exactly once per invocation
// (not once per model step).
func TestPromptOverridesInstruction(t *testing.T) {
	renders := 0
	counter := prompt.SectionFunc{
		SecName: "counter", SecOrder: 100,
		RenderFn: func(prompt.Context) (string, error) {
			renders++
			return "BUILT", nil
		},
	}

	echo := tool.New("echo", "echo", func(_ *tool.Context, in echoArgs) (string, error) {
		return "echo:" + in.Text, nil
	})

	// Two-step loop (tool call -> final) so the engine makes two model calls.
	var systems []string
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		systems = append(systems, req.System)
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "echo", `{"text":"hi"}`)
	})

	a := agent.New(agent.Config{
		Name:        "a",
		Model:       model,
		Instruction: "IGNORED",
		Prompt:      prompt.New().Add(counter),
		Tools:       []tool.Tool{echo},
	})
	r := runner.New(runner.Config{Root: a})

	for _, err := range r.Run(context.Background(), "u", "s", core.UserText("hi")) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if renders != 1 {
		t.Fatalf("prompt rendered %d times, want 1", renders)
	}
	if len(systems) != 2 {
		t.Fatalf("expected 2 model calls, got %d", len(systems))
	}
	for i, s := range systems {
		if s != "BUILT" {
			t.Fatalf("system[%d] = %q, want %q (Prompt should win over Instruction)", i, s, "BUILT")
		}
	}
}

// TestInstructionFallback confirms the legacy Instruction path is unchanged
// when no Prompt builder is configured.
func TestInstructionFallback(t *testing.T) {
	var system string
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		system = req.System
		return mock.Text("ok")
	})

	a := agent.New(agent.Config{Name: "a", Model: model, Instruction: "legacy persona"})
	r := runner.New(runner.Config{Root: a})

	for _, err := range r.Run(context.Background(), "u", "s", core.UserText("hi")) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if system != "legacy persona" {
		t.Fatalf("system = %q, want legacy persona", system)
	}
}
