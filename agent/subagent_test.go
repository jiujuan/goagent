package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func TestSubAgentIsolationReturnsOnlyFinalText(t *testing.T) {
	// The child uses a PRIVATE tool the parent does not know about, then answers.
	lookup := tool.New("lookup", "private lookup",
		func(_ *tool.Context, _ struct{}) (string, error) { return "secret-42", nil })
	child, err := agent.New(
		agent.WithModel(mock.New("child", func(req *llm.Request) *llm.Response {
			if tr, ok := mock.LastToolResult(req); ok {
				return mock.Text("child found: " + tr.Content[0].(core.Text).Text)
			}
			return mock.CallTool("c1", "lookup", "{}")
		})),
		agent.WithTools(lookup),
	)
	if err != nil {
		t.Fatal(err)
	}

	// The parent only knows the "research" tool (the wrapped child). If the
	// child's private "lookup" call leaked into the parent's context, the parent
	// loop would try to run an unknown tool — it does not, proving isolation.
	parent, err := agent.New(
		agent.WithModel(mock.New("parent", func(req *llm.Request) *llm.Response {
			if tr, ok := mock.LastToolResult(req); ok {
				return mock.Text("parent saw: " + tr.Content[0].(core.Text).Text)
			}
			return mock.CallTool("p1", "research", `{"task":"find the number"}`)
		})),
		agent.WithTools(agent.AsTool(child, "research", "delegate a research task to a sub-agent")),
	)
	if err != nil {
		t.Fatal(err)
	}

	out, err := parent.Run(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	// Parent received exactly the child's FINAL text, nothing of its internals.
	if out != "parent saw: child found: secret-42" {
		t.Fatalf("isolation broken or wrong result: %q", out)
	}
}
