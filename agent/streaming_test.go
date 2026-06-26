package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
)

// TestStreamingFlowsThroughEngine verifies that partial responses from a
// streaming model surface as partial events (not committed) and the final
// aggregated message is the one committed and used as the answer.
func TestStreamingFlowsThroughEngine(t *testing.T) {
	model := mock.NewStream("stream", func(*llm.Request) []*llm.Response {
		return []*llm.Response{
			mock.Partial("He"),
			mock.Partial("Hello"),
			mock.Partial("Hello wor"),
			mock.Text("Hello world"), // final, non-partial
		}
	})
	a := agent.New(agent.Config{Name: "a", Model: model})
	r := runner.New(runner.Config{Root: a})

	var partials, finals int
	var lastFinal string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("hi")) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.Message == nil || ev.Message.Role != core.RoleAssistant {
			continue
		}
		if ev.Partial {
			partials++
		} else {
			finals++
			lastFinal = ev.Message.Text()
		}
	}

	if partials != 3 {
		t.Fatalf("expected 3 partial assistant events, got %d", partials)
	}
	if finals != 1 {
		t.Fatalf("expected 1 final assistant event, got %d", finals)
	}
	if lastFinal != "Hello world" {
		t.Fatalf("final text = %q", lastFinal)
	}
}
