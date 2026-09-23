package agent_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// gateCountingMW records whether the BeforeTool gate was consulted.
type gateCountingMW struct {
	agent.BaseMiddleware
	beforeTool atomic.Int32
}

func (m *gateCountingMW) BeforeTool(*agent.LoopContext, *core.ToolCall) (core.Directive, error) {
	m.beforeTool.Add(1)
	return core.Directive{}, nil
}

func truncatedCall(id, name string, args string) *llm.Response {
	return &llm.Response{
		Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
			core.ToolCall{ID: id, Name: name, Args: json.RawMessage(args)},
		}},
		StopReason: llm.StopMaxTokens,
	}
}

func toolResultText(tr core.ToolResult) string {
	var sb strings.Builder
	for _, p := range tr.Content {
		if t, ok := p.(core.Text); ok {
			sb.WriteString(t.Text)
		}
	}
	return sb.String()
}

func TestTruncatedToolCallsAreNotExecuted(t *testing.T) {
	var invoked atomic.Int32
	w := tool.New("write_file", "write a file", func(*tool.Context, struct {
		Path string `json:"path"`
	}) (string, error) {
		invoked.Add(1)
		return "ok", nil
	})

	mw := &gateCountingMW{}
	m := mock.New("truncator", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			if !tr.IsError || !strings.Contains(toolResultText(tr), "output token limit") {
				t.Errorf("unexpected tool result: %+v", tr)
			}
			return mock.Text("recovered")
		}
		// Malformed (truncated) JSON args: they must never reach the handler
		// nor the BeforeTool gate.
		return truncatedCall("c1", "write_file", `{"path":`)
	})

	a, err := agent.New(agent.WithModel(m), agent.WithTools(w), agent.WithMiddleware(mw))
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "write something")
	var started, done []core.Event
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch ev.(type) {
		case core.ToolStarted:
			started = append(started, ev)
		case core.ToolDone:
			done = append(done, ev)
		}
	}

	res, err := run.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Text() != "recovered" {
		t.Fatalf("final message = %q, want %q", res.Message.Text(), "recovered")
	}
	if n := invoked.Load(); n != 0 {
		t.Fatalf("truncated tool was invoked %d times, want 0", n)
	}
	if n := mw.beforeTool.Load(); n != 0 {
		t.Fatalf("BeforeTool gate consulted %d times for truncated batch, want 0", n)
	}
	if len(started) != 1 || len(done) != 1 {
		t.Fatalf("events: got %d ToolStarted / %d ToolDone, want 1/1", len(started), len(done))
	}
	tr, ok := done[0].(core.ToolDone)
	if !ok || !tr.Result.IsError || tr.Result.CallID != "c1" {
		t.Fatalf("ToolDone = %+v, want error result for call c1", done[0])
	}
}

func TestTruncatedGuardOnlySkipsTokenLimitedReplies(t *testing.T) {
	var invoked atomic.Int32
	round := 0
	w := tool.New("echo", "echo back", func(*tool.Context, struct {
		S string `json:"s"`
	}) (string, error) {
		invoked.Add(1)
		return "hi", nil
	})

	m := mock.New("caller", func(req *llm.Request) *llm.Response {
		round++
		switch round {
		case 1:
			// Normal tool_use stop: the call must execute as before.
			return mock.CallTool("c1", "echo", `{"s":"x"}`)
		default:
			return mock.Text("done")
		}
	})

	a, err := agent.New(agent.WithModel(m), agent.WithTools(w))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if n := invoked.Load(); n != 1 {
		t.Fatalf("echo invoked %d times, want 1 (StopToolUse must not trigger the guard)", n)
	}
}
