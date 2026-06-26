package middleware_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
)

// captureModel records the request it last received, then returns a canned
// response. It lets tests observe how middleware rewrote the request.
func captureModel(got **llm.Request) llm.Model {
	return mock.New("capture", func(req *llm.Request) *llm.Response {
		*got = req
		return mock.Text("ok")
	})
}

func drain(t *testing.T, m llm.Model, req *llm.Request) {
	t.Helper()
	for _, err := range m.Generate(context.Background(), req) {
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
	}
}

func TestCompactionSummarizesOldHistory(t *testing.T) {
	summarizer := mock.New("sum", func(*llm.Request) *llm.Response {
		return mock.Text("STRUCTURED-SUMMARY")
	})
	var got *llm.Request
	model := middleware.Chain(captureModel(&got),
		middleware.Compaction(summarizer, &middleware.CompactionOptions{
			MaxTokens:        40,
			KeepRecentTokens: 16,
		}))

	var msgs []core.Message
	for i := range 20 {
		role := core.RoleUser
		if i%2 == 1 {
			role = core.RoleAssistant
		}
		msgs = append(msgs, core.Message{Role: role, Parts: []core.Part{
			core.Text{Text: "this is message number with some filler content"},
		}})
	}
	last := msgs[len(msgs)-1]

	drain(t, model, &llm.Request{Messages: msgs})

	if got == nil {
		t.Fatal("model never called")
	}
	if len(got.Messages) >= len(msgs) {
		t.Fatalf("expected compaction to shrink history: got %d, was %d", len(got.Messages), len(msgs))
	}
	if !strings.Contains(got.Messages[0].Text(), "STRUCTURED-SUMMARY") {
		t.Fatalf("first message should be the summary, got %q", got.Messages[0].Text())
	}
	tail := got.Messages[len(got.Messages)-1]
	if tail.Text() != last.Text() || tail.Role != last.Role {
		t.Fatalf("last message not preserved: %+v", tail)
	}
}

func TestCompactionNoOpWhenSmall(t *testing.T) {
	calls := 0
	summarizer := mock.New("sum", func(*llm.Request) *llm.Response {
		calls++
		return mock.Text("X")
	})
	var got *llm.Request
	model := middleware.Chain(captureModel(&got),
		middleware.Compaction(summarizer, &middleware.CompactionOptions{MaxTokens: 10000}))

	drain(t, model, &llm.Request{Messages: []core.Message{core.UserText("short")}})

	if len(got.Messages) != 1 {
		t.Fatalf("history should be untouched, got %d", len(got.Messages))
	}
	if calls != 0 {
		t.Fatalf("summarizer should not be called, was called %d times", calls)
	}
}

func TestCompactionKeepsToolPairs(t *testing.T) {
	summarizer := mock.New("sum", func(*llm.Request) *llm.Response { return mock.Text("S") })
	var got *llm.Request
	model := middleware.Chain(captureModel(&got),
		middleware.Compaction(summarizer, &middleware.CompactionOptions{MaxTokens: 30, KeepRecentTokens: 10}))

	var msgs []core.Message
	for range 10 {
		msgs = append(msgs, core.UserText("filler filler filler filler"))
	}
	msgs = append(msgs,
		core.Message{Role: core.RoleAssistant, Parts: []core.Part{core.ToolCall{ID: "1", Name: "t", Args: []byte(`{}`)}}},
		core.Message{Role: core.RoleTool, Parts: []core.Part{core.ToolResult{CallID: "1", Name: "t", Content: []core.Part{core.Text{Text: "result"}}}}},
	)

	drain(t, model, &llm.Request{Messages: msgs})

	for i, m := range got.Messages {
		if i == 0 {
			continue // summary
		}
		if m.Role == core.RoleTool {
			prev := got.Messages[i-1]
			if len(prev.ToolCalls()) == 0 {
				t.Fatalf("tool result at %d has no preceding tool call", i)
			}
		}
	}
}
