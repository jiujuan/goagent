package agent_test

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// foldCompacter records each rewrite and drops nothing, so the fold order can
// be observed.
type foldCompacter struct {
	agent.BaseMiddleware
	tag  string
	seen *[]string
}

func (f foldCompacter) CompactHistory(_ *agent.LoopContext, h []core.Message) []core.Message {
	*f.seen = append(*f.seen, f.tag+":"+string(rune('0'+len(h))))
	return h
}

// trimCompacter keeps only the last keep messages, copying them into a fresh
// slice so the returned history never aliases dropped backing memory.
type trimCompacter struct {
	agent.BaseMiddleware
	keep int
}

func (t trimCompacter) CompactHistory(_ *agent.LoopContext, h []core.Message) []core.Message {
	if len(h) <= t.keep {
		return h
	}
	out := make([]core.Message, t.keep)
	copy(out, h[len(h)-t.keep:])
	return out
}

func TestStackCompactHistoryOrderAndNoop(t *testing.T) {
	var seen []string
	s := agent.NewStack(
		foldCompacter{tag: "a", seen: &seen},
		agent.BaseMiddleware{}, // no CompactHistory: skipped
		foldCompacter{tag: "b", seen: &seen},
	)
	h := []core.Message{core.UserText("x")}
	got := s.CompactHistory(&agent.LoopContext{}, h)
	if len(got) != 1 {
		t.Fatalf("history changed size: %v", got)
	}
	// Only the two implementers run, in registration order.
	want := []string{"a:1", "b:1"}
	if len(seen) != 2 || seen[0] != want[0] || seen[1] != want[1] {
		t.Fatalf("fold order = %v, want %v", seen, want)
	}
	// A stack with no compacter is a pure no-op.
	if out := agent.NewStack(agent.BaseMiddleware{}).CompactHistory(&agent.LoopContext{}, h); len(out) != 1 {
		t.Fatal("noop stack altered history")
	}
}

// TestLoopHistoryCompacterIsDurable proves a CompactHistory rewrite reaches the
// next request AND the checkpoint (State.Messages shrinks), the whole point of
// the capability over the request-only ModifyRequest path.
func TestLoopHistoryCompacterIsDurable(t *testing.T) {
	echo := tool.New("echo", "echo", func(_ *tool.Context, _ struct{}) (string, error) {
		return "e", nil
	})
	turn := 0
	var maxSeen int
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if len(req.Messages) > maxSeen {
			maxSeen = len(req.Messages)
		}
		turn++
		if turn <= 6 {
			return mock.CallTool("c", "echo", "{}")
		}
		return mock.Text("done")
	})
	store := checkpoint.NewMemory()
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(echo),
		agent.WithMiddleware(trimCompacter{keep: 3}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	// Without the compacter 6 tool turns would grow history past 12; the
	// per-step trim keeps every request small.
	if maxSeen > 5 {
		t.Fatalf("request history never trimmed, maxSeen=%d", maxSeen)
	}
}
