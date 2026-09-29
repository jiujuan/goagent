package middleware_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

func bigMsgs(n int) []core.Message {
	out := make([]core.Message, n)
	for i := range out {
		out[i] = core.Message{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: strings.Repeat("x", 200)}}}
	}
	return out
}

// TestCompactionMutualExclusion: with Persist on, ModifyRequest must NOT touch
// the request; the rewrite happens only through CompactHistory.
func TestCompactionMutualExclusion(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUM") })
	mw := middleware.Compaction(middleware.CompactionOptions{Model: sum, MaxTokens: 10, KeepRecent: 2, Persist: true})
	lc := &agent.LoopContext{RunContext: &agent.RunContext{Context: context.Background(), State: &core.State{}}}

	req := &llm.Request{Messages: bigMsgs(10)}
	if err := mw.ModifyRequest(lc, req); err != nil {
		t.Fatal(err)
	}
	if len(req.Messages) != 10 {
		t.Fatalf("persist mode must leave the request untouched, got %d messages", len(req.Messages))
	}

	hc, ok := mw.(agent.HistoryCompacter)
	if !ok {
		t.Fatal("Compaction must implement HistoryCompacter")
	}
	out := hc.CompactHistory(lc, bigMsgs(10))
	if len(out) >= 10 || !strings.HasPrefix(out[0].Text(), "[earlier conversation summary]") {
		t.Fatalf("persist CompactHistory should prepend a summary and shrink, got %d msgs first=%q", len(out), out[0].Text())
	}
}

// TestCompactionPersistShrinksState runs a real agent: the durable rewrite must
// reach the checkpointed State.Messages, and a HistoryCompacted event must fire.
func TestCompactionPersistShrinksState(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUM") })
	turn := 0
	conv := mock.New("m", func(*llm.Request) *llm.Response {
		turn++
		if turn <= 4 {
			return mock.CallTool("c", "fetch", "{}")
		}
		return mock.Text("done")
	})
	fetch := tool.New("fetch", "fetch", func(_ *tool.Context, _ struct{}) (string, error) {
		return strings.Repeat("y", 400), nil
	})
	store := checkpoint.NewMemory()
	a, err := agent.New(
		agent.WithModel(conv),
		agent.WithTools(fetch),
		agent.WithMiddleware(middleware.Compaction(middleware.CompactionOptions{Model: sum, MaxTokens: 20, KeepRecent: 2, Persist: true})),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(context.Background(), "go")
	var compacted bool
	var dropped int
	for ev := range collect(run) {
		if e, ok := ev.(core.HistoryCompacted); ok {
			compacted = true
			dropped = e.Dropped
		}
	}
	if !compacted {
		t.Fatal("expected a HistoryCompacted event in persist mode")
	}
	if dropped <= 0 {
		t.Fatalf("event Dropped should be positive, got %d", dropped)
	}

	cp, err := store.Latest(context.Background(), run.ThreadID)
	if err != nil || cp == nil {
		t.Fatalf("no checkpoint: %v", err)
	}
	if !strings.HasPrefix(cp.State.Messages[0].Text(), "[earlier conversation summary]") {
		t.Fatalf("durable history should start with the summary note, got %q", cp.State.Messages[0].Text())
	}
	// 4 tool turns would leave ~9 messages untrimmed; persist keeps it tiny.
	if len(cp.State.Messages) > 6 {
		t.Fatalf("State.Messages not trimmed: %d", len(cp.State.Messages))
	}
}

// TestCompactionRequestModeLeavesStateFull contrasts with persist: request mode
// checkpoints the FULL history (unchanged behavior).
func TestCompactionRequestModeLeavesStateFull(t *testing.T) {
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUM") })
	turn := 0
	conv := mock.New("m", func(*llm.Request) *llm.Response {
		turn++
		if turn <= 4 {
			return mock.CallTool("c", "fetch", "{}")
		}
		return mock.Text("done")
	})
	fetch := tool.New("fetch", "fetch", func(_ *tool.Context, _ struct{}) (string, error) {
		return strings.Repeat("y", 400), nil
	})
	store := checkpoint.NewMemory()
	a, err := agent.New(
		agent.WithModel(conv),
		agent.WithTools(fetch),
		agent.WithMiddleware(middleware.Compaction(middleware.CompactionOptions{Model: sum, MaxTokens: 20, KeepRecent: 2})), // request mode
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "go")
	var compacted bool
	for ev := range collect(run) {
		if _, ok := ev.(core.HistoryCompacted); ok {
			compacted = true
		}
	}
	if compacted {
		t.Fatal("request mode must not emit HistoryCompacted (no durable rewrite)")
	}
	cp, _ := store.Latest(context.Background(), run.ThreadID)
	if strings.HasPrefix(cp.State.Messages[0].Text(), "[earlier conversation summary]") {
		t.Fatal("request mode must not rewrite the stored history")
	}
}
