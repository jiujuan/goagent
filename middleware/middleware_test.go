package middleware_test

import (
	"context"
	"errors"
	"iter"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

func loopCtx(ctx context.Context) *agent.LoopContext {
	return &agent.LoopContext{RunContext: &agent.RunContext{Context: ctx}}
}

// dangerAgent calls a "danger" tool on the first turn, then answers.
func dangerAgent(t *testing.T, mws ...agent.Middleware) *agent.Agent {
	t.Helper()
	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		return "done", nil
	})
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("ran: " + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "danger", "{}")
	})
	opts := []agent.Option{agent.WithModel(model), agent.WithTools(danger)}
	for _, m := range mws {
		opts = append(opts, agent.WithMiddleware(m))
	}
	a, err := agent.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestPermissionAsksApproval(t *testing.T) {
	a := dangerAgent(t, middleware.Permission(middleware.RequireApprovalFor("danger")))
	var interrupted, ranTool bool
	for ev := range collect(a.Stream(context.Background(), "go")) {
		switch ev.(type) {
		case core.Interrupted:
			interrupted = true
		case core.ToolDone:
			ranTool = true
		}
	}
	if !interrupted || ranTool {
		t.Fatalf("ask: interrupted=%v ranTool=%v", interrupted, ranTool)
	}
}

func TestPermissionDenyStops(t *testing.T) {
	a := dangerAgent(t, middleware.Permission(middleware.DenyFor("danger")))
	var ranTool bool
	for ev := range collect(a.Stream(context.Background(), "go")) {
		if _, ok := ev.(core.ToolDone); ok {
			ranTool = true
		}
	}
	if ranTool {
		t.Fatal("deny should prevent the tool from running")
	}
}

// flaky errors the first `failures` calls, then succeeds.
type flaky struct {
	failures int
	calls    int
}

func (f *flaky) Name() string { return "flaky" }
func (f *flaky) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		f.calls++
		if f.calls <= f.failures {
			yield(nil, errors.New("transient boom"))
			return
		}
		yield(mock.Text("recovered"), nil)
	}
}

func TestRetryModelRecovers(t *testing.T) {
	f := &flaky{failures: 2}
	a, err := agent.New(agent.WithModel(
		middleware.RetryModel(f, middleware.RetryOptions{MaxAttempts: 3, BaseDelay: time.Millisecond}),
	))
	if err != nil {
		t.Fatal(err)
	}
	out, err := a.Run(context.Background(), "go")
	if err != nil {
		t.Fatalf("retry should have recovered: %v", err)
	}
	if out != "recovered" || f.calls != 3 {
		t.Fatalf("out=%q calls=%d, want recovered/3", out, f.calls)
	}
}

func TestRetryModelGivesUp(t *testing.T) {
	f := &flaky{failures: 5}
	a, _ := agent.New(agent.WithModel(
		middleware.RetryModel(f, middleware.RetryOptions{MaxAttempts: 2, BaseDelay: time.Millisecond}),
	))
	if _, err := a.Run(context.Background(), "go"); err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
}

func TestCompactionShrinksHistory(t *testing.T) {
	summarizer := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUMMARY") })
	mw := middleware.Compaction(middleware.CompactionOptions{Model: summarizer, MaxTokens: 1, KeepRecent: 2})

	msgs := make([]core.Message, 0, 10)
	for i := 0; i < 10; i++ {
		msgs = append(msgs, core.UserText(strings.Repeat("x", 40)))
	}
	req := &llm.Request{Messages: msgs}
	if err := mw.ModifyRequest(loopCtx(context.Background()), req); err != nil {
		t.Fatal(err)
	}
	if len(req.Messages) != 3 {
		t.Fatalf("compacted to %d messages, want 3 (summary + 2 recent)", len(req.Messages))
	}
	if !strings.Contains(req.Messages[0].Text(), "SUMMARY") {
		t.Fatalf("first message is not the summary: %q", req.Messages[0].Text())
	}
}

func TestRAGInjectsContext(t *testing.T) {
	ret := middleware.NewInMemory(
		"Go has goroutines for concurrency",
		"Python uses a GIL",
		"Rust enforces ownership",
	)
	mw := middleware.RAG(middleware.RAGOptions{Retriever: ret, K: 2})
	req := &llm.Request{System: "base", Messages: []core.Message{core.UserText("tell me about go goroutines")}}
	if err := mw.ModifyRequest(loopCtx(context.Background()), req); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(req.System, "goroutines") {
		t.Fatalf("RAG did not inject context: %q", req.System)
	}
}

func TestRateLimitCancellable(t *testing.T) {
	mw := middleware.RateLimit(middleware.RateLimitOptions{RPS: 0.001, Burst: 1})
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := mw.BeforeModel(loopCtx(ctx)); err != nil { // consumes the one burst token
		t.Fatal(err)
	}
	cancel()
	if _, err := mw.BeforeModel(loopCtx(ctx)); err == nil { // empty bucket + cancelled → error
		t.Fatal("expected cancellation error while throttled")
	}
}

// collect drains a run's events into a channel for ranging in tests.
func collect(run *agent.Run) <-chan core.Event {
	ch := make(chan core.Event)
	go func() {
		defer close(ch)
		for ev, err := range run.Iter() {
			if err != nil {
				return
			}
			ch <- ev
		}
	}()
	return ch
}
