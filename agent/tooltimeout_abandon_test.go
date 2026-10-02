package agent_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// stuckTool ignores its context entirely: it reports only when its own sleep is
// over, which is exactly what the loop's abandoned wait exists to survive.
type stuckTool struct {
	name  string
	delay time.Duration
	// late, when set, is what the handler returns after the loop has already
	// answered the call.
	late *tool.Result
}

func (s stuckTool) Name() string        { return s.name }
func (s stuckTool) Description() string { return "sleeps regardless of its context" }
func (s stuckTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{},"required":[]}`)
}

func (s stuckTool) Call(_ *tool.Context, _ json.RawMessage) (*tool.Result, error) {
	time.Sleep(s.delay)
	if s.late != nil {
		return s.late, nil
	}
	return tool.TextResult("late:" + s.name), nil
}

// answerAfterTool is the usual two-step script: call the tool, then repeat back
// what its result said.
func answerAfterTool(name string) *mock.Model {
	return mock.New(name, func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + toolResultText(tr))
		}
		return mock.CallTool("c1", "stuck", `{}`)
	})
}

// TestTimeoutUnblocksBatchThatIgnoresContext: a tool that never looks at its
// context used to hold the batch, and with it the run, forever. The loop now
// answers for it and carries on.
func TestTimeoutUnblocksBatchThatIgnoresContext(t *testing.T) {
	stuck := stuckTool{name: "stuck", delay: 20 * time.Second}
	a, err := agent.New(
		agent.WithModel(answerAfterTool("m")),
		agent.WithTools(stuck),
		agent.WithToolTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	// The outer cap is a tripwire: without the abandoned wait this call would
	// block for the full 20s handler sleep instead of failing here.
	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	answer, err := a.Run(runCtx, "go")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("run took %s; the abandoned call still held the loop", elapsed)
	}
	if !strings.Contains(answer, `tool "stuck" timed out after`) {
		t.Fatalf("answer = %q, want the timeout wording", answer)
	}
}

// TestAbandonedToolCannotMutateStateAfterTimeout: the late handler asks to write
// State.KV and to stop the run. Both belong to a call the run already answered,
// so neither may take effect.
func TestAbandonedToolCannotMutateStateAfterTimeout(t *testing.T) {
	stuck := stuckTool{
		name:  "stuck",
		delay: 300 * time.Millisecond,
		late: &tool.Result{
			Content: []core.Part{core.Text{Text: "too late"}},
			Control: &core.Directive{Kind: core.Stop, Reason: "must be discarded"},
			State:   []core.StateOp{{Kind: core.OpSetKV, Key: "late_key", Value: "late_value"}},
		},
	}
	spy := &stateSpy{}
	a, err := agent.New(
		agent.WithModel(answerAfterTool("m")),
		agent.WithTools(stuck),
		agent.WithToolTimeout(50*time.Millisecond),
		agent.WithMiddleware(spy),
	)
	if err != nil {
		t.Fatal(err)
	}

	answer, err := a.Run(context.Background(), "go")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// The handler's Stop directive would have ended the run before the second
	// model call, so reaching an answer at all proves it was dropped.
	if !strings.Contains(answer, "timed out") {
		t.Fatalf("answer = %q, want the timeout wording", answer)
	}

	// Wait past the handler's own return, then check the run state never changed.
	time.Sleep(500 * time.Millisecond)
	if v := spy.kvValue("late_key"); v != nil {
		t.Fatalf("abandoned handler wrote State.KV late_key = %v, want nothing written", v)
	}
}

// TestRunCancelWordsCancellationNotTimeout: cancelling the run is not the tool
// running out of time, and the model is told which of the two happened.
func TestRunCancelWordsCancellationNotTimeout(t *testing.T) {
	stuck := stuckTool{name: "stuck", delay: 20 * time.Second}
	a, err := agent.New(
		agent.WithModel(answerAfterTool("m")),
		agent.WithTools(stuck),
		// Generous bound: only the run-level cancellation can end the wait.
		agent.WithToolTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	var seen []string
	run := a.Stream(context.Background(), "go")
	for ev, err := range run.Iter() {
		if err != nil {
			break // the cancellation surfaces as a failed run, which is expected
		}
		switch e := ev.(type) {
		case core.ToolStarted:
			run.Cancel()
		case core.ToolDone:
			seen = append(seen, toolResultText(e.Result))
		}
	}
	if _, err := run.Wait(); err != nil && !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("run error = %v, want none or the cancellation itself", err)
	}
	// mock.Model never reads its context, so a cancelled run may still reach its
	// final answer here; with a real provider the next model call fails. Either
	// way the call the loop was waiting on must be worded as a cancellation.
	if len(seen) != 1 || !strings.Contains(seen[0], "the run was cancelled") {
		t.Fatalf("tool results = %q, want one cancellation wording", seen)
	}
	if strings.Contains(seen[0], "timed out") {
		t.Fatalf("cancellation was worded as a timeout: %q", seen[0])
	}
}

// TestSequentialBatchSlowToolDoesNotBlockRest: in a serial batch every call gets
// its own bound, so one abandoned call does not cost the rest of the batch.
func TestSequentialBatchSlowToolDoesNotBlockRest(t *testing.T) {
	slow := stuckTool{name: "slow", delay: 20 * time.Second}
	quick := tool.New("quick", "returns at once",
		func(_ *tool.Context, _ struct{}) (string, error) { return "ready", nil })

	// Scripted on call count, not on the tool result: the batch's result message
	// carries two parts, and this test cares about execution, not about which
	// part a responder happens to read back.
	calls := 0
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		calls++
		if calls > 1 {
			return mock.Text("done")
		}
		// One batch, two calls, in this order.
		return &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.ToolCall{ID: "c1", Name: "slow", Args: []byte(`{}`)},
				core.ToolCall{ID: "c2", Name: "quick", Args: []byte(`{}`)},
			}},
			StopReason: llm.StopToolUse,
		}
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(slow, quick),
		agent.WithToolExecution(agent.ToolSequential),
		agent.WithToolTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	var order, texts []string
	run := a.Stream(context.Background(), "go")
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := ev.(core.ToolDone); ok {
			order = append(order, e.Result.CallID)
			texts = append(texts, toolResultText(e.Result))
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "c1" || order[1] != "c2" {
		t.Fatalf("ToolDone order = %v, want c1 then c2", order)
	}
	if !strings.Contains(texts[0], "timed out") {
		t.Fatalf("first result = %q, want the timeout wording", texts[0])
	}
	if texts[1] != "ready" {
		t.Fatalf("second result = %q, want the quick tool's own answer", texts[1])
	}
}

// TestResumedHitlBatchHonoursTimeout: the batch an approval leaves behind runs
// through the same machinery, so it is bounded too.
func TestResumedHitlBatchHonoursTimeout(t *testing.T) {
	ctx := context.Background()
	stuck := stuckTool{name: "danger", delay: 20 * time.Second}
	gate := &gateOnce{}

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + toolResultText(tr))
		}
		return mock.CallTool("c1", "danger", `{}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(stuck),
		agent.WithMiddleware(gate),
		agent.WithCheckpointer(checkpoint.NewMemory()),
		agent.WithToolTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	run, pending := pauseOn(t, a, ctx, "go")
	run.Decide(agent.Allow(pending[0].CallID))
	cont, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for ev, err := range cont.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := ev.(core.ToolDone); ok {
			texts = append(texts, toolResultText(e.Result))
		}
	}
	if _, err := cont.Wait(); err != nil {
		t.Fatal(err)
	}
	// gateOnce rewrites whatever the tool produced, so the timeout text arrives
	// prefixed — proof the synthesized result went through the normal tail.
	if len(texts) != 1 || !strings.Contains(texts[0], "timed out") {
		t.Fatalf("resumed batch results = %q, want one timeout wording", texts)
	}
}

// TestTimeoutResultReachesOtelAfterTool: the abandoned call still gets exactly one
// AfterTool pass carrying the synthesized error result. That single pass is what
// ends the tool span and records the call as an error in obs/otel
// (obs/otel/otel.go AfterTool), so a second pass would double-count it and a
// missing one would leave the span open.
func TestTimeoutResultReachesOtelAfterTool(t *testing.T) {
	stuck := stuckTool{name: "stuck", delay: 20 * time.Second}
	rec := &resultSpy{}
	a, err := agent.New(
		agent.WithModel(answerAfterTool("m")),
		agent.WithTools(stuck),
		agent.WithToolTimeout(50*time.Millisecond),
		agent.WithMiddleware(rec),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}

	got := rec.seen()
	if len(got) != 1 {
		t.Fatalf("AfterTool saw %d results, want exactly 1", len(got))
	}
	if !got[0].IsError || !strings.Contains(toolResultText(got[0]), "timed out") {
		t.Fatalf("AfterTool result = %+v, want one timeout error result", got[0])
	}
}

// resultSpy records the results AfterTool is given.
type resultSpy struct {
	agent.BaseMiddleware

	mu      sync.Mutex
	results []core.ToolResult
}

func (r *resultSpy) AfterTool(_ *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *tr
	r.results = append(r.results, cp)
	return core.Directive{}, nil
}

func (r *resultSpy) seen() []core.ToolResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]core.ToolResult(nil), r.results...)
}

// stateSpy snapshots State.KV at the end of the run, which is where an abandoned
// handler's late State request would have landed.
type stateSpy struct {
	agent.BaseMiddleware

	mu sync.Mutex
	kv map[string]any
}

func (s *stateSpy) FinishRun(rc *agent.RunContext, _ core.Result, _ error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv = map[string]any{}
	for k, v := range rc.State.KV {
		s.kv[k] = v
	}
}

func (s *stateSpy) kvValue(key string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv[key]
}
