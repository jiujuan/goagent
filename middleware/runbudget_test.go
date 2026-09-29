package middleware_test

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// budgetModel answers with a paid tool call while tools are advertised, then
// (mode dependent) wraps up or hallucinates a call even without tools.
type budgetModel struct {
	reqs     []*llm.Request
	calls    int
	stubborn bool // keep calling tools even when the request has none
	stopAt   int  // after this many calls answer with text even if tools exist
}

func (m *budgetModel) Name() string { return "budgetmock" }

func (m *budgetModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.reqs = append(m.reqs, req)
		if (len(req.Tools) > 0 || m.stubborn) && (m.stopAt == 0 || m.calls < m.stopAt) {
			m.calls++
			resp := &llm.Response{
				Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
					core.ToolCall{ID: fmt.Sprintf("c%d", m.calls), Name: "fetch", Args: json.RawMessage(`{}`)},
				}},
				Usage: &core.Usage{InputTokens: 100, OutputTokens: 50},
			}
			yield(resp, nil)
			return
		}
		yield(&llm.Response{
			Message: core.AssistantText("final wrap-up"),
			Usage:   &core.Usage{InputTokens: 10, OutputTokens: 5},
		}, nil)
	}
}

func budgetAgent(t *testing.T, m llm.Model, guard agent.Middleware, opts ...agent.Option) *agent.Agent {
	t.Helper()
	fetch := tool.New("fetch", "fetch", func(_ *tool.Context, _ struct{}) (string, error) {
		return "ok", nil
	})
	opts = append([]agent.Option{agent.WithModel(m), agent.WithTools(fetch), agent.WithMiddleware(guard)}, opts...)
	a, err := agent.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// runOnce streams a run and reports the terminal shape.
func runOnce(t *testing.T, a *agent.Agent) (res core.Result, exceeded []string, done, failed, interrupted bool) {
	t.Helper()
	for ev := range collect(a.Stream(context.Background(), "go")) {
		switch e := ev.(type) {
		case core.BudgetExceeded:
			exceeded = append(exceeded, e.Resource)
		case core.RunDone:
			res, done = e.Result, true
		case core.RunFailed:
			failed = true
		case core.Interrupted:
			interrupted = true
		}
	}
	return
}

// countWarned counts [run-budget] warning messages in one request's history.
func countWarned(req *llm.Request) int {
	n := 0
	for _, m := range req.Messages {
		if m.Role == core.RoleUser && strings.Contains(m.Text(), middleware.WarnMarkerBudget) {
			n++
		}
	}
	return n
}

func TestRunBudgetWrapUpOnTokens(t *testing.T) {
	m := &budgetModel{}
	var reports []middleware.BudgetReport
	guard := middleware.RunBudget(middleware.RunBudgetOptions{
		MaxTotalTokens: 400,
		WarnRatio:      0.5, // warn at 200 cumulative tokens
		OnExceed:       func(r middleware.BudgetReport) { reports = append(reports, r) },
	})
	a := budgetAgent(t, m, guard)

	res, exceeded, done, failed, _ := runOnce(t, a)
	if failed || !done {
		t.Fatalf("done=%v failed=%v", done, failed)
	}
	if res.Message.Text() != "final wrap-up" {
		t.Fatalf("result = %q, want wrap-up text", res.Message.Text())
	}
	if len(exceeded) != 1 || exceeded[0] != "total_tokens" {
		t.Fatalf("exceeded = %v", exceeded)
	}
	if len(reports) != 1 || reports[0].InputTokens != 300 || reports[0].Turns != 3 {
		t.Fatalf("reports = %+v", reports)
	}
	// warned once only (at 300 ≥ 200 the third call crosses the hard cap instead)
	if n := countWarned(m.reqs[len(m.reqs)-1]); n != 1 {
		t.Fatalf("warning messages in final history = %d, want 1", n)
	}
	// the wrap-up request carried no tools
	if len(m.reqs[len(m.reqs)-1].Tools) != 0 {
		t.Fatal("last request should have had its tools cleared")
	}
	// and the wrap-up instruction was appended to the system prompt
	if !strings.Contains(m.reqs[len(m.reqs)-1].System, middleware.WarnMarkerBudget) {
		t.Fatal("last request system prompt should carry the wrap-up instruction")
	}
}

func TestRunBudgetHallucinatedCallStops(t *testing.T) {
	m := &budgetModel{stubborn: true}
	guard := middleware.RunBudget(middleware.RunBudgetOptions{MaxTotalTokens: 400})
	a := budgetAgent(t, m, guard)

	_, exceeded, done, failed, _ := runOnce(t, a)
	if failed || !done {
		t.Fatalf("the safeguard must end via Stop → RunDone: done=%v failed=%v", done, failed)
	}
	if len(exceeded) == 0 {
		t.Fatal("expected BudgetExceeded events")
	}
	// 150, 300, 450 → wrap after the third call; the hallucinated fourth call
	// is gated and never executed.
	if m.calls != 4 {
		t.Fatalf("model calls = %d, want 4 (last one gated)", m.calls)
	}
}

func TestRunBudgetLastTurnWrap(t *testing.T) {
	m := &budgetModel{}
	guard := middleware.RunBudget(middleware.RunBudgetOptions{MaxTotalTokens: 1_000_000})
	a := budgetAgent(t, m, guard, agent.WithMaxTurns(2))

	res, _, done, failed, _ := runOnce(t, a)
	if failed || !done || res.Message.Text() != "final wrap-up" {
		t.Fatalf("done=%v failed=%v res=%q — the final turn should wrap up, not error", done, failed, res.Message.Text())
	}
	if len(m.reqs) != 2 || len(m.reqs[1].Tools) != 0 {
		t.Fatalf("second (final) request should be tool-free, reqs=%d", len(m.reqs))
	}
}

func TestRunBudgetDurationCap(t *testing.T) {
	m := &budgetModel{}
	t0 := time.Now()
	ticks := 0
	guard := middleware.RunBudget(middleware.RunBudgetOptions{
		MaxDuration: 12 * time.Second,
		Now:         func() time.Time { ticks++; return t0.Add(time.Duration(ticks) * 6 * time.Second) },
	})
	a := budgetAgent(t, m, guard)

	res, exceeded, done, failed, _ := runOnce(t, a)
	if failed || !done || res.Message.Text() != "final wrap-up" {
		t.Fatalf("done=%v failed=%v res=%q", done, failed, res.Message.Text())
	}
	if len(exceeded) != 1 || exceeded[0] != "duration" {
		t.Fatalf("exceeded = %v, want [duration]", exceeded)
	}
}

func TestRunBudgetCostCap(t *testing.T) {
	m := &budgetModel{}
	guard := middleware.RunBudget(middleware.RunBudgetOptions{
		MaxCostUSD: 0.0005,
		Price:      middleware.Price{InputPerMTok: 1, OutputPerMTok: 2}, // 0.0002 per call
	})
	a := budgetAgent(t, m, guard)

	_, exceeded, done, _, _ := runOnce(t, a)
	if !done {
		t.Fatal("expected RunDone")
	}
	if len(exceeded) != 1 || exceeded[0] != "cost" {
		t.Fatalf("exceeded = %v, want [cost]", exceeded)
	}
}

func TestRunBudgetTurnsCap(t *testing.T) {
	m := &budgetModel{}
	guard := middleware.RunBudget(middleware.RunBudgetOptions{MaxTurns: 2})
	a := budgetAgent(t, m, guard)

	res, exceeded, done, _, _ := runOnce(t, a)
	if !done || res.Message.Text() != "final wrap-up" {
		t.Fatalf("done=%v res=%q", done, res.Message.Text())
	}
	if len(exceeded) != 1 || exceeded[0] != "turns" {
		t.Fatalf("exceeded = %v, want [turns]", exceeded)
	}
}

func TestRunBudgetObserveOnly(t *testing.T) {
	m := &budgetModel{stopAt: 2} // the agent would finish on its own after 2 calls
	guard := middleware.RunBudget(middleware.RunBudgetOptions{})
	a := budgetAgent(t, m, guard)

	res, exceeded, done, _, _ := runOnce(t, a)
	if !done || res.Message.Text() != "final wrap-up" {
		t.Fatalf("expected natural RunDone, done=%v res=%q", done, res.Message.Text())
	}
	if len(exceeded) != 0 {
		t.Fatalf("observe-only must not fire events: %v", exceeded)
	}
	if countWarned(m.reqs[len(m.reqs)-1]) != 0 {
		t.Fatal("observe-only must not steer warnings")
	}
	for _, r := range m.reqs {
		if len(r.Tools) == 0 {
			t.Fatal("observe-only must never clear the tool table")
		}
	}
}

// TestRunBudgetLedgerDurable proves the record survives a JSONL checkpoint
// round-trip: a fresh checkpointer over the same directory reads back the same
// cumulative numbers with float64/string shapes.
func TestRunBudgetLedgerDurable(t *testing.T) {
	dir := t.TempDir()
	m := &budgetModel{stubborn: true}
	guard := middleware.RunBudget(middleware.RunBudgetOptions{MaxTotalTokens: 400})
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := budgetAgent(t, m, guard, agent.WithCheckpointer(store))
	run := a.Stream(context.Background(), "go")
	for range collect(run) {
	}

	store2, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := store2.Latest(context.Background(), run.ThreadID)
	if err != nil || cp == nil {
		t.Fatalf("no checkpoint to resume (%v)", err)
	}
	b, ok := cp.State.KV["_budget"].(map[string]any)
	if !ok {
		t.Fatalf("persisted KV = %v, want a _budget record", cp.State.KV)
	}
	if b["wrapup"] != true {
		t.Fatalf("persisted ledger = %v, want wrapup=true", b)
	}
	if numOfTest(b["in"]) < 300 {
		t.Fatalf("persisted in = %v, want >= 300", b["in"])
	}
	if _, ok := b["warned"].(map[string]any); !ok {
		t.Fatalf("persisted warned = %T, want map", b["warned"])
	}
}

func numOfTest(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return -1
}
