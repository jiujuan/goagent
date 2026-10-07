package middleware_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// wndLC is a bare loop context for driving one hook directly.
func wndLC() *agent.LoopContext {
	return &agent.LoopContext{RunContext: &agent.RunContext{Context: context.Background(), State: &core.State{}}}
}

func wndAsst(ids ...string) core.Message {
	parts := make([]core.Part, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, core.ToolCall{ID: id, Name: "t", Args: []byte("{}")})
	}
	return core.Message{Role: core.RoleAssistant, Parts: parts}
}

func wndTool(ids ...string) core.Message {
	parts := make([]core.Part, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, core.ToolResult{CallID: id, Name: "t", Content: []core.Part{core.Text{Text: "r"}}})
	}
	return core.Message{Role: core.RoleTool, Parts: parts}
}

func wndUser(s string) core.Message { return core.UserText(s) }

// wndOrphans lists tool results whose call is not in the same slice — the shape
// a provider rejects (both wire converters copy CallID through unchecked).
func wndOrphans(msgs []core.Message) []string {
	calls := map[string]bool{}
	for _, m := range msgs {
		for _, c := range m.ToolCalls() {
			calls[c.ID] = true
		}
	}
	var out []string
	for _, m := range msgs {
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok && !calls[tr.CallID] {
				out = append(out, tr.CallID)
			}
		}
	}
	return out
}

// perMsgCounter weighs every message the same, so a budget reads as a count.
func perMsgCounter(per int) middleware.TokenCounter {
	return func(msgs []core.Message) int { return len(msgs) * per }
}

// TestWindowRecentNByUnitNotMessage: keep is a count of units. The newest unit is
// one assistant call plus its result, so keep=1 must hand back two messages, and
// the call may never be split from its result.
func TestWindowRecentNByUnitNotMessage(t *testing.T) {
	mw := middleware.Window(middleware.WindowOptions{Strategy: middleware.RecentN(1)})
	req := &llm.Request{Messages: []core.Message{
		wndUser("old question"),
		wndAsst("c1"), wndTool("c1"),
		wndUser("follow-up"),
		wndAsst("c2"), wndTool("c2"),
	}}
	if err := mw.ModifyRequest(wndLC(), req); err != nil {
		t.Fatal(err)
	}
	got := req.Messages
	if len(got) != 2 {
		t.Fatalf("kept %d messages, want 2 (the last unit is a call and its result): %+v", len(got), got)
	}
	if got[0].Role != core.RoleAssistant || got[1].Role != core.RoleTool {
		t.Fatalf("kept wrong messages: %v / %v", got[0].Role, got[1].Role)
	}
	if orphans := wndOrphans(got); len(orphans) > 0 {
		t.Fatalf("kept an orphan result %v", orphans)
	}
}

// TestWindowSlidingWindowKeepsTailWithinBudget: with every message worth 100,
// a budget of 250 fits the newest unit (2 messages) but not the next one.
func TestWindowSlidingWindowKeepsTailWithinBudget(t *testing.T) {
	mw := middleware.Window(middleware.WindowOptions{
		Strategy: middleware.SlidingWindow(250),
		Counter:  perMsgCounter(100),
	})
	req := &llm.Request{Messages: []core.Message{
		wndUser("q1"), wndUser("q2"), wndUser("q3"),
		wndAsst("c1"), wndTool("c1"),
	}}
	if err := mw.ModifyRequest(wndLC(), req); err != nil {
		t.Fatal(err)
	}
	if len(req.Messages) != 2 {
		t.Fatalf("kept %d messages, want only the newest unit (200 of a 250 budget)", len(req.Messages))
	}
	if req.Messages[0].Role != core.RoleAssistant {
		t.Fatalf("kept the wrong tail: %v", req.Messages[0].Role)
	}
	// The full history still fits when the budget covers it: nothing is evicted.
	mw2 := middleware.Window(middleware.WindowOptions{
		Strategy: middleware.SlidingWindow(1000),
		Counter:  perMsgCounter(100),
	})
	req2 := &llm.Request{Messages: []core.Message{wndUser("q1"), wndUser("q2"), wndUser("q3")}}
	if err := mw2.ModifyRequest(wndLC(), req2); err != nil {
		t.Fatal(err)
	}
	if len(req2.Messages) != 3 {
		t.Fatalf("under budget the request must be untouched, got %d messages", len(req2.Messages))
	}
}

// TestWindowSlidingWindowOversizedNewestUnit: a newest unit larger than the whole
// budget is kept verbatim rather than split — and when it is all there is,
// nothing was evicted, so no WindowTrimmed may be published.
func TestWindowSlidingWindowOversizedNewestUnit(t *testing.T) {
	// Each message weighs 10; the budget is 15, so any two-message unit is
	// already over it.
	mw := middleware.Window(middleware.WindowOptions{
		Strategy: middleware.SlidingWindow(15),
		Counter:  perMsgCounter(10),
	})
	big := []core.Message{wndAsst("c1", "c2", "c3"), wndTool("c1", "c2", "c3")}

	// Case 1: the oversized unit is the whole history → nothing to evict, no event.
	b := bus.New()
	ch, cancel := b.Subscribe("oversized", bus.Lossy)
	lc := &agent.LoopContext{RunContext: &agent.RunContext{
		Context: context.Background(), State: &core.State{}, Bus: b, Topic: "oversized",
	}}
	req := &llm.Request{Messages: big}
	if err := mw.ModifyRequest(lc, req); err != nil {
		t.Fatal(err)
	}
	cancel()
	if len(req.Messages) != 2 {
		t.Fatalf("the oversized newest unit must be kept whole, got %d messages", len(req.Messages))
	}
	if n := len(winDrain(ch)); n != 0 {
		t.Fatalf("a step that evicted nothing must publish nothing, saw %d events", n)
	}

	// Case 2: with older units in front, those are dropped and the event fires.
	req2 := &llm.Request{Messages: append([]core.Message{wndUser("q1"), wndUser("q2")}, big...)}
	ch2, cancel2 := b.Subscribe("oversized", bus.Lossy)
	if err := mw.ModifyRequest(lc, req2); err != nil {
		t.Fatal(err)
	}
	cancel2()
	if len(req2.Messages) != 2 {
		t.Fatalf("older units should have been evicted, kept %d", len(req2.Messages))
	}
	evs := winDrain(ch2)
	if len(evs) != 1 {
		t.Fatalf("events = %d, want 1", len(evs))
	}
	w, ok := evs[0].(core.WindowTrimmed)
	if !ok || w.Strategy != "sliding_window" || w.Dropped != 2 || w.Kept != 2 {
		t.Fatalf("event = %+v, want one sliding_window trim dropping 2 keeping 2", evs[0])
	}
	// EstTokens is the pre-eviction measure: 4 messages × 10, factor 1.
	if w.EstTokens != 40 {
		t.Fatalf("EstTokens = %d, want 40 (the history before eviction)", w.EstTokens)
	}
}

// drain reads a closed event channel dry.
func winDrain(ch <-chan core.Event) []core.Event {
	var out []core.Event
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

// TestWindowMutualExclusion: with Persist on, ModifyRequest must not touch the
// request; the rewrite happens only through CompactHistory (and the other way
// round in request mode).
func TestWindowMutualExclusion(t *testing.T) {
	persist := middleware.Window(middleware.WindowOptions{
		Strategy: middleware.RecentN(1),
		Persist:  true,
	})
	lc := wndLC()
	req := &llm.Request{Messages: []core.Message{wndUser("a"), wndUser("b"), wndUser("c")}}
	if err := persist.ModifyRequest(lc, req); err != nil {
		t.Fatal(err)
	}
	if len(req.Messages) != 3 {
		t.Fatalf("persist mode must leave the request untouched, got %d messages", len(req.Messages))
	}
	hc, ok := persist.(agent.HistoryCompacter)
	if !ok {
		t.Fatal("Window must implement HistoryCompacter")
	}
	if out := hc.CompactHistory(lc, []core.Message{wndUser("a"), wndUser("b"), wndUser("c")}); len(out) != 1 {
		t.Fatalf("persist CompactHistory should keep one unit, got %d", len(out))
	}

	request := middleware.Window(middleware.WindowOptions{Strategy: middleware.RecentN(1)})
	hc2 := request.(agent.HistoryCompacter)
	if out := hc2.CompactHistory(lc, []core.Message{wndUser("a"), wndUser("b"), wndUser("c")}); len(out) != 3 {
		t.Fatalf("request mode must leave the history untouched, got %d", len(out))
	}
}

// TestWindowResultPassesPairingCheck: whatever a step actually evicts, what it
// leaves behind has no orphan result — the shape a provider rejects.
//
// The last sub-case pins the deliberate exception: when a step evicts nothing,
// Window returns the input untouched and does not run the pairing repair, so a
// malformed history (here, a tool result whose call is gone) keeps being sent.
// Window is not a history cleaner; silently rewriting State.Messages from a step
// that decided to keep everything would be a different feature.
func TestWindowResultPassesPairingCheck(t *testing.T) {
	msgs := []core.Message{
		wndUser("q"),
		wndAsst("c1", "c2"), wndTool("c1"), wndTool("c2"),
		wndUser("next"),
		wndAsst("c3"), wndTool("c3"),
		wndTool("gone"), // an orphan already in the stored history
	}
	cases := []struct {
		name     string
		strategy middleware.WindowStrategy
		evicts   bool
	}{
		{"recent_n evicts", middleware.RecentN(2), true},
		// Five units weigh 1+3+1+2+1 messages; a budget of 4 fits only the last three.
		{"sliding_window evicts", middleware.SlidingWindow(4), true},
		{"sliding_window under budget", middleware.SlidingWindow(1000), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mw := middleware.Window(middleware.WindowOptions{Strategy: tc.strategy, Counter: perMsgCounter(1)})
			req := &llm.Request{Messages: msgs}
			if err := mw.ModifyRequest(wndLC(), req); err != nil {
				t.Fatal(err)
			}
			orphans := wndOrphans(req.Messages)
			if tc.evicts {
				if len(req.Messages) >= len(msgs) {
					t.Fatalf("expected eviction, kept %d of %d", len(req.Messages), len(msgs))
				}
				if len(orphans) > 0 {
					t.Fatalf("an evicting step left orphans %v", orphans)
				}
				return
			}
			if len(req.Messages) != len(msgs) {
				t.Fatalf("under budget nothing may be evicted, kept %d of %d", len(req.Messages), len(msgs))
			}
			if len(orphans) != 1 || orphans[0] != "gone" {
				t.Fatalf("the no-eviction path must hand the history back as stored, orphans=%v", orphans)
			}
		})
	}
}

// --- through the real loop -----------------------------------------------------

// wndAgent wires a model that calls `fetch` n times then answers, with a tool
// whose result is deliberately large.
func wndAgent(n int, mws ...agent.Middleware) (*agent.Agent, checkpoint.Checkpointer) {
	turn := 0
	conv := mock.New("m", func(*llm.Request) *llm.Response {
		turn++
		if turn <= n {
			return mock.CallTool("c"+string(rune('a'+turn)), "fetch", "{}")
		}
		return mock.Text("done")
	})
	fetch := tool.New("fetch", "fetch", func(_ *tool.Context, _ struct{}) (string, error) {
		return strings.Repeat("y", 400), nil
	})
	store := checkpoint.NewMemory()
	a, _ := agent.New(
		agent.WithModel(conv),
		agent.WithTools(fetch),
		agent.WithMiddleware(mws...),
		agent.WithCheckpointer(store),
	)
	return a, store
}

// TestWindowPersistShrinksState: the durable eviction must reach the checkpoint.
func TestWindowPersistShrinksState(t *testing.T) {
	a, store := wndAgent(4, middleware.Window(middleware.WindowOptions{
		Strategy: middleware.RecentN(2),
		Persist:  true,
	}))
	run := a.Stream(context.Background(), "go")
	trimmed := 0
	var last core.WindowTrimmed
	for ev := range collect(run) {
		if w, ok := ev.(core.WindowTrimmed); ok {
			trimmed++
			last = w
		}
	}
	if trimmed == 0 {
		t.Fatal("expected at least one WindowTrimmed event in persist mode")
	}
	if last.Strategy != "recent_n" || last.Kept <= 0 || last.Dropped <= 0 {
		t.Fatalf("event not shaped like an eviction: %+v", last)
	}
	cp, err := store.Latest(context.Background(), run.ThreadID)
	if err != nil || cp == nil {
		t.Fatalf("no checkpoint: %v", err)
	}
	// 4 tool turns would leave 9 messages (user + 4×(assistant+tool) + final);
	// a durable keep-2-units history stays at or below that.
	if len(cp.State.Messages) > 5 {
		t.Fatalf("State.Messages not trimmed: %d", len(cp.State.Messages))
	}
}

// TestWindowRequestModeLeavesStateFull contrasts with persist: the stored
// history keeps everything, even though the model only ever saw the window.
func TestWindowRequestModeLeavesStateFull(t *testing.T) {
	a, store := wndAgent(4, middleware.Window(middleware.WindowOptions{
		Strategy: middleware.RecentN(2), // request mode (default)
	}))
	run := a.Stream(context.Background(), "go")
	trimmed := 0
	for ev := range collect(run) {
		if _, ok := ev.(core.WindowTrimmed); ok {
			trimmed++
		}
	}
	if trimmed == 0 {
		t.Fatal("request mode should still report each trimmed step")
	}
	cp, err := store.Latest(context.Background(), run.ThreadID)
	if err != nil || cp == nil {
		t.Fatalf("no checkpoint: %v", err)
	}
	if len(cp.State.Messages) < 9 {
		t.Fatalf("request mode must keep the full history, got %d messages", len(cp.State.Messages))
	}
	// Every message stored is still well-formed: no orphan ever reaches State.
	if orphans := wndOrphans(cp.State.Messages); len(orphans) > 0 {
		t.Fatalf("stored history carries orphans %v", orphans)
	}
}

// TestWindowEventPublished checks the event fields against the shape observed on
// the wire: Dropped + Kept must add up to what went in, and EstTokens is the
// pre-eviction measure.
func TestWindowEventPublished(t *testing.T) {
	a, _ := wndAgent(4, middleware.Window(middleware.WindowOptions{
		Strategy: middleware.RecentN(1),
		Persist:  true,
		Counter:  perMsgCounter(10),
	}))
	run := a.Stream(context.Background(), "go")
	var events []core.WindowTrimmed
	for ev := range collect(run) {
		if w, ok := ev.(core.WindowTrimmed); ok {
			events = append(events, w)
		}
	}
	if len(events) == 0 {
		t.Fatal("no WindowTrimmed published")
	}
	for i, e := range events {
		if e.Strategy != "recent_n" {
			t.Fatalf("event %d: strategy = %q, want recent_n", i, e.Strategy)
		}
		if e.Dropped <= 0 || e.Kept <= 0 {
			t.Fatalf("event %d: dropped=%d kept=%d, both should be positive", i, e.Dropped, e.Kept)
		}
		if e.EstTokens < e.Kept*10 {
			t.Fatalf("event %d: EstTokens=%d below the kept set's own measure (kept %d × 10)", i, e.EstTokens, e.Kept)
		}
	}
}

// TestWindowCalibrationIsolatedFromCompaction: both middlewares learn a factor
// from the same response, and each must keep its own copy in State.KV. The
// calibration write replaces the whole value at its key, so sharing _compaction
// would leave one of the two factors at whatever the last writer put there.
//
// Both estimators are pinned to constants and the reported Usage to a fixed
// number, so the expected factors are exact rather than merely plausible:
// Compaction measures 100 for the request, Window measures 200, the provider
// says 150. If the two ever share a key, one of these two numbers moves.
func TestWindowCalibrationIsolatedFromCompaction(t *testing.T) {
	const (
		compactionEst = 100 // 150/100 → factor 1.5
		windowEst     = 200 // 150/200 → factor 0.75
		reported      = 150
	)
	conv := mock.New("m", func(*llm.Request) *llm.Response {
		return &llm.Response{
			Message: core.AssistantText("done"),
			Usage:   &core.Usage{InputTokens: reported},
		}
	})
	sum := mock.New("s", func(*llm.Request) *llm.Response { return mock.Text("SUM") })
	store := checkpoint.NewMemory()
	a, err := agent.New(
		agent.WithModel(conv),
		agent.WithMiddleware(
			middleware.Compaction(middleware.CompactionOptions{
				Model: sum, MaxTokens: 10, KeepRecent: 1, Persist: true,
				Counter: fixedCounter(compactionEst),
			}),
			middleware.Window(middleware.WindowOptions{
				Strategy: middleware.SlidingWindow(1000),
				Counter:  fixedCounter(windowEst),
			}),
		),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), strings.Repeat("x", 600))
	for range collect(run) {
	}
	cp, err := store.Latest(context.Background(), run.ThreadID)
	if err != nil || cp == nil {
		t.Fatalf("no checkpoint: %v", err)
	}
	wv, wok := cp.State.KV["_window"].(map[string]any)
	cv, cok := cp.State.KV["_compaction"].(map[string]any)
	if !wok || !cok {
		t.Fatalf("both calibration keys should be checkpointed, got _window=%v _compaction=%v", cp.State.KV["_window"], cp.State.KV["_compaction"])
	}
	if got, isFloat := wv["calib"].(float64); !isFloat || got < 0.7 || got > 0.8 {
		t.Fatalf("_window calib = %v, want 0.75", wv["calib"])
	}
	if got, isFloat := cv["calib"].(float64); !isFloat || got < 1.45 || got > 1.55 {
		t.Fatalf("_compaction calib = %v, want 1.5", cv["calib"])
	}
}

// fixedCounter always reports n regardless of the messages, which is what makes
// the calibration arithmetic above exact instead of estimator-dependent.
func fixedCounter(n int) middleware.TokenCounter {
	return func([]core.Message) int { return n }
}

// TestWindowOrderWithCompaction pins what the registration order actually
// changes. Stack.ModifyRequest runs middleware in registration order
// (agent/middleware.go:187-194), so with both installed one of them sees the
// full history and the other sees the first one's output.
//
// Final size does NOT distinguish the two orders: compaction only ever shrinks a
// history (one note replaces a prefix), so whatever the window's cap was, putting
// compaction after it cannot exceed it either. What does change is whether the
// summarizer runs at all, and whether the summary note survives the step — which
// is the cost that matters, because summarizing costs a model call.
//
//	Compaction → Window:  summarizer runs on the full history, then the window
//	                      evicts the note it just produced.
//	Window → Compaction:  the window shrinks first; the summarizer's threshold is
//	                      no longer met, so no call is spent.
func TestWindowOrderWithCompaction(t *testing.T) {
	const summaryPrefix = "[earlier conversation summary] "
	history := func() []core.Message {
		out := make([]core.Message, 10)
		for i := range out {
			out[i] = core.UserText(strings.Repeat("q", 40))
		}
		return out
	}
	newCompaction := func(calls *int) agent.Middleware {
		sum := mock.New("s", func(req *llm.Request) *llm.Response {
			*calls++
			if len(req.Messages) > 0 {
				// The summarizer is handed one rendered transcript message.
				_ = req.Messages[0].Text()
			}
			return mock.Text("SUMMARY")
		})
		return middleware.Compaction(middleware.CompactionOptions{
			Model: sum, MaxTokens: 10, KeepRecent: 6,
			Counter: fixedCounter(100), // always over the threshold once history is long
		})
	}
	window := func() agent.Middleware {
		return middleware.Window(middleware.WindowOptions{Strategy: middleware.RecentN(1)})
	}

	t.Run("compaction first spends the call and the window drops the note", func(t *testing.T) {
		calls := 0
		s := agent.NewStack(newCompaction(&calls), window())
		req := &llm.Request{Messages: history()}
		if err := s.ModifyRequest(wndLC(), req); err != nil {
			t.Fatal(err)
		}
		if calls == 0 {
			t.Fatal("compaction ran first and should have summarized")
		}
		if len(req.Messages) != 1 {
			t.Fatalf("the window last: kept %d messages, want 1", len(req.Messages))
		}
		if strings.HasPrefix(req.Messages[0].Text(), summaryPrefix) {
			t.Fatal("the note survived; the window did not evict it")
		}
	})

	t.Run("window first leaves the summarizer nothing to do", func(t *testing.T) {
		calls := 0
		s := agent.NewStack(window(), newCompaction(&calls))
		req := &llm.Request{Messages: history()}
		if err := s.ModifyRequest(wndLC(), req); err != nil {
			t.Fatal(err)
		}
		if calls != 0 {
			t.Fatalf("the window shrank the history first, yet the summarizer ran %d times", calls)
		}
		if len(req.Messages) != 1 {
			t.Fatalf("kept %d messages, want 1", len(req.Messages))
		}
	})
}

// TestWindowImportanceKeepsTheMidConversationDecision is the end-to-end case for
// ImportanceWeighted: a run whose tool outputs are bulky and whose decisive turn
// sits in the middle. A time-based policy would drop that turn as "old"; scoring
// keeps it while giving up the bulky, stale tool results.
//
// The decision turn carries a small tool call as well as its text: an assistant
// message without tool calls would end the run there, and the point is to keep
// deciding *after* it. The assertions read the request the model actually
// received (the middleware rewrites req.Messages before the call), not the
// stored history.
func TestWindowImportanceKeepsTheMidConversationDecision(t *testing.T) {
	const decision = "decision: ship plan B, budget first"
	turn := 0
	var lastSent []core.Message
	conv := mock.New("m", func(req *llm.Request) *llm.Response {
		lastSent = req.Messages
		turn++
		switch turn {
		case 1, 3, 4, 5: // each fetch returns 400 characters
			return mock.CallTool("c"+string(rune('0'+turn)), "fetch", "{}")
		case 2: // the decisive turn, plus one cheap call so the run continues
			return &llm.Response{Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.Text{Text: decision},
				core.ToolCall{ID: "c2", Name: "note", Args: []byte("{}")},
			}}}
		default:
			return mock.Text("done")
		}
	})
	fetch := tool.New("fetch", "fetch", func(_ *tool.Context, _ struct{}) (string, error) {
		return strings.Repeat("y", 400), nil
	})
	note := tool.New("note", "note", func(_ *tool.Context, _ struct{}) (string, error) {
		return "noted", nil
	})
	store := checkpoint.NewMemory()
	a, err := agent.New(
		agent.WithModel(conv),
		agent.WithTools(fetch, note),
		agent.WithMiddleware(middleware.Window(middleware.WindowOptions{
			// The built-in heuristic scorer: no injected scoring, no network.
			Strategy: middleware.ImportanceWeighted(middleware.ImportanceOptions{BudgetTokens: 200}),
		})),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run := a.Stream(context.Background(), "how do we cap the budget")
	for range collect(run) {
	}
	if turn < 6 || lastSent == nil {
		t.Fatalf("test premise broken: model calls = %d", turn)
	}

	var sent string
	sentBig := 0
	for _, m := range lastSent {
		sent += m.Text()
		for _, p := range m.Parts {
			tr, ok := p.(core.ToolResult)
			if !ok {
				continue
			}
			for _, c := range tr.Content {
				if t2, ok := c.(core.Text); ok && strings.Contains(t2.Text, strings.Repeat("y", 200)) {
					sentBig++
				}
			}
		}
	}
	if !strings.Contains(sent, decision) {
		t.Fatalf("the mid-run decision is missing from what the model saw (%d messages, %d big results sent)", len(lastSent), sentBig)
	}

	cp, err := store.Latest(context.Background(), run.ThreadID)
	if err != nil || cp == nil {
		t.Fatalf("no checkpoint: %v", err)
	}
	storedBig := 0
	for _, m := range cp.State.Messages {
		for _, p := range m.Parts {
			tr, ok := p.(core.ToolResult)
			if !ok {
				continue
			}
			for _, c := range tr.Content {
				if t2, ok := c.(core.Text); ok && strings.Contains(t2.Text, strings.Repeat("y", 200)) {
					storedBig++
				}
			}
		}
	}
	if storedBig != 4 {
		t.Fatalf("test premise broken: stored big tool results = %d, want 4", storedBig)
	}
	// The window did drop bulk: only the newest fetch unit can be in the request.
	if sentBig == 0 || sentBig >= storedBig {
		t.Fatalf("sent %d big results of %d stored: nothing was traded away", sentBig, storedBig)
	}
	// Request mode keeps everything in the store, so the trimmed turns are still
	// recoverable from the checkpoint even though the model did not see them.
	if len(cp.State.Messages) <= len(lastSent) {
		t.Fatalf("stored history should be larger than the trimmed request: %d vs %d", len(cp.State.Messages), len(lastSent))
	}
	if orphans := wndOrphans(lastSent); len(orphans) > 0 {
		t.Fatalf("the trimmed request carries orphan results %v", orphans)
	}
}
