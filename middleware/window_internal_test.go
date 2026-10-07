package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// winAsst is an assistant message carrying one tool call per id.
func winAsst(ids ...string) core.Message {
	parts := make([]core.Part, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, core.ToolCall{ID: id, Name: "t_" + id, Args: json.RawMessage(`{}`)})
	}
	return core.Message{Role: core.RoleAssistant, Parts: parts}
}

// winTool is one tool message carrying one result per id, the shape the loop
// appends (all results of a step in a single message, agent/loop.go:256).
func winTool(ids ...string) core.Message {
	parts := make([]core.Part, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, core.ToolResult{CallID: id, Name: "t_" + id, Content: []core.Part{core.Text{Text: "r"}}})
	}
	return core.Message{Role: core.RoleTool, Parts: parts}
}

func winUser(s string) core.Message { return core.UserText(s) }

// winRoles renders a history as a role string for readable failure output.
func winRoles(msgs []core.Message) string {
	out := ""
	for _, m := range msgs {
		out += string(m.Role[0])
	}
	return out
}

func TestGroupUnits(t *testing.T) {
	cases := []struct {
		name string
		msgs []core.Message
		want [][2]int
	}{
		{
			name: "no calls: every message is its own unit",
			msgs: []core.Message{winUser("a"), winAsst(), winUser("b")},
			want: [][2]int{{0, 1}, {1, 2}, {2, 3}},
		},
		{
			name: "call binds its result",
			msgs: []core.Message{winUser("q"), winAsst("c1"), winTool("c1")},
			want: [][2]int{{0, 1}, {1, 3}},
		},
		{
			name: "two calls, results split across two tool messages: one unit",
			msgs: []core.Message{winUser("q"), winAsst("c1", "c2"), winTool("c1"), winTool("c2"), winAsst()},
			want: [][2]int{{0, 1}, {1, 4}, {4, 5}},
		},
		{
			name: "dangling call with no results is a unit of one",
			msgs: []core.Message{winUser("q"), winAsst("c1")},
			want: [][2]int{{0, 1}, {1, 2}},
		},
		{
			name: "leading orphan result stands alone",
			msgs: []core.Message{winTool("gone"), winUser("q"), winAsst("c1"), winTool("c1")},
			want: [][2]int{{0, 1}, {1, 2}, {2, 4}},
		},
		{
			name: "a user message between call and result breaks adjacency",
			msgs: []core.Message{winUser("q"), winAsst("c1"), winUser("steer"), winTool("c1")},
			want: [][2]int{{0, 1}, {1, 2}, {2, 3}, {3, 4}},
		},
		{
			name: "absorb on at least one match, stop at a message reporting none",
			msgs: []core.Message{winAsst("c1"), winTool("c1", "c9"), winTool("c9"), winUser("q")},
			want: [][2]int{{0, 2}, {2, 3}, {3, 4}},
		},
		{
			name: "empty history has no units",
			msgs: nil,
			want: [][2]int{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := groupUnits(tc.msgs)
			if len(got) != len(tc.want) {
				t.Fatalf("units = %d %v over [%s], want %d %v", len(got), gotUnits(got), winRoles(tc.msgs), len(tc.want), tc.want)
			}
			for i, u := range got {
				if u.start != tc.want[i][0] || u.end != tc.want[i][1] {
					t.Fatalf("unit %d = [%d,%d), want %v over [%s]", i, u.start, u.end, tc.want[i], winRoles(tc.msgs))
				}
			}
			// Units tile the input exactly: contiguous, non-overlapping, full cover.
			prev := 0
			for _, u := range got {
				if u.start != prev || u.end <= u.start {
					t.Fatalf("units not contiguous at [%d,%d) (prev %d)", u.start, u.end, prev)
				}
				prev = u.end
			}
			if prev != len(tc.msgs) {
				t.Fatalf("units cover %d of %d messages", prev, len(tc.msgs))
			}
		})
	}
}

func gotUnits(us []unit) [][2]int {
	out := make([][2]int, len(us))
	for i, u := range us {
		out[i] = [2]int{u.start, u.end}
	}
	return out
}

func TestRepairPairingDropsOrphanResults(t *testing.T) {
	// A result whose call was evicted, next to one that survives.
	in := []core.Message{
		winUser("q"),
		winTool("gone"),
		winAsst("c1"),
		winTool("c1", "gone2"),
	}
	out := repairPairing(in)
	if len(out) != 3 {
		t.Fatalf("kept %d messages [%s], want 3", len(out), winRoles(out))
	}
	if out[0].Role != core.RoleUser || out[1].Role != core.RoleAssistant {
		t.Fatalf("kept wrong messages: [%s]", winRoles(out))
	}
	tr, ok := out[2].Parts[0].(core.ToolResult)
	if !ok || len(out[2].Parts) != 1 || tr.CallID != "c1" {
		t.Fatalf("surviving tool message keeps %+v, want only c1", out[2].Parts)
	}
	// The input must not be mutated: history is shared with State.Messages.
	if len(in[1].Parts) != 1 || len(in[3].Parts) != 2 {
		t.Fatalf("repairPairing mutated its input: %d then %d parts", len(in[1].Parts), len(in[3].Parts))
	}
}

func TestRepairPairingKeepsPendingCallUntouched(t *testing.T) {
	// The shape a pending HITL interrupt checkpoints (agent/loop.go:236-244):
	// an assistant tool call with no result yet. Repair must leave it alone —
	// synthesizing a result would make Resume see an already-answered call.
	in := []core.Message{winUser("q"), winAsst("c1")}
	out := repairPairing(in)
	if len(out) != 2 {
		t.Fatalf("kept %d messages, want 2 unchanged", len(out))
	}
	if out[1].Role != core.RoleAssistant || len(out[1].Parts) != 1 {
		t.Fatalf("pending call was altered: %+v", out[1].Parts)
	}
}

func TestKeptMessagesNeverSplitsAUnit(t *testing.T) {
	// A history in which every result has its call: two call/result pairs, one
	// dangling-free assistant reply, one user turn in between.
	msgs := []core.Message{
		winUser("q"),
		winAsst("c1", "c2"), winTool("c1"), winTool("c2"),
		winUser("follow-up"),
		winAsst("c3"), winTool("c3"),
		winAsst(),
	}
	units := groupUnits(msgs)
	if len(units) != 5 {
		t.Fatalf("units = %d %v, want 5 ([0,1) [1,4) [4,5) [5,7) [7,8))", len(units), gotUnits(units))
	}

	// No selection of whole units can create an orphan, before any repair:
	// that is the whole point of deciding at unit granularity.
	subsets := [][]int{{}, {0}, {2}, {4}, {0, 2, 4}, {1, 3}, {1, 4}, {0, 1, 2, 3, 4}}
	for _, pick := range subsets {
		var keep []unit
		for _, idx := range pick {
			keep = append(keep, units[idx])
		}
		sel := keptMessages(msgs, keep)
		if orphans := winOrphanCallIDs(sel); len(orphans) > 0 {
			t.Fatalf("subset %v produced orphans %v: [%s]", pick, orphans, winRoles(sel))
		}
		// So repair is a no-op on a history that is already well-formed.
		if got := repairPairing(sel); len(got) != len(sel) {
			t.Fatalf("subset %v: repair dropped %d of %d messages from a well-formed selection", pick, len(sel)-len(got), len(sel))
		}
		// The result is a copy, never a window over the input's backing array:
		// a later append to sel would otherwise overwrite live history.
		if len(sel) > 0 {
			if first := units[pick[0]].start; &sel[0] == &msgs[first] {
				t.Fatalf("subset %v aliases the input at message %d", pick, first)
			}
		}
	}

	// A history that already carries an orphan (its call was never in the input)
	// is where repair earns its keep: the orphan is its own unit, and selecting
	// it alone must leave nothing behind.
	dirty := append([]core.Message{}, msgs...)
	dirty = append(dirty, winTool("gone"))
	dirtyUnits := groupUnits(dirty)
	if len(dirtyUnits) != 6 {
		t.Fatalf("dirty units = %d %v, want 6 (the orphan stands alone)", len(dirtyUnits), gotUnits(dirtyUnits))
	}
	orphanUnit := dirtyUnits[5]
	sel := keptMessages(dirty, []unit{dirtyUnits[0], orphanUnit})
	if orphans := winOrphanCallIDs(sel); len(orphans) != 1 {
		t.Fatalf("orphan selection should carry 1 orphan before repair, got %v", orphans)
	}
	if got := repairPairing(sel); len(got) != 1 || got[0].Role != core.RoleUser {
		t.Fatalf("repair kept [%s], want only the user message", winRoles(got))
	}
}

// winOrphanCallIDs lists the tool results in msgs whose call is absent.
func winOrphanCallIDs(msgs []core.Message) []string {
	calls := map[string]bool{}
	for _, m := range msgs {
		for _, c := range m.ToolCalls() {
			calls[c.ID] = true
		}
	}
	var orphans []string
	for _, m := range msgs {
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok && !calls[tr.CallID] {
				orphans = append(orphans, tr.CallID)
			}
		}
	}
	return orphans
}

// --- the middleware body ------------------------------------------------------

// newCounter gives each strategy test a token measure it can state exactly: n
// messages weigh n×per, so a budget reads as a message count.
func newCounter(per int) TokenCounter {
	return func(msgs []core.Message) int { return len(msgs) * per }
}

func TestWindowApplyCalibratesCount(t *testing.T) {
	st := &core.State{}
	lc := newLC(st, nil)
	w := Window(WindowOptions{Strategy: SlidingWindow(1000), Counter: newCounter(10)}).(*window)

	msgs := []core.Message{winUser("a"), winUser("b"), winUser("c")}
	// No calibration record yet → the factor is 1, so 3 messages × 10 = 30.
	if _, est, trimmed := w.apply(lc, msgs); est != 30 || trimmed {
		t.Fatalf("uncalibrated apply: est=%d trimmed=%v, want 30/false", est, trimmed)
	}

	saveCalibAt(lc, kvWindow, 2.0)
	if _, est, _ := w.apply(lc, msgs); est != 60 {
		t.Fatalf("calibrated at 2.0: est=%d, want 60", est)
	}
	// A different key must not be read: Compaction's factor stays out of it.
	saveCalibAt(lc, kvCompaction, 0.5)
	if _, est, _ := w.apply(lc, msgs); est != 60 {
		t.Fatalf("_compaction leaked into the window factor: est=%d, want 60", est)
	}
}

// keepAll is a strategy that evicts nothing but can be asked to.
type keepAll struct{}

func (keepAll) Name() string { return "keep_all" }
func (keepAll) Select(_ *agent.LoopContext, _ TokenCounter, msgs []core.Message) []core.Message {
	return msgs
}

func TestWindowApplySkipsRepairWhenNothingEvicted(t *testing.T) {
	// A stored history carrying a pre-existing orphan result.
	msgs := []core.Message{winUser("q"), winTool("gone")}
	w := Window(WindowOptions{Strategy: keepAll{}, Counter: newCounter(1)}).(*window)

	out, _, trimmed := w.apply(&agent.LoopContext{RunContext: &agent.RunContext{State: &core.State{}}}, msgs)
	if trimmed {
		t.Fatal("a strategy that evicts nothing must not report a trim")
	}
	if &out[0] != &msgs[0] {
		t.Fatal("the no-eviction path must hand back the input slice, not a repaired copy")
	}
	// The same strategy asked to drop one unit does run repair on the result.
	dropOne := func(lc *agent.LoopContext, count TokenCounter, m []core.Message) []core.Message {
		units := groupUnits(m)
		return keptMessages(m, units[1:])
	}
	w2 := Window(WindowOptions{Strategy: funcStrategy(dropOne), Counter: newCounter(1)}).(*window)
	out2, _, trimmed2 := w2.apply(&agent.LoopContext{RunContext: &agent.RunContext{State: &core.State{}}}, msgs)
	if !trimmed2 || len(out2) != 0 {
		t.Fatalf("after eviction the orphan should be repaired away, got trimmed=%v len=%d", trimmed2, len(out2))
	}
}

// funcStrategy adapts a plain function to WindowStrategy (test-only).
type funcStrategy func(lc *agent.LoopContext, count TokenCounter, msgs []core.Message) []core.Message

func (f funcStrategy) Name() string { return "func" }
func (f funcStrategy) Select(lc *agent.LoopContext, count TokenCounter, msgs []core.Message) []core.Message {
	return f(lc, count, msgs)
}

// --- the importance-weighted strategy ----------------------------------------

// stubScorer is a MessageScorer whose per-message scores are supplied by hand,
// so a test can separate the scoring policy from the selection policy.
type stubScorer struct {
	scores []float64
	calls  int
}

func (s *stubScorer) ScoreContent(_ context.Context, _ string, msgs []core.Message) ([]float64, error) {
	s.calls++
	if len(s.scores) != len(msgs) {
		return nil, nil
	}
	return s.scores, nil
}

// textMsg is a plain message of exactly n characters of the given filler, which
// under the built-in estimator weighs n/4 + 4 tokens. The filler makes units
// identifiable after keptMessages copies them: pointer identity cannot be used,
// and equal-length fillers keep every unit the same size so that only the term
// under test decides.
func textMsg(role core.Role, n int) core.Message {
	return textFill(role, 'x', n)
}

func textFill(role core.Role, fill byte, n int) core.Message {
	return core.Message{Role: role, Parts: []core.Part{core.Text{Text: strings.Repeat(string(fill), n)}}}
}

// fills lists the filler of each message, i.e. which units survived.
func fills(msgs []core.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		t := m.Text()
		if t == "" {
			b.WriteByte('-')
			continue
		}
		b.WriteByte(t[0])
	}
	return b.String()
}

func TestImportanceGreedyUsesValuePerToken(t *testing.T) {
	// One large unit with the highest content score (92 tokens), and six small
	// ones (9 tokens each). The budget must be one the large unit can take first
	// and then have little left for: at 120, ranking by score keeps the large unit
	// plus two small ones, while ranking by score per token keeps all six small
	// ones and drops the large one. At 100 the two rankings agree, which is why
	// the first version of this test proved nothing.
	msgs := []core.Message{
		textFill(core.RoleAssistant, 'b', 350), // 92 tokens, score 1.0
		textMsg(core.RoleAssistant, 20),        // 9 tokens each, score 0.2
		textMsg(core.RoleAssistant, 20),
		textMsg(core.RoleAssistant, 20),
		textMsg(core.RoleAssistant, 20),
		textMsg(core.RoleAssistant, 20),
		textMsg(core.RoleAssistant, 20),
	}
	scores := []float64{1.0, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2}
	st := &stubScorer{scores: scores}
	s := ImportanceWeighted(ImportanceOptions{BudgetTokens: 120, Scorer: st, NoPinFirst: true})

	out := s.Select(newLC(&core.State{}, nil), estimateTokens, msgs)
	if got := fills(out); strings.Contains(got, "b") {
		t.Fatalf("kept [%s]: the oversized unit was taken first, so ranking was by score, not score per token", got)
	}
	if len(out) < 6 {
		t.Fatalf("kept %d messages [%s], want the six small units", len(out), fills(out))
	}
}

func TestImportancePinFirstUser(t *testing.T) {
	// Four units of identical size, identical content score; a budget for two.
	// Without pinning, recency alone decides, so the oldest (the user turn) loses.
	msgs := []core.Message{
		textFill(core.RoleUser, 'q', 20),
		textFill(core.RoleAssistant, 'a', 20),
		textFill(core.RoleAssistant, 'b', 20),
		textFill(core.RoleAssistant, 'c', 20),
	}
	scores := []float64{0.2, 0.2, 0.2, 0.2}

	pinned := ImportanceWeighted(ImportanceOptions{BudgetTokens: 18, Scorer: &stubScorer{scores: scores}})
	if got := fills(pinned.Select(newLC(&core.State{}, nil), estimateTokens, msgs)); got != "qc" {
		t.Fatalf("with pinning kept [%s], want [qc] (the task definition plus the newest unit)", got)
	}

	unpinned := ImportanceWeighted(ImportanceOptions{BudgetTokens: 18, Scorer: &stubScorer{scores: scores}, NoPinFirst: true})
	if got := fills(unpinned.Select(newLC(&core.State{}, nil), estimateTokens, msgs)); got != "bc" {
		t.Fatalf("without pinning kept [%s], want [bc] (the two most recent units)", got)
	}
}

func TestImportanceSkipsUnfitUnitsInsteadOfStopping(t *testing.T) {
	// A unit that no longer fits is skipped, not a stopping point: otherwise one
	// large valuable unit evaluated early would block every smaller one behind it.
	// Units: A = a call plus its results (2 messages, content 0.8 each), B = one
	// message (content 0.4), plus the newest unit which is always kept.
	msgs := []core.Message{
		winAsst("c1", "c2", "c3"),
		winTool("c1", "c2", "c3"),
		textFill(core.RoleAssistant, 'b', 20),
		textFill(core.RoleAssistant, 'd', 20),
	}
	scores := []float64{0.8, 0.8, 0.4, 0.4}
	s := ImportanceWeighted(ImportanceOptions{BudgetTokens: 2, Scorer: &stubScorer{scores: scores}, NoPinFirst: true})
	out := s.Select(newLC(&core.State{}, nil), newCounter(1), msgs)
	if got := fills(out); got != "bd" {
		t.Fatalf("kept [%s], want [bd]: an unfit but valuable unit should be skipped, not stop the pass", got)
	}
}

func TestImportanceUnitScoreIsSummed(t *testing.T) {
	// A unit is a call and its result, and it is worth the sum of its parts: an
	// average would make a two-message unit score like a one-message one, letting
	// the budget prefer a lone turn over a decision plus its evidence. TauUnits is
	// set huge so the recency term adds the same to every unit, leaving
	// aggregation as the only thing that decides.
	msgs := []core.Message{
		winAsst("c1"),                         // content 0.9
		winTool("c1"),                         // content 0.9 → unit A, two messages
		textFill(core.RoleAssistant, 'b', 20), // content 0.6 → unit B, one message
		textFill(core.RoleAssistant, 'd', 20), // newest unit, always kept
	}
	scores := []float64{0.9, 0.9, 0.6, 0.6}
	s := ImportanceWeighted(ImportanceOptions{BudgetTokens: 3, TauUnits: 10000, Scorer: &stubScorer{scores: scores}, NoPinFirst: true})
	out := s.Select(newLC(&core.State{}, nil), newCounter(1), msgs)
	if len(out) != 3 {
		t.Fatalf("kept %d messages [%s], want 3 (unit A plus the newest unit)", len(out), fills(out))
	}
	if _, ok := out[1].Parts[0].(core.ToolResult); !ok {
		t.Fatalf("unit A's result is missing, so the unit was averaged rather than summed: [%s]", fills(out))
	}
}

func TestImportanceRecencyBreaksTies(t *testing.T) {
	// Equal content scores and equal sizes; a budget for two units. Without the
	// recency term the tie would be broken by original order, keeping the OLDEST
	// of the candidates instead of the one nearest the end.
	msgs := []core.Message{
		textFill(core.RoleAssistant, 'a', 20),
		textFill(core.RoleAssistant, 'b', 20),
		textFill(core.RoleAssistant, 'c', 20),
		textFill(core.RoleAssistant, 'd', 20),
	}
	scores := []float64{0.5, 0.5, 0.5, 0.5}
	s := ImportanceWeighted(ImportanceOptions{BudgetTokens: 18, Scorer: &stubScorer{scores: scores}, NoPinFirst: true})
	if got := fills(s.Select(newLC(&core.State{}, nil), estimateTokens, msgs)); got != "cd" {
		t.Fatalf("kept [%s], want [cd]: recency did not break the tie", got)
	}
}

func TestImportanceScorerErrorLeavesHistoryUnchanged(t *testing.T) {
	msgs := []core.Message{core.UserText("q"), textMsg(core.RoleAssistant, 60), textMsg(core.RoleAssistant, 60)}
	// The stub returns a usable-looking result AND an error: a stub that returned
	// nil with the error would let an "ignore the error" defect hide behind the
	// length check, and the test would pass for the wrong reason.
	bad := errScorer{scores: []float64{0.9, 0.9, 0.9}}
	s := ImportanceWeighted(ImportanceOptions{BudgetTokens: 1, Scorer: bad})

	// Direct call: untouched.
	if out := s.Select(newLC(&core.State{}, nil), estimateTokens, msgs); &out[0] != &msgs[0] {
		t.Fatal("a scorer error must return the input history")
	}
	// Through the middleware: no rewrite and no event.
	b := bus.New()
	ch, cancel := b.Subscribe("importance-error", bus.Lossy)
	defer cancel()
	lc := &agent.LoopContext{RunContext: &agent.RunContext{
		Context: context.Background(), State: &core.State{}, Bus: b, Topic: "importance-error",
	}}
	req := &llm.Request{Messages: msgs}
	if err := Window(WindowOptions{Strategy: s}).ModifyRequest(lc, req); err != nil {
		t.Fatal(err)
	}
	if len(req.Messages) != len(msgs) {
		t.Fatalf("a scorer error must not evict, kept %d of %d", len(req.Messages), len(msgs))
	}
	cancel()
	if n := len(winDrainEvents(ch)); n != 0 {
		t.Fatalf("a failed scoring step must publish nothing, saw %d", n)
	}
}

// errScorer hands back a full-length result together with an error.
type errScorer struct{ scores []float64 }

func (e errScorer) ScoreContent(context.Context, string, []core.Message) ([]float64, error) {
	return e.scores, errors.New("scorer down")
}

// winDrainEvents reads a closed event channel dry.
func winDrainEvents(ch <-chan core.Event) []core.Event {
	var out []core.Event
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func TestHeuristicScorerReadsToolResultsAndCJK(t *testing.T) {
	// A tool result mentioning the question's terms must not score below an
	// assistant message that does not: Message.Text() alone cannot see inside a
	// ToolResult, so windowText has to.
	tool := core.Message{Role: core.RoleTool, Parts: []core.Part{
		core.ToolResult{CallID: "c1", Name: "t", Content: []core.Part{core.Text{Text: "the budget table rows"}}},
	}}
	plain := textMsg(core.RoleAssistant, 20)
	scores, err := heuristicScorer{}.ScoreContent(context.Background(), "budget table rows", []core.Message{tool, plain})
	if err != nil {
		t.Fatal(err)
	}
	if scores[0] <= scores[1] {
		t.Fatalf("a tool result matching the query scored %v, an unmatched assistant %v", scores[0], scores[1])
	}

	// Chinese is not split on whitespace: single wide characters become tokens, so
	// a CJK question must still match a CJK message.
	cjkTool := core.Message{Role: core.RoleTool, Parts: []core.Part{
		core.ToolResult{CallID: "c1", Name: "t", Content: []core.Part{core.Text{Text: "会话预算已经超了"}}},
	}}
	cjkPlain := core.Message{Role: core.RoleAssistant, Parts: []core.Part{core.Text{Text: "一切正常"}}}
	got, err := heuristicScorer{}.ScoreContent(context.Background(), "预算超了", []core.Message{cjkTool, cjkPlain})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] <= got[1] {
		t.Fatalf("CJK matching failed: matched tool %v, unmatched assistant %v", got[0], got[1])
	}
	// Empty ref: role prior only, and no division by zero.
	empty, err := heuristicScorer{}.ScoreContent(context.Background(), "", []core.Message{core.UserText("x"), cjkTool})
	if err != nil || empty[0] <= empty[1] {
		t.Fatalf("with no query the role prior should decide, got %v err=%v", empty, err)
	}
}

func TestImportanceDefaults(t *testing.T) {
	s := ImportanceWeighted(ImportanceOptions{}).(*importanceWeighted)
	if s.budget != defaultWindowBudget || s.tau != float64(defaultImportanceTau) || !s.pinFirst {
		t.Fatalf("defaults = %+v, want budget %d tau %d pinned", s, defaultWindowBudget, defaultImportanceTau)
	}
	if _, ok := s.scorer.(heuristicScorer); !ok {
		t.Fatalf("scorer = %T, want the built-in heuristic", s.scorer)
	}
	if s.Name() != "importance_weighted" {
		t.Fatalf("Name = %q", s.Name())
	}
	// An empty history and a scorer returning the wrong length are both no-ops.
	count := newCounter(1)
	if out := s.Select(newLC(&core.State{}, nil), count, nil); out != nil {
		t.Fatalf("empty history should come back empty, got %d", len(out))
	}
	wrongLen := ImportanceWeighted(ImportanceOptions{Scorer: &stubScorer{scores: []float64{1}}})
	msgs := []core.Message{core.UserText("a"), core.UserText("b")}
	if out := wrongLen.Select(newLC(&core.State{}, nil), count, msgs); &out[0] != &msgs[0] {
		t.Fatal("a scorer result of the wrong length must leave the history alone")
	}
}

func TestWindowStrategyDefaults(t *testing.T) {
	if r := RecentN(0).(*recentN); r.keep != 1 {
		t.Fatalf("RecentN(0).keep = %d, want 1", r.keep)
	}
	if s := SlidingWindow(0).(*slidingWindow); s.budget != defaultWindowBudget {
		t.Fatalf("SlidingWindow(0).budget = %d, want %d", s.budget, defaultWindowBudget)
	}
	if got := []string{RecentN(2).Name(), SlidingWindow(10).Name()}; got[0] != "recent_n" || got[1] != "sliding_window" {
		t.Fatalf("strategy names = %v", got)
	}
	// A nil strategy is a no-op middleware, not a panic (the RAG posture).
	mw := Window(WindowOptions{})
	lc := newLC(&core.State{}, &llm.Request{Messages: []core.Message{winUser("a")}})
	if err := mw.ModifyRequest(lc, lc.Request); err != nil {
		t.Fatal(err)
	}
	if len(lc.Request.Messages) != 1 {
		t.Fatalf("nil strategy must leave the request alone, got %d", len(lc.Request.Messages))
	}
	if got := mw.(agent.HistoryCompacter).CompactHistory(lc, []core.Message{winUser("a")}); len(got) != 1 {
		t.Fatalf("nil strategy must leave the history alone, got %d", len(got))
	}
	if _, err := mw.AfterModel(lc, &llm.Response{Usage: &core.Usage{InputTokens: 10}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadCalibAt(lc, kvWindow); ok {
		t.Fatal("a middleware without a strategy must not write a calibration")
	}
}
