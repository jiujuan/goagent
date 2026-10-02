package middleware_test

import (
	"context"
	"encoding/json"
	"strconv"
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

// --- the stage ---------------------------------------------------------------

// lookupTool advertises two required arguments, so a call missing either one is
// refused by schema validation before its handler is reached.
type lookupTool struct{ runs *int }

func (l lookupTool) Name() string        { return "lookup" }
func (l lookupTool) Description() string { return "look something up" }
func (l lookupTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"},"limit":{"type":"integer"}},"required":["q","limit"]}`)
}

func (l lookupTool) Call(_ *tool.Context, _ json.RawMessage) (*tool.Result, error) {
	*l.runs++
	return tool.TextResult("found"), nil
}

// flakyTool answers every call with a business failure it reports itself. That is
// a result, not a refusal, so ArgGuard must ignore it.
type flakyTool struct{ runs *int }

func (f flakyTool) Name() string        { return "flaky" }
func (f flakyTool) Description() string { return "upstream is unreliable" }
func (f flakyTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{},"required":[]}`)
}

func (f flakyTool) Call(_ *tool.Context, _ json.RawMessage) (*tool.Result, error) {
	*f.runs++
	return tool.ErrorResult("upstream 500"), nil
}

// argScript drives the model: each turn emits the next call from bad (cycling when
// loop is set) and only falls back to text once the script is spent. Scripted on
// the model-call count, not on the last tool result — a refused call still leaves
// a result in history, so that branch would end the run after one refusal.
type argScript struct {
	bad  []string
	loop bool
	turn int
}

func (s *argScript) model() *mock.Model {
	return mock.New("m", func(*llm.Request) *llm.Response {
		s.turn++
		if !s.loop && s.turn > len(s.bad) {
			return mock.Text("giving up")
		}
		return mock.CallTool("c"+strconv.Itoa(s.turn), "lookup", s.bad[(s.turn-1)%len(s.bad)])
	})
}

func argAgent(t *testing.T, store checkpoint.Checkpointer, m llm.Model, tools []tool.Tool, mws ...agent.Middleware) *agent.Agent {
	t.Helper()
	opts := []agent.Option{agent.WithModel(m), agent.WithTools(tools...), agent.WithMaxTurns(12), agent.WithMiddleware(mws...)}
	if store != nil {
		opts = append(opts, agent.WithCheckpointer(store))
	}
	a, err := agent.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// wave is what one stream (or one resumed continuation) produced.
type wave struct {
	rejected  []core.ArgRejected
	interrupt int
	pending   []core.ApprovalRequest
	done      bool
	failed    bool
}

func drive(r *agent.Run) wave {
	var w wave
	for ev, err := range r.Iter() {
		if err != nil {
			w.failed = true
			continue
		}
		switch e := ev.(type) {
		case core.ArgRejected:
			w.rejected = append(w.rejected, e)
		case core.Interrupted:
			w.interrupt++
			w.pending = e.Pending
		case core.RunDone:
			w.done = true
		case core.RunFailed:
			w.failed = true
		}
	}
	if _, err := r.Wait(); err != nil {
		w.failed = true
	}
	return w
}

// --- scenarios ---------------------------------------------------------------

// TestArgGuardCountsAcrossDifferentBadArgs is the background gap stated as a test:
// three refusals of one tool, each for a different reason. LoopGuard's repeat_call
// needs an identical call signature and its error_streak an identical first line of
// error text, so nothing else would notice this sequence.
func TestArgGuardCountsAcrossDifferentBadArgs(t *testing.T) {
	runs := 0
	store := checkpoint.NewMemory()
	script := &argScript{bad: []string{
		`{"limit":5}`,                 // missing q
		`{"q":7,"limit":5}`,           // q of the wrong kind
		`{"q":"cats","limit":"many"}`, // limit of the wrong kind
	}}
	guard := middleware.ArgGuard(middleware.ArgGuardOptions{WarnThreshold: 3, EscalateThreshold: 100})
	a := argAgent(t, store, script.model(), []tool.Tool{lookupTool{runs: &runs}}, guard)

	ctx := context.Background()
	w := drive(a.Stream(ctx, "go", agent.OnThread("t1")))

	if len(w.rejected) != 3 {
		t.Fatalf("ArgRejected events = %d (%v), want 3", len(w.rejected), w.rejected)
	}
	for i, e := range w.rejected {
		want := core.ArgRejected{Tool: "lookup", Class: "schema_invalid", Count: i + 1, Step: i}
		if e != want {
			t.Fatalf("event %d = %+v, want %+v", i, e, want)
		}
	}
	if runs != 0 {
		t.Fatalf("handler ran %d times, want 0: every call was refused first", runs)
	}

	cp, err := store.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got := countMarkers(cp.State.Messages, middleware.WarnMarkerArg); got != 1 {
		t.Fatalf("%s warnings in history = %d, want exactly one", middleware.WarnMarkerArg, got)
	}
	text := markerText(cp.State.Messages, middleware.WarnMarkerArg)
	for _, want := range []string{`"lookup"`, "refused 3 times", `last reason: invalid arguments:`, "required arguments are: q, limit"} {
		if !strings.Contains(text, want) {
			t.Fatalf("warning %q lacks %q", text, want)
		}
	}
	if got := recordOf(cp.State.KV, "lookup"); got != 3 {
		t.Fatalf("State.KV[_argguard.tools].lookup count = %d, want 3", got)
	}
	if _, ok := cp.State.KV["_argguard.actions"].(map[string]string); !ok {
		t.Fatalf("_argguard.actions = %T, want the guard's own map", cp.State.KV["_argguard.actions"])
	}
}

// TestArgGuardEscalateInterruptsThenResumes: the escalation pauses the run through
// the existing HITL path, the pause is on disk, and deciding lets it continue.
func TestArgGuardEscalateInterruptsThenResumes(t *testing.T) {
	runs := 0
	store := checkpoint.NewMemory()
	script := &argScript{bad: []string{`{"limit":5}`}, loop: true}
	guard := middleware.ArgGuard(middleware.ArgGuardOptions{WarnThreshold: 2, EscalateThreshold: 4})
	a := argAgent(t, store, script.model(), []tool.Tool{lookupTool{runs: &runs}}, guard)

	ctx := context.Background()
	w := drive(a.Stream(ctx, "go", agent.OnThread("t1")))
	if w.interrupt != 1 || len(w.pending) != 1 {
		t.Fatalf("first wave: interrupts=%d pending=%v, want one pause holding one call", w.interrupt, w.pending)
	}
	if len(w.rejected) < 4 {
		t.Fatalf("refusals before the pause = %d, want at least 4", len(w.rejected))
	}
	runsAtPause := runs

	cp, err := store.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if cp.Pending == nil || len(cp.Pending.Pending) == 0 {
		t.Fatalf("the pause left no PendingHITL snapshot: %+v", cp.Pending)
	}

	cont, err := a.Resume(ctx, "t1", agent.Allow(w.pending[0].CallID))
	if err != nil {
		t.Fatal(err)
	}
	w2 := drive(cont)
	if !w2.done && w2.interrupt == 0 {
		t.Fatal("the resumed run neither continued to a further pause nor finished")
	}
	// The approved call is still a refused one: it never reaches the handler, in
	// the resumed batch or afterwards.
	if runs != runsAtPause {
		t.Fatalf("handler ran %d times after resume, want %d", runs, runsAtPause)
	}
}

// TestArgGuardStopPolicy: OnEscalate=ArgGuardStop ends the run with no pause.
func TestArgGuardStopPolicy(t *testing.T) {
	runs := 0
	store := checkpoint.NewMemory()
	script := &argScript{bad: []string{`{"limit":5}`}, loop: true}
	guard := middleware.ArgGuard(middleware.ArgGuardOptions{
		WarnThreshold: 2, EscalateThreshold: 3, OnEscalate: middleware.ArgGuardStop,
	})
	a := argAgent(t, store, script.model(), []tool.Tool{lookupTool{runs: &runs}}, guard)

	w := drive(a.Stream(context.Background(), "go", agent.OnThread("t1")))
	if w.interrupt != 0 {
		t.Fatalf("interrupts = %d, want none under the stop policy", w.interrupt)
	}
	if !w.done || w.failed {
		t.Fatalf("run done=%v failed=%v, want a clean RunDone", w.done, w.failed)
	}
	if runs != 0 {
		t.Fatalf("handler ran %d times, want 0", runs)
	}
}

// TestArgGuardInterventionBudget: each escalation spends one unit of budget, and
// past it a marked tool is stopped instead of paused again.
func TestArgGuardInterventionBudget(t *testing.T) {
	runs := 0
	store := checkpoint.NewMemory()
	script := &argScript{bad: []string{`{"limit":5}`}, loop: true}
	guard := middleware.ArgGuard(middleware.ArgGuardOptions{
		WarnThreshold: 2, EscalateThreshold: 3, MaxInterventions: 1,
	})
	a := argAgent(t, store, script.model(), []tool.Tool{lookupTool{runs: &runs}}, guard)

	ctx := context.Background()
	w := drive(a.Stream(ctx, "go", agent.OnThread("t1")))
	if w.interrupt != 1 {
		t.Fatalf("first wave interrupts = %d, want 1 (the budget's first unit)", w.interrupt)
	}
	cp, err := store.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got := intOf(cp.State.KV, "_argguard.interventions"); got != 1 {
		t.Fatalf("_argguard.interventions = %d, want 1", got)
	}

	cont, err := a.Resume(ctx, "t1", agent.Allow(w.pending[0].CallID))
	if err != nil {
		t.Fatal(err)
	}
	w2 := drive(cont)
	if w2.interrupt != 0 {
		t.Fatalf("second wave interrupts = %d, want 0 once the budget is spent", w2.interrupt)
	}
	if !w2.done {
		t.Fatal("the second wave should end the run rather than pause again")
	}
	if runs != 0 {
		t.Fatalf("handler ran %d times, want 0", runs)
	}
}

// TestArgGuardIgnoresHandlerIsError: a tool that runs and reports failure answered
// the call, so an outage does not count against the model's argument quality.
func TestArgGuardIgnoresHandlerIsError(t *testing.T) {
	runs := 0
	store := checkpoint.NewMemory()
	model := mock.New("m", func(*llm.Request) *llm.Response {
		return mock.CallTool("c1", "flaky", `{}`)
	})
	guard := middleware.ArgGuard(middleware.ArgGuardOptions{WarnThreshold: 2, EscalateThreshold: 3})
	a := argAgent(t, store, model, []tool.Tool{flakyTool{runs: &runs}}, guard)

	w := drive(a.Stream(context.Background(), "go", agent.OnThread("t1")))
	if runs == 0 {
		t.Fatal("the flaky handler never ran, so the scenario proves nothing")
	}
	if len(w.rejected) != 0 {
		t.Fatalf("ArgRejected events = %v, want none for results the tool reported itself", w.rejected)
	}
	cp, err := store.Latest(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := cp.State.KV["_argguard.tools"]; ok {
		t.Fatalf("ledger = %v, want nothing recorded", v)
	}
}

// TestArgGuardWithLoopGuard: both guards may fire on the same batch. The merged
// directive is still an interrupt, and each keeps its marks in its own namespace.
func TestArgGuardWithLoopGuard(t *testing.T) {
	runs := 0
	store := checkpoint.NewMemory()
	// One identical malformed call, repeated: a repeated signature for LoopGuard and
	// repeated refusals of one tool for ArgGuard. Both escalate into the same step's
	// BeforeTool gate, which is the point of the scenario.
	script := &argScript{bad: []string{`{"limit":5}`}, loop: true}
	arg := middleware.ArgGuard(middleware.ArgGuardOptions{WarnThreshold: 1, EscalateThreshold: 2})
	loop := middleware.LoopGuard(middleware.LoopGuardOptions{RepeatThreshold: 2})
	a := argAgent(t, store, script.model(), []tool.Tool{lookupTool{runs: &runs}}, loop, arg)

	ctx := context.Background()
	w := drive(a.Stream(ctx, "go", agent.OnThread("t1")))
	if w.interrupt != 1 {
		t.Fatalf("interrupts = %d, want 1 (both guards escalate on this batch)", w.interrupt)
	}
	cp, err := store.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got := intOf(cp.State.KV, "_argguard.interventions"); got < 1 {
		t.Fatalf("_argguard.interventions = %d, want at least 1", got)
	}
	if got := intOf(cp.State.KV, "_loopguard.interventions"); got < 1 {
		t.Fatalf("_loopguard.interventions = %d, want at least 1", got)
	}
	for _, marker := range []string{middleware.WarnMarkerArg, middleware.WarnMarker} {
		if countMarkers(cp.State.Messages, marker) == 0 {
			t.Fatalf("no %s warning in history", marker)
		}
	}
	// Namespaces: ArgGuard keys its ledger by tool name, LoopGuard keys its marks by
	// call signature. Neither reads nor writes the other's keys.
	if got := recordOf(cp.State.KV, "lookup"); got == 0 {
		t.Fatalf("_argguard.tools has no entry for lookup: %v", cp.State.KV["_argguard.tools"])
	}
	loopActs, _ := cp.State.KV["_loopguard.actions"].(map[string]string)
	argActs, _ := cp.State.KV["_argguard.actions"].(map[string]string)
	if _, cross := loopActs["lookup"]; cross {
		t.Fatalf("ArgGuard's tool-name key leaked into _loopguard.actions: %v", loopActs)
	}
	for key := range argActs {
		if _, ok := loopActs[key]; ok {
			t.Fatalf("both guards share the key %q, so their namespaces are not separate", key)
		}
	}
}

// TestArgGuardDurableAcrossRestart: a count that died with the process would reset
// the ladder exactly where it matters, so a second agent over the same thread keeps
// counting from what the checkpoint decoded.
func TestArgGuardDurableAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	runs := 0

	store1, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	first := &argScript{bad: []string{`{"limit":5}`, `{"q":7,"limit":5}`}, loop: false}
	a1 := argAgent(t, store1, first.model(), []tool.Tool{lookupTool{runs: &runs}},
		middleware.ArgGuard(middleware.ArgGuardOptions{WarnThreshold: 2, EscalateThreshold: 100}))
	w := drive(a1.Stream(ctx, "go", agent.OnThread("t1")))
	if len(w.rejected) != 2 || w.rejected[1].Count != 2 {
		t.Fatalf("process 1 refusals = %v, want two ending at count 2", w.rejected)
	}
	cp, err := store1.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got := recordOf(cp.State.KV, "lookup"); got != 2 {
		t.Fatalf("count after process 1 = %d, want 2", got)
	}
	warnedAfterRestart := countMarkers(cp.State.Messages, middleware.WarnMarkerArg)

	// "Process 2": a brand-new agent and a brand-new file store over the same dir.
	// Its model script has one bad call, so a guard that forgot the ledger would
	// report Count 1 and warn a second time.
	store2, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	second := &argScript{bad: []string{`{"q":"cats","limit":"many"}`}, loop: false}
	a2 := argAgent(t, store2, second.model(), []tool.Tool{lookupTool{runs: &runs}},
		middleware.ArgGuard(middleware.ArgGuardOptions{WarnThreshold: 2, EscalateThreshold: 100}))
	w2 := drive(a2.Stream(ctx, "again", agent.OnThread("t1")))
	if len(w2.rejected) == 0 {
		t.Fatal("process 2 recorded no refusal")
	}
	if got := w2.rejected[len(w2.rejected)-1].Count; got != 3 {
		t.Fatalf("count in process 2 = %d, want 3 (read back from the checkpoint's float64)", got)
	}
	cp2, err := store2.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got := recordOf(cp2.State.KV, "lookup"); got != 3 {
		t.Fatalf("State.KV count = %d, want 3", got)
	}
	if got := countMarkers(cp2.State.Messages, middleware.WarnMarkerArg); got != warnedAfterRestart {
		t.Fatalf("warnings grew across the restart (%d -> %d): the warned flag did not survive", warnedAfterRestart, got)
	}
}

// --- helpers -----------------------------------------------------------------

func countMarkers(msgs []core.Message, marker string) int {
	var n int
	for _, m := range msgs {
		if m.Role == core.RoleUser && strings.Contains(m.Text(), marker) {
			n++
		}
	}
	return n
}

func markerText(msgs []core.Message, marker string) string {
	for _, m := range msgs {
		if m.Role == core.RoleUser && strings.Contains(m.Text(), marker) {
			return m.Text()
		}
	}
	return ""
}

// intOf reads a plain numeric KV entry, tolerating what a file checkpoint decodes
// back as float64.
func intOf(kv map[string]any, key string) int {
	switch v := kv[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return 0
}

// recordOf reads one tool's refusal count out of the guard's ledger in either shape.
func recordOf(kv map[string]any, name string) int {
	tools, ok := kv["_argguard.tools"].(map[string]any)
	if !ok {
		return 0
	}
	rec, ok := tools[name].(map[string]any)
	if !ok {
		return 0
	}
	return intOf(rec, "count")
}
