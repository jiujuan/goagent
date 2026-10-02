package middleware

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"strings"
)

func newArgLC(kv map[string]any) *agent.LoopContext {
	if kv == nil {
		kv = map[string]any{}
	}
	return &agent.LoopContext{
		RunContext: &agent.RunContext{Context: context.Background(), State: &core.State{KV: kv}},
	}
}

func refusedCall(name string, class agent.RejectClass) agent.ToolRejection {
	return agent.ToolRejection{
		Call:   core.ToolCall{ID: "c1", Name: name, Args: json.RawMessage(`{}`)},
		Class:  class,
		Detail: "invalid arguments: missing required field \"q\"",
	}
}

// TestArgGuardCountsOnlyConfiguredClasses: the default set is the three
// argument-side classes, and Count replaces it rather than extending it.
func TestArgGuardCountsOnlyConfiguredClasses(t *testing.T) {
	lc := newArgLC(nil)
	g := ArgGuard(ArgGuardOptions{}).(*argGuard)
	for _, class := range []agent.RejectClass{agent.RejectUnknownTool, agent.RejectPrepareFailed, agent.RejectSchemaInvalid} {
		g.OnToolReject(lc, refusedCall("search", class))
	}
	g.OnToolReject(lc, refusedCall("search", agent.RejectHandlerError))
	g.OnToolReject(lc, refusedCall("search", agent.RejectTimedOut))
	if got := loadArgLedger(lc).tools["search"].count; got != 3 {
		t.Fatalf("default set counted %d refusals, want 3 (handler error and timeout are not counted)", got)
	}

	lc2 := newArgLC(nil)
	g2 := ArgGuard(ArgGuardOptions{Count: []agent.RejectClass{agent.RejectHandlerError, agent.RejectTimedOut}}).(*argGuard)
	g2.OnToolReject(lc2, refusedCall("fetch", agent.RejectSchemaInvalid))
	g2.OnToolReject(lc2, refusedCall("fetch", agent.RejectHandlerError))
	g2.OnToolReject(lc2, refusedCall("fetch", agent.RejectTimedOut))
	led := loadArgLedger(lc2)
	if got := led.tools["fetch"].count; got != 2 {
		t.Fatalf("custom set counted %d refusals, want 2", got)
	}
	if _, ok := led.tools["search"]; ok {
		t.Fatal("the custom set leaked a record for a tool it never saw")
	}
}

// TestArgLedgerResumeShapes checks the guard reads what a JSONL checkpoint decoded
// back (map[string]any with float64 numbers) as well as what a live run holds.
func TestArgLedgerResumeShapes(t *testing.T) {
	lc := newArgLC(map[string]any{
		kvArgTools: map[string]any{
			"search": map[string]any{"count": float64(4), "warned": true},
		},
		kvArgActions:       map[string]any{"search": "interrupt"},
		kvArgInterventions: float64(2),
	})
	led := loadArgLedger(lc)
	if got := led.tools["search"]; got.count != 4 || !got.warned {
		t.Fatalf("resumed record = %+v, want {count:4 warned:true}", got)
	}
	if led.actions["search"] != "interrupt" {
		t.Fatalf("resumed mark = %q, want interrupt", led.actions["search"])
	}
	if led.interventions != 2 {
		t.Fatalf("resumed interventions = %d, want 2", led.interventions)
	}

	// A record that is not a map is skipped rather than panicking.
	lc.State.KV[kvArgTools] = map[string]any{"broken": "not a map"}
	if got := loadArgLedger(lc).tools["broken"]; got.count != 0 {
		t.Fatalf("broken record read as %+v, want the zero record", got)
	}
}

// TestArgLedgerSurvivesJSON writes the ledger the way OnToolReject does, sends it
// through JSON, and reads it back — the file checkpointer's whole round trip.
func TestArgLedgerSurvivesJSON(t *testing.T) {
	lc := newArgLC(nil)
	g := ArgGuard(ArgGuardOptions{WarnThreshold: 2, EscalateThreshold: 100}).(*argGuard)
	g.OnToolReject(lc, refusedCall("search", agent.RejectSchemaInvalid))
	g.OnToolReject(lc, refusedCall("search", agent.RejectSchemaInvalid))
	g.OnToolReject(lc, refusedCall("search", agent.RejectSchemaInvalid))

	raw, err := json.Marshal(lc.State.KV)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	lc2 := newArgLC(decoded)
	led := loadArgLedger(lc2)
	rec := led.tools["search"]
	if rec.count != 3 || !rec.warned {
		t.Fatalf("after JSON round trip = %+v, want {count:3 warned:true} (json=%s)", rec, raw)
	}
}

// TestRequiredArgsDegrades: every unreadable case drops the clause instead of
// failing the warning.
func TestRequiredArgsDegrades(t *testing.T) {
	withTools := func(schema string) *agent.LoopContext {
		lc := newArgLC(nil)
		lc.Request = &llm.Request{Tools: []llm.ToolSchema{
			{Name: "other", Parameters: json.RawMessage(`{"required":["zzz"]}`)},
			{Name: "search", Parameters: json.RawMessage(schema)},
		}}
		return lc
	}

	if got := requiredArgs(newArgLC(nil), "search"); got != nil {
		t.Fatalf("without a request = %v, want nil", got)
	}
	if got := requiredArgs(withTools(`{"type":"object","required":["q","limit"]}`), "search"); len(got) != 2 || got[0] != "q" || got[1] != "limit" {
		t.Fatalf("required = %v, want [q limit]", got)
	}
	if got := requiredArgs(withTools(`{"type":"object"}`), "search"); got != nil {
		t.Fatalf("schema without required = %v, want nil", got)
	}
	if got := requiredArgs(withTools(`{"required":"q"}`), "search"); got != nil {
		t.Fatalf("required of the wrong kind = %v, want nil", got)
	}
	if got := requiredArgs(withTools(`not json`), "search"); got != nil {
		t.Fatalf("unparsable schema = %v, want nil", got)
	}
	if got := requiredArgs(withTools(`{"required":["q"]}`), "absent"); got != nil {
		t.Fatalf("unadvertised tool = %v, want nil", got)
	}
	// Non-string entries are skipped, not fatal.
	if got := requiredArgs(withTools(`{"required":["q",7]}`), "search"); len(got) != 1 || got[0] != "q" {
		t.Fatalf("mixed required = %v, want [q]", got)
	}
}

// TestArgMarkIsConsumedOnce: one escalation acts once, so a tool the model later
// calls correctly is not stopped by a mark it already paid for.
func TestArgMarkIsConsumedOnce(t *testing.T) {
	g := ArgGuard(ArgGuardOptions{}).(*argGuard)
	marked := func(act string) *agent.LoopContext {
		return newArgLC(map[string]any{kvArgActions: map[string]string{"search": act}})
	}

	lc := marked(actionInterrupt)
	d, err := g.BeforeTool(lc, &core.ToolCall{ID: "c1", Name: "search"})
	if err != nil || d.Kind != core.Interrupt {
		t.Fatalf("first honored mark = %v (%v), want interrupt", d.Kind, err)
	}
	if d.Reason == "" || !strings.Contains(d.Reason, WarnMarkerArg) {
		t.Fatalf("reason = %q, want the guard marker in it", d.Reason)
	}
	if _, ok := argMarkedAction(lc, "search"); ok {
		t.Fatal("the mark should have been consumed")
	}
	if d, _ := g.BeforeTool(lc, &core.ToolCall{ID: "c2", Name: "search"}); d.Kind != core.Continue {
		t.Fatalf("second call without a mark = %v, want continue", d.Kind)
	}

	if d, _ := g.BeforeTool(marked(actionStop), &core.ToolCall{ID: "c1", Name: "search"}); d.Kind != core.Stop {
		t.Fatalf("stop mark = %v, want stop", d.Kind)
	}

	// An unmarked tool is never touched, however spent the budget is.
	spent := newArgLC(map[string]any{kvArgInterventions: float64(9)})
	if d, _ := g.BeforeTool(spent, &core.ToolCall{ID: "c1", Name: "other"}); d.Kind != core.Continue {
		t.Fatalf("unmarked tool with a spent budget = %v, want continue", d.Kind)
	}
}

// TestArgBudgetTurnsMarksIntoStops: the choice is made when the mark is written,
// against the budget still unspent, so the escalation that spends the last unit is
// still the gentle one and only the next one stops.
func TestArgBudgetTurnsMarksIntoStops(t *testing.T) {
	g := ArgGuard(ArgGuardOptions{WarnThreshold: 100, EscalateThreshold: 2, MaxInterventions: 1}).(*argGuard)
	lc := newArgLC(nil)
	g.OnToolReject(lc, refusedCall("search", agent.RejectSchemaInvalid))
	if _, ok := argMarkedAction(lc, "search"); ok {
		t.Fatal("no mark yet, so nothing should be queued")
	}

	g.OnToolReject(lc, refusedCall("search", agent.RejectSchemaInvalid))
	if act, ok := argMarkedAction(lc, "search"); !ok || act != actionInterrupt {
		t.Fatalf("first escalation mark = %q (%v), want interrupt", act, ok)
	}
	if got := argInterventions(lc); got != 1 {
		t.Fatalf("interventions = %d, want 1", got)
	}

	// Budget spent: the next escalation marks the tool to stop instead of pause.
	lc2 := newArgLC(map[string]any{kvArgInterventions: 1})
	g2 := ArgGuard(ArgGuardOptions{WarnThreshold: 100, EscalateThreshold: 1, MaxInterventions: 1}).(*argGuard)
	g2.OnToolReject(lc2, refusedCall("search", agent.RejectSchemaInvalid))
	if act, ok := argMarkedAction(lc2, "search"); !ok || act != actionStop {
		t.Fatalf("escalation past the budget = %q (%v), want stop", act, ok)
	}
}

// TestArgWarningNamesTheRealConsequence: the warning closes by telling the model
// what the next refusals do, and that must be the consequence it will actually
// meet — pausing for a decision while the budget lasts, ending the run after.
func TestArgWarningNamesTheRealConsequence(t *testing.T) {
	pause := ArgGuard(ArgGuardOptions{}).(*argGuard).warningMessage("search", 3, "detail", nil, false).Text()
	if !strings.Contains(pause, "pause this run for a human decision") {
		t.Fatalf("warning with budget left = %q", pause)
	}

	stop := ArgGuard(ArgGuardOptions{}).(*argGuard).warningMessage("search", 3, "detail", nil, true).Text()
	if !strings.Contains(stop, "end this run") || strings.Contains(stop, "pause this run") {
		t.Fatalf("warning after the budget = %q", stop)
	}

	forced := ArgGuard(ArgGuardOptions{OnEscalate: ArgGuardStop}).(*argGuard).warningMessage("search", 3, "detail", nil, false).Text()
	if !strings.Contains(forced, "end this run") {
		t.Fatalf("warning under the stop policy = %q", forced)
	}
	// The refused call's own text is quoted back, and a multi-line one is clipped
	// to its first line so the warning stays one message.
	withReason := ArgGuard(ArgGuardOptions{}).(*argGuard).warningMessage("search", 3, "line one\nline two", []string{"q"}, false).Text()
	if !strings.Contains(withReason, "last reason: line one") || strings.Contains(withReason, "line two") {
		t.Fatalf("warning = %q, want only the first line of the reason", withReason)
	}
	if !strings.Contains(withReason, "required arguments are: q") {
		t.Fatalf("warning = %q, want the required list", withReason)
	}
}

// TestArgLedgerWritesPlainMaps guards the one thing that would silently break
// durability: a value JSON cannot describe.
func TestArgLedgerWritesPlainMaps(t *testing.T) {
	lc := newArgLC(nil)
	g := ArgGuard(ArgGuardOptions{WarnThreshold: 100, EscalateThreshold: 2}).(*argGuard)
	g.OnToolReject(lc, refusedCall("search", agent.RejectSchemaInvalid))
	g.OnToolReject(lc, refusedCall("search", agent.RejectSchemaInvalid))

	tools, ok := lc.State.KV[kvArgTools].(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want map[string]any", kvArgTools, lc.State.KV[kvArgTools])
	}
	inner, ok := tools["search"].(map[string]any)
	if !ok {
		t.Fatalf("record = %T, want map[string]any", tools["search"])
	}
	if _, isStruct := inner["count"].(json.RawMessage); isStruct {
		t.Fatal("count must be a plain number")
	}
	if got := loadArgLedger(lc).actions["search"]; got != actionInterrupt {
		t.Fatalf("escalation mark = %q, want interrupt", got)
	}
	if got := loadArgLedger(lc).interventions; got != 1 {
		t.Fatalf("interventions = %d, want 1", got)
	}
}
