package middleware

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Price is a flat USD price per million tokens, used to turn reported Usage
// into a cost figure. A single flat table is deliberately approximate for
// agents that route across models (fallback): the recorded figure is the cost
// of the configured price applied to reported usage, not a real invoice.
type Price struct {
	InputPerMTok  float64
	OutputPerMTok float64
}

// BudgetReport is the cumulative per-run usage snapshot carried to OnExceed
// when a cap is first crossed. The same figures live in State.KV["_budget"]
// for durable inspection (RunFinisher, memx consolidation, audit).
type BudgetReport struct {
	InputTokens  int
	OutputTokens int
	Turns        int
	CostUSD      float64
	Elapsed      time.Duration
	Exceeded     []string // resource names that crossed their cap
}

// RunBudgetOptions configures RunBudget. All caps default to 0 (no cap); with
// every cap zero the middleware still records usage into State.KV but changes
// nothing else.
type RunBudgetOptions struct {
	MaxInputTokens  int
	MaxOutputTokens int
	MaxTotalTokens  int
	// MaxCostUSD requires Price to be set to be meaningful.
	MaxCostUSD float64
	Price      Price
	// MaxDuration bounds wall-clock time since the run's first model call,
	// counted across HITL pauses and resumes (the conservative queue-consumer
	// reading).
	MaxDuration time.Duration
	// MaxTurns bounds model-call count over the whole run lifetime including
	// resumed waves — orthogonal to the loop's per-wave MaxTurns.
	MaxTurns int
	// WarnRatio is the fraction of each cap at which one warning is steered
	// into the conversation per resource (default 0.8).
	WarnRatio float64
	// NoWrapUpLastTurn disables the automatic wrap-up instruction on the
	// loop's final turn (enabled by default once any cap is set).
	NoWrapUpLastTurn bool
	// Now is the clock, injectable for tests (default time.Now), same
	// convention as CircuitOptions.Now.
	Now func() time.Time
	// OnExceed, if set, is called once when a cap is first crossed, right
	// before the run enters wrap-up mode.
	OnExceed func(BudgetReport)
}

// RunBudget enforces per-run cost/time limits and turns "out of budget" into a
// usable final answer instead of an error. It records every model call's
// reported Usage into State.KV (durable, resume-safe — same stateless-derivation
// stance as LoopGuard), warns once per resource when WarnRatio of its cap is
// reached, and on crossing a hard cap enters wrap-up mode: the next
// ModifyRequest empties req.Tools and appends a "produce your final answer
// now" instruction, so the loop's natural no-tool-call exit condition ends the
// run with that answer (RunDone, not RunFailed).
//
// The same wrap-up applies on the loop's final turn (Step == MaxTurns-1, seen
// via the new LoopContext.MaxTurns field), so burning the whole step budget
// yields a summary instead of ErrMaxTurnsExceeded. If a model still emits a
// tool call on a request that advertised no tools, BeforeTool stops the run
// gracefully (Stop directive → RunDone).
//
// Known inaccuracy: calls that fail mid-stream were billed by the provider but
// never reported Usage, so they are invisible to the record.
func RunBudget(o RunBudgetOptions) agent.Middleware {
	if o.WarnRatio <= 0 || o.WarnRatio > 1 {
		o.WarnRatio = 0.8
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	b := &runBudget{opts: o}
	b.caps = b.hasCaps()
	return b
}

// WarnMarkerBudget prefixes every budget warning so the conversation shows
// which message came from the guard.
const WarnMarkerBudget = "[run-budget]"

const (
	kvBudget = "_budget"

	budgetInstr = "\n\n[run-budget] Tools are no longer available for this reply. Produce your final answer now: summarize what was accomplished and what remains. Do not request any tool calls."
)

// Resource names for caps and events.
const (
	ResInputTokens  = "input_tokens"
	ResOutputTokens = "output_tokens"
	ResTotalTokens  = "total_tokens"
	ResCost         = "cost"
	ResDuration     = "duration"
	ResTurns        = "turns"
)

type runBudget struct {
	agent.BaseMiddleware
	opts RunBudgetOptions
	caps bool // at least one hard cap configured
}

// budgetLedger is the decoded State.KV["_budget"] record. Numbers are stored
// as float64 so the live value and the JSONL-resumed value have one shape.
type budgetLedger struct {
	in, out  float64
	cost     float64
	turns    float64
	started  time.Time
	hasStart bool
	warned   map[string]bool
	wrapup   bool
}

func (b *runBudget) hasCaps() bool {
	o := b.opts
	return o.MaxInputTokens > 0 || o.MaxOutputTokens > 0 || o.MaxTotalTokens > 0 ||
		o.MaxCostUSD > 0 || o.MaxDuration > 0 || o.MaxTurns > 0
}

func (b *runBudget) AfterModel(lc *agent.LoopContext, resp *llm.Response) (core.Directive, error) {
	if lc.State == nil {
		return core.Directive{}, nil
	}
	l := loadBudgetLedger(lc)
	if !l.hasStart {
		l.started, l.hasStart = b.opts.Now(), true
	}
	if u := resp.Usage; u != nil {
		l.in += float64(u.InputTokens)
		l.out += float64(u.OutputTokens)
		l.cost += float64(u.InputTokens)/1e6*b.opts.Price.InputPerMTok +
			float64(u.OutputTokens)/1e6*b.opts.Price.OutputPerMTok
	}
	l.turns++
	b.enforce(lc, &l)
	saveBudget(lc, l)
	return core.Directive{}, nil
}

func (b *runBudget) BeforeModel(lc *agent.LoopContext) (core.Directive, error) {
	if lc.State == nil {
		return core.Directive{}, nil
	}
	l := loadBudgetLedger(lc)
	if !l.hasStart {
		l.started, l.hasStart = b.opts.Now(), true
	}
	b.enforce(lc, &l)
	saveBudget(lc, l)
	return core.Directive{}, nil
}

// ModifyRequest executes wrap-up mode: with the durable flag set, or on the
// loop's final turn when last-turn wrap-up is enabled, the model is offered no
// tools and is instructed to answer for real — the loop then ends the run on
// its natural no-tool-call exit condition.
func (b *runBudget) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	if lc.State == nil {
		return nil
	}
	if loadBudgetLedger(lc).wrapup || b.finalTurn(lc) {
		req.Tools = nil
		req.System += budgetInstr
	}
	return nil
}

// BeforeTool is the wrap-up safeguard: inside wrap-up mode (or on the loop's
// final turn) a call answering a request that advertised no tools is exactly
// the "model ignores the budget stop" case, so the run ends with that message
// via Stop (a RunDone carrying it), never by executing the hallucinated call.
func (b *runBudget) BeforeTool(lc *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	if !b.caps || lc.State == nil || lc.Request == nil || len(lc.Request.Tools) > 0 {
		return core.Directive{}, nil
	}
	if !loadBudgetLedger(lc).wrapup && !b.finalTurn(lc) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Stop, Reason: "run-budget: budget exhausted, tool call " + c.Name + " not executed"}, nil
}

// finalTurn reports whether this step is the loop's last one and last-turn
// wrap-up applies (caps configured, not disabled, MaxTurns observable).
func (b *runBudget) finalTurn(lc *agent.LoopContext) bool {
	return b.caps && !b.opts.NoWrapUpLastTurn && lc.MaxTurns > 0 && lc.Step == lc.MaxTurns-1
}

// enforce checks every configured resource against its cap: a first-time
// crossing flips wrap-up mode (event + OnExceed once), and crossing the warn
// line steers one reminder per resource. It mutates the ledger, not State.
func (b *runBudget) enforce(lc *agent.LoopContext, l *budgetLedger) {
	if !b.caps {
		return
	}
	var exceeded, warns []string
	elapsed := time.Duration(0)
	if l.hasStart {
		elapsed = b.opts.Now().Sub(l.started)
	}
	for _, d := range b.dims(l, elapsed) {
		switch {
		case d.val >= d.cap:
			exceeded = append(exceeded, d.name)
		case !l.warned[d.name] && d.val >= d.cap*b.opts.WarnRatio:
			l.warned[d.name] = true
			warns = append(warns, fmt.Sprintf("%s reached %.0f%% of its limit", d.name, 100*d.val/d.cap))
		}
	}
	if len(warns) > 0 {
		sort.Strings(warns)
		lc.Steer(core.UserText(WarnMarkerBudget + " Budget notice: " + strings.Join(warns,
			"; ") + ". Start winding down: prefer finishing the current step and answer with what you have."))
	}
	if len(exceeded) > 0 && !l.wrapup {
		l.wrapup = true
		report := l.report(exceeded, elapsed)
		if lc.Bus != nil {
			for _, r := range exceeded {
				lc.Bus.Publish(lc.Topic, core.BudgetExceeded{Resource: r, Step: lc.Step})
			}
		}
		if b.opts.OnExceed != nil {
			b.opts.OnExceed(report)
		}
	}
}

type budgetDim struct {
	name string
	cap  float64
	val  float64
}

func (b *runBudget) dims(l *budgetLedger, elapsed time.Duration) []budgetDim {
	o := b.opts
	out := make([]budgetDim, 0, 6)
	add := func(name string, cap float64, val float64) {
		if cap > 0 {
			out = append(out, budgetDim{name, cap, val})
		}
	}
	add(ResInputTokens, float64(o.MaxInputTokens), l.in)
	add(ResOutputTokens, float64(o.MaxOutputTokens), l.out)
	add(ResTotalTokens, float64(o.MaxTotalTokens), l.in+l.out)
	add(ResCost, o.MaxCostUSD, l.cost)
	add(ResDuration, o.MaxDuration.Seconds(), elapsed.Seconds())
	add(ResTurns, float64(o.MaxTurns), l.turns)
	return out
}

func (l budgetLedger) report(exceeded []string, elapsed time.Duration) BudgetReport {
	return BudgetReport{
		InputTokens:  int(l.in),
		OutputTokens: int(l.out),
		Turns:        int(l.turns),
		CostUSD:      l.cost,
		Elapsed:      elapsed,
		Exceeded:     exceeded,
	}
}

// --- State.KV round-trip ------------------------------------------------------

func loadBudgetLedger(lc *agent.LoopContext) budgetLedger {
	l := budgetLedger{warned: map[string]bool{}}
	raw, ok := lc.State.KV[kvBudget].(map[string]any)
	if !ok {
		return l
	}
	l.in = numOf(raw["in"])
	l.out = numOf(raw["out"])
	l.cost = numOf(raw["cost"])
	l.turns = numOf(raw["turns"])
	if w, ok := raw["warned"].(map[string]any); ok {
		for k, v := range w {
			if b, ok := v.(bool); ok {
				l.warned[k] = b
			}
		}
	}
	if s, ok := raw["started"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			l.started, l.hasStart = t, true
		}
	}
	l.wrapup, _ = raw["wrapup"].(bool)
	return l
}

func saveBudget(lc *agent.LoopContext, l budgetLedger) {
	warned := make(map[string]any, len(l.warned))
	for k, v := range l.warned {
		warned[k] = v
	}
	started := ""
	if l.hasStart {
		started = l.started.Format(time.RFC3339Nano)
	}
	lc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: kvBudget, Value: map[string]any{
		"in": l.in, "out": l.out, "cost": l.cost, "turns": l.turns,
		"started": started, "warned": warned, "wrapup": l.wrapup,
	}})
}

func numOf(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

var _ agent.Middleware = (*runBudget)(nil)
