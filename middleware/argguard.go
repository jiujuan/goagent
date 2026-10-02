package middleware

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
)

// ArgGuardPolicy selects what an escalated tool does to the run.
type ArgGuardPolicy int

const (
	// ArgGuardInterrupt pauses the run for a human decision: the marked call
	// goes through the existing HITL checkpoint/Resume path. Default.
	ArgGuardInterrupt ArgGuardPolicy = iota
	// ArgGuardStop ends the run, keeping the last assistant message as result.
	ArgGuardStop
)

// ArgGuardOptions configures ArgGuard. Zero values select the defaults.
type ArgGuardOptions struct {
	// WarnThreshold is how many refusals of one tool earn it a single
	// [arg-guard] warning (default 3, min 1).
	WarnThreshold int
	// EscalateThreshold is how many refusals of one tool escalate to a guard
	// action on the following step (default 6).
	EscalateThreshold int
	// OnEscalate selects the escalated action (default ArgGuardInterrupt).
	OnEscalate ArgGuardPolicy
	// Count lists the refusal classes that are counted at all. It defaults to
	// the three argument-side ones (unknown tool, failed argument preparation,
	// schema invalid). Handler errors and timeouts are left out on purpose: a
	// failing remote service is not a model that cannot call the tool. Add
	// agent.RejectHandlerError or agent.RejectTimedOut to count them too.
	Count []agent.RejectClass
	// MaxInterventions is how many escalations one run may spend; once spent, a
	// marked tool is stopped rather than interrupted (default 3). Calls that
	// carry no mark keep running either way.
	MaxInterventions int
	// OnDetect, if set, is called for every counted refusal right after its
	// event is published (same role as LoopGuardOptions.OnDetect).
	OnDetect func(tool string, count int, class agent.RejectClass)
}

// WarnMarkerArg prefixes every ArgGuard warning so the conversation shows which
// message came from the guard.
const WarnMarkerArg = "[arg-guard]"

const (
	kvArgTools         = "_argguard.tools"         // tool -> {"count": int, "warned": bool}
	kvArgActions       = "_argguard.actions"       // tool -> "interrupt" | "stop"
	kvArgInterventions = "_argguard.interventions" // int, escalations spent this run
)

// ArgGuard counts the tool calls the loop refused before answering them and acts
// when one tool's count keeps climbing however often the model changes its
// arguments.
//
// That last clause is the whole reason it is not a LoopGuard rule: LoopGuard's
// repeat_call needs an identical call signature and its error_streak needs an
// identical first line of error text, and a model that is groping its way through
// a schema changes both every time (missing "q", then missing "limit", then "q"
// as a string). ArgGuard keys on the tool name alone, so that sequence counts as
// three refusals of one tool.
//
// Ladder, per tool: at WarnThreshold it steers one [arg-guard] warning into the
// next model call naming the schema's required arguments; at EscalateThreshold it
// marks the tool and the next step's BeforeTool honors that mark with an interrupt
// (default) or a stop. The mark is consumed when honored, so a tool the model
// later calls correctly is never blocked by an old mark. MaxInterventions bounds
// how many escalations one run spends; after that a marked call stops instead of
// pausing, which is what keeps "interrupt, approve, repeat" from becoming the new
// loop.
//
// Everything is recorded in State.KV, not in memory: the middleware instance is
// shared across runs, and a count that dies on a HITL pause or a process restart
// would reset the ladder exactly where it matters (same stance as LoopGuard and
// RunBudget).
//
// Boundary with LoopGuard: both may fire on the same batch and neither dedups the
// other. Their marks live in separate KV namespaces, and core.Resolve picks the
// higher-precedence directive. Do not merge the two components — they watch
// different things and their thresholds and exemptions mean different things.
func ArgGuard(o ArgGuardOptions) agent.Middleware {
	if o.WarnThreshold < 1 {
		o.WarnThreshold = 3
	}
	if o.EscalateThreshold <= 0 {
		o.EscalateThreshold = 6
	}
	if o.MaxInterventions <= 0 {
		o.MaxInterventions = 3
	}
	counted := make(map[agent.RejectClass]bool, len(o.Count))
	if len(o.Count) == 0 {
		counted[agent.RejectUnknownTool] = true
		counted[agent.RejectPrepareFailed] = true
		counted[agent.RejectSchemaInvalid] = true
	} else {
		for _, class := range o.Count {
			counted[class] = true
		}
	}
	return &argGuard{opts: o, counted: counted}
}

type argGuard struct {
	agent.BaseMiddleware
	opts    ArgGuardOptions
	counted map[agent.RejectClass]bool
}

// OnToolReject updates one tool's ledger and takes this refusal's action.
//
// It runs on the loop's goroutine after the whole batch has joined (see
// agent.ToolRejecter), which is why the State.KV writes below need no locking —
// unlike AfterTool. If that dispatch ever moves into a tool's worker goroutine,
// this method must serialize its own ledger access.
func (g *argGuard) OnToolReject(lc *agent.LoopContext, r agent.ToolRejection) {
	if lc.State == nil || !g.counted[r.Class] {
		return // nowhere durable to count, or a class this guard is not watching
	}
	name := r.Call.Name
	ledger := loadArgLedger(lc)
	rec := ledger.tools[name]
	rec.count++
	ledger.tools[name] = rec

	g.observe(lc, name, rec.count, r.Class)

	switch {
	case rec.count >= g.opts.EscalateThreshold:
		// The action is chosen here, against the budget still unspent: an
		// escalation that spends the last unit should still be the gentle one, and
		// only the next should stop (same ordering as LoopGuard).
		act := actionInterrupt
		if g.opts.OnEscalate == ArgGuardStop || ledger.interventions >= g.opts.MaxInterventions {
			act = actionStop
		}
		ledger.actions[name] = act
		ledger.interventions++
	case rec.count >= g.opts.WarnThreshold && !rec.warned:
		rec.warned = true
		ledger.tools[name] = rec
		lc.Steer(g.warningMessage(name, rec.count, r.Detail, requiredArgs(lc, name)))
	}
	lc.State.Apply(ledger.ops()...)
}

// BeforeTool honors the mark the previous batch left for this tool, once.
func (g *argGuard) BeforeTool(lc *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	act, ok := argMarkedAction(lc, c.Name)
	if !ok {
		return core.Directive{}, nil
	}
	clearArgMark(lc, c.Name)

	reason := WarnMarkerArg + " tool " + c.Name + " was refused repeatedly (see the preceding " + WarnMarkerArg + " warning)"
	if act == actionStop {
		return core.Directive{Kind: core.Stop, Reason: reason + "; intervention budget spent"}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: reason}, nil
}

func (g *argGuard) observe(lc *agent.LoopContext, name string, count int, class agent.RejectClass) {
	if lc.Bus != nil {
		lc.Bus.Publish(lc.Topic, core.ArgRejected{Tool: name, Class: class.String(), Count: count, Step: lc.Step})
	}
	if g.opts.OnDetect != nil {
		g.opts.OnDetect(name, count, class)
	}
}

// warningMessage tells the model which tool it is failing, how often, why last
// time, and what that tool requires. The required list is dropped when the
// advertisement cannot be read (see requiredArgs).
func (g *argGuard) warningMessage(name string, count int, detail string, required []string) core.Message {
	var b strings.Builder
	b.WriteString(WarnMarkerArg + " Tool " + strconv.Quote(name) + " has been refused " + strconv.Itoa(count) + " times")
	if reason := firstLine(detail); reason != "" {
		b.WriteString("; last reason: " + reason)
	}
	b.WriteString(".")
	if len(required) > 0 {
		b.WriteString(" Its required arguments are: " + strings.Join(required, ", ") + ".")
	}
	b.WriteString(" Supply every required argument in one call, get what is missing another way, or answer without this tool:")
	b.WriteString(" further refusals will " + g.escalationWords() + ".")
	return core.UserText(b.String())
}

func (g *argGuard) escalationWords() string {
	if g.opts.OnEscalate == ArgGuardStop {
		return "end this run"
	}
	return "pause this run for a human decision"
}

// --- the advertised schema's required list -----------------------------------

// requiredArgs reads the required fields of one advertised tool, so the warning
// can name them. Anything missing yields nil and the clause is dropped: no
// request yet (a resumed batch never went through CallModel), the tool is not
// advertised, the schema has no required keyword, or it will not parse.
func requiredArgs(lc *agent.LoopContext, name string) []string {
	if lc.Request == nil {
		return nil
	}
	for _, t := range lc.Request.Tools {
		if t.Name != name {
			continue
		}
		var schema map[string]any
		if err := json.Unmarshal(t.Parameters, &schema); err != nil {
			return nil
		}
		list, ok := schema["required"].([]any)
		if !ok {
			return nil
		}
		out := make([]string, 0, len(list))
		for _, v := range list {
			if field, ok := v.(string); ok {
				out = append(out, field)
			}
		}
		return out
	}
	return nil
}

// --- State.KV ledger ---------------------------------------------------------

// argToolRecord is one tool's entry: how often it was refused and whether the
// warning has been spent on it already.
type argToolRecord struct {
	count  int
	warned bool
}

// argLedger is the guard's whole durable state.
type argLedger struct {
	tools         map[string]argToolRecord
	actions       map[string]string
	interventions int
}

func loadArgLedger(lc *agent.LoopContext) argLedger {
	l := argLedger{tools: map[string]argToolRecord{}, actions: map[string]string{}}
	if lc.State == nil {
		return l
	}
	switch m := lc.State.KV[kvArgTools].(type) {
	case map[string]any:
		for name, v := range m {
			fields, ok := v.(map[string]any)
			if !ok {
				continue
			}
			l.tools[name] = argToolRecord{count: argInt(fields["count"]), warned: argBool(fields["warned"])}
		}
	}
	for name, act := range argActionsMap(lc) {
		l.actions[name] = act
	}
	l.interventions = argInterventions(lc)
	return l
}

// ops writes the ledger back as plain maps: a struct with unexported fields would
// marshal to {} and the counts would silently vanish on resume.
func (l argLedger) ops() []core.StateOp {
	tools := make(map[string]any, len(l.tools))
	for name, rec := range l.tools {
		tools[name] = map[string]any{"count": rec.count, "warned": rec.warned}
	}
	actions := make(map[string]string, len(l.actions))
	for name, act := range l.actions {
		actions[name] = act
	}
	return []core.StateOp{
		{Kind: core.OpSetKV, Key: kvArgTools, Value: tools},
		{Kind: core.OpSetKV, Key: kvArgActions, Value: actions},
		{Kind: core.OpSetKV, Key: kvArgInterventions, Value: l.interventions},
	}
}

// argActionsMap reads the marks. A live run sees the map[string]string this
// package wrote; a resumed one sees the map[string]any the checkpoint decoded.
func argActionsMap(lc *agent.LoopContext) map[string]string {
	if lc.State == nil {
		return nil
	}
	out := map[string]string{}
	switch m := lc.State.KV[kvArgActions].(type) {
	case map[string]string:
		for k, v := range m {
			out[k] = v
		}
	case map[string]any:
		for k, v := range m {
			if act, ok := v.(string); ok {
				out[k] = act
			}
		}
	}
	return out
}

func argMarkedAction(lc *agent.LoopContext, name string) (string, bool) {
	act, ok := argActionsMap(lc)[name]
	return act, ok
}

// clearArgMark consumes one tool's mark: the guard acts once per escalation, and a
// stale mark would keep interrupting a tool the model has since learned to call.
func clearArgMark(lc *agent.LoopContext, name string) {
	if lc.State == nil {
		return
	}
	acts := argActionsMap(lc)
	delete(acts, name)
	lc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: kvArgActions, Value: acts})
}

func argInterventions(lc *agent.LoopContext) int {
	if lc.State == nil {
		return 0
	}
	return argInt(lc.State.KV[kvArgInterventions])
}

// argInt and argBool tolerate what a JSONL checkpoint decodes back (every number
// a float64) as well as what a live run holds.
func argInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

func argBool(v any) bool {
	b, _ := v.(bool)
	return b
}

var (
	_ agent.Middleware   = (*argGuard)(nil)
	_ agent.ToolRejecter = (*argGuard)(nil)
)
