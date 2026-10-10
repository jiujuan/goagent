package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// LoopGuardPolicy selects what an escalated stuck-pattern does to the run.
type LoopGuardPolicy int

const (
	// LoopGuardInterrupt pauses the run for a human decision (the escalated
	// batch goes through the existing HITL checkpoint/Resume path). Default.
	LoopGuardInterrupt LoopGuardPolicy = iota
	// LoopGuardStop ends the run immediately, keeping the last assistant
	// message as the result.
	LoopGuardStop
)

// LoopGuardOptions configures LoopGuard. Zero values select the defaults.
type LoopGuardOptions struct {
	// Window is how many past tool-call batches (steps) a signature is
	// counted over (default 8).
	Window int
	// RepeatThreshold is how many batches a call signature may appear in
	// before it is flagged, counting the current one (default 2, min 2).
	RepeatThreshold int
	// ErrorStreakThreshold is how many consecutive same-signature error tool
	// results flag a call (default 3).
	ErrorStreakThreshold int
	// ExemptTools never trigger detection (polling-style tools). Defaults to
	// just "write_todos".
	ExemptTools []string
	// StripArgKeys removes top-level argument keys before hashing, for
	// volatile fields like request ids.
	StripArgKeys []string
	// OnRepeat selects the escalated action (default LoopGuardInterrupt).
	OnRepeat LoopGuardPolicy
	// MaxInterventions is the per-run budget of guard actions (warn or
	// escalate); beyond it every hit force-stops the run (default 3).
	MaxInterventions int
	// OnDetect, if set, is called for every detected hit after the event is
	// published (same role as CircuitOptions.OnStateChange).
	OnDetect func(rule, reason string, step int)
}

// LoopGuard detects stuck repetition loops in the agent loop and escalates
// before the remaining MaxTurns budget is burned. Two rules, both derived from
// the persisted message history (so verdicts survive HITL pauses and process
// restarts; Compaction never rewrites State, so the history is complete):
//
//   - repeat_call: the same call signature (tool name + canonicalized args)
//     reappears in Window past batches; covers single-call复读 and A/B ping-pong.
//   - error_streak: ErrorStreakThreshold consecutive identical error results
//     (including max_tokens truncation voids) for a tool the model still calls.
//
// Ladder: first hit steers a [loop-guard] warning into the next model call;
// a repeat after that warning escalates per OnRepeat. The per-run count of
// interventions lives in State.KV and survives resume; past
// MaxInterventions hits stop the run outright. Every hit publishes
// core.StuckDetected.
//
// Detection runs in AfterModel (the only serial point that sees the batch
// before any tool executes); escalation is carried to BeforeTool through a
// State.KV mark so the pause takes the existing PendingHITL checkpoint path.
// Truncated replies never reach BeforeTool, so an escalation there stops the
// run directly in AfterModel.
func LoopGuard(o LoopGuardOptions) agent.Middleware {
	if o.Window <= 0 {
		o.Window = 8
	}
	if o.RepeatThreshold < 2 {
		o.RepeatThreshold = 2
	}
	if o.ErrorStreakThreshold <= 0 {
		o.ErrorStreakThreshold = 3
	}
	if o.MaxInterventions <= 0 {
		o.MaxInterventions = 3
	}
	if o.ExemptTools == nil {
		o.ExemptTools = []string{"write_todos"}
	}
	g := &loopGuard{opts: o, exempt: toSet(o.ExemptTools), strip: toSet(o.StripArgKeys)}
	return g
}

const (
	// WarnMarker prefixes every injected warning so recurrence-after-warning
	// is recognizable from history alone (no in-memory state).
	WarnMarker = "[loop-guard]"

	kvLoopActions       = "_loopguard.actions"       // map sig -> "interrupt"|"stop"
	kvLoopInterventions = "_loopguard.interventions" // int, per-run budget spent

	actionInterrupt = "interrupt"
	actionStop      = "stop"
)

type loopGuard struct {
	agent.BaseMiddleware
	opts   LoopGuardOptions
	exempt map[string]bool
	strip  map[string]bool
}

// guardHit is one stuck-pattern flagged for a pending call signature.
type guardHit struct {
	sig      string
	tool     string
	rules    []string
	reason   string
	escalate bool // already warned since the last occurrence
}

func (g *loopGuard) AfterModel(lc *agent.LoopContext, resp *llm.Response) (core.Directive, error) {
	if lc.State == nil {
		return core.Directive{}, nil // no durable KV to carry budget/marks
	}
	calls := resp.Message.ToolCalls()
	if len(calls) == 0 {
		return core.Directive{}, nil
	}
	hits := g.detect(lc, calls)
	if len(hits) == 0 {
		return core.Directive{}, nil
	}

	budget := interventions(lc)
	actions := make(map[string]string)
	var warns []string
	escalated := false
	for _, h := range hits {
		g.observe(lc, strings.Join(h.rules, "+"), h.reason)
		if h.escalate || budget >= g.opts.MaxInterventions {
			escalated = true
			act := actionInterrupt
			if g.opts.OnRepeat == LoopGuardStop || budget >= g.opts.MaxInterventions {
				act = actionStop
			}
			actions[h.sig] = act
		} else {
			warns = append(warns, h.reason)
		}
	}

	// Apply the step's actions first, then warn. The interventions counter and
	// the marks must be visible to this step's BeforeTool (same *State).
	lc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: kvLoopActions, Value: actions})
	lc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: kvLoopInterventions, Value: budget + 1})
	if len(warns) > 0 {
		lc.Steer(warningMessage(warns))
	}

	// A truncated batch never reaches the BeforeTool gate (loop.go skips it),
	// so an escalation cannot be deferred to the gate: end the run here.
	if escalated && resp.StopReason == llm.StopMaxTokens {
		return core.Directive{Kind: core.Stop, Reason: "loop-guard: stuck loop in truncated replies, stopping run"}, nil
	}
	return core.Directive{}, nil
}

func (g *loopGuard) BeforeTool(lc *agent.LoopContext, c *core.ToolCall) (core.Directive, error) {
	sig := signature(c.Name, c.Args, g.strip)
	act, ok := markedAction(lc, sig)
	if !ok {
		return core.Directive{}, nil
	}
	reason := "loop-guard: stuck-pattern tool call " + c.Name + " (see preceding " + WarnMarker + " warning)"
	if act == actionStop {
		return core.Directive{Kind: core.Stop, Reason: reason}, nil
	}
	if lc.IsApproved(c) {
		// LoopGuard is a first-party HITL gate. A human approval consumes this
		// one interrupt mark, but never changes a hard stop decision above.
		clearMarkedAction(lc, sig)
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: reason}, nil
}

func (g *loopGuard) observe(lc *agent.LoopContext, rule, reason string) {
	if lc.Bus != nil {
		lc.Bus.Publish(lc.Topic, core.StuckDetected{Rule: rule, Reason: reason, Step: lc.Step})
	}
	if g.opts.OnDetect != nil {
		g.opts.OnDetect(rule, reason, lc.Step)
	}
}

// detect folds both rules into per-signature hits for the pending batch.
func (g *loopGuard) detect(lc *agent.LoopContext, calls []core.ToolCall) []guardHit {
	hits := map[string]*guardHit{}
	add := func(sig, toolName, rule, reason string, escalate bool) {
		h := hits[sig]
		if h == nil {
			h = &guardHit{sig: sig, tool: toolName}
			hits[sig] = h
		}
		h.rules = append(h.rules, rule)
		if h.reason == "" {
			h.reason = reason
		}
		h.escalate = h.escalate || escalate
	}

	for _, c := range calls {
		if g.exempt[c.Name] {
			continue
		}
		sig := signature(c.Name, c.Args, g.strip)
		count, lastIdx := g.pastOccurrences(lc.History, sig)
		if count >= g.opts.RepeatThreshold-1 {
			add(sig, c.Name, "repeat_call",
				"tool "+c.Name+" called with identical arguments in "+strconv.Itoa(count+1)+" batches",
				g.warnedAfter(lc.History, lastIdx))
		}
	}

	if tool, errText, count, lastIdx, ok := g.errorStreak(lc.History); ok && count >= g.opts.ErrorStreakThreshold {
		for _, c := range calls {
			if c.Name == tool {
				sig := signature(c.Name, c.Args, g.strip)
				add(sig, tool, "error_streak",
					"tool "+tool+" failed "+strconv.Itoa(count)+" times in a row ("+errText+")",
					g.warnedAfter(lc.History, lastIdx))
			}
		}
	}

	out := make([]guardHit, 0, len(hits))
	// Deterministic order for events/warnings: sort by sig.
	sigs := make([]string, 0, len(hits))
	for k := range hits {
		sigs = append(sigs, k)
	}
	sort.Strings(sigs)
	for _, k := range sigs {
		out = append(out, *hits[k])
	}
	return out
}

// pastOccurrences counts batches (assistant messages with tool calls) among
// the last Window that contain sig, and reports the message index of the most
// recent one.
func (g *loopGuard) pastOccurrences(history []core.Message, sig string) (count, lastIdx int) {
	var batches []struct {
		idx  int
		sigs map[string]bool
	}
	for i, m := range history {
		if m.Role != core.RoleAssistant {
			continue
		}
		mc := m.ToolCalls()
		if len(mc) == 0 {
			continue
		}
		sset := make(map[string]bool, len(mc))
		for _, c := range mc {
			if !g.exempt[c.Name] {
				sset[signature(c.Name, c.Args, g.strip)] = true
			}
		}
		if len(sset) == 0 {
			continue
		}
		batches = append(batches, struct {
			idx  int
			sigs map[string]bool
		}{i, sset})
	}
	start := 0
	if len(batches) > g.opts.Window {
		start = len(batches) - g.opts.Window
	}
	for _, b := range batches[start:] {
		if b.sigs[sig] {
			count++
			lastIdx = b.idx
		}
	}
	return count, lastIdx
}

// errorStreak scans history backwards for the longest run of consecutive
// identical error results. Exempt tools neither extend nor break the streak.
// Returns the tool name, its error's first line, the streak length, and the
// message index of the newest error.
func (g *loopGuard) errorStreak(history []core.Message) (tool, errText string, count, lastIdx int, ok bool) {
	var want string
	for i := len(history) - 1; i >= 0; i-- {
		m := history[i]
		if m.Role != core.RoleTool {
			continue
		}
		for j := len(m.Parts) - 1; j >= 0; j-- {
			tr, isTR := m.Parts[j].(core.ToolResult)
			if !isTR || g.exempt[tr.Name] {
				continue
			}
			if !tr.IsError {
				return tool, errText, count, lastIdx, ok
			}
			key := tr.Name + "\x00" + firstLine(resultText(tr))
			switch {
			case want == "":
				want, tool, errText, count, lastIdx, ok = key, tr.Name, firstLine(resultText(tr)), 1, i, true
			case key == want:
				count++
			default:
				return tool, errText, count, lastIdx, ok
			}
		}
	}
	return tool, errText, count, lastIdx, ok
}

// warnedAfter reports whether a guard warning appears after message idx.
func (g *loopGuard) warnedAfter(history []core.Message, idx int) bool {
	if idx < 0 {
		return false
	}
	start := min(idx+1, len(history))
	for _, m := range history[start:] {
		if m.Role == core.RoleUser && strings.Contains(m.Text(), WarnMarker) {
			return true
		}
	}
	return false
}

// --- signature canonicalization ---------------------------------------------

// signature hashes a tool name with its canonicalized arguments. Argument JSON
// is re-marshaled so key order is stable; top-level keys in strip are removed
// (volatile fields like request ids). Malformed JSON signs as its raw bytes.
func signature(name string, args json.RawMessage, strip map[string]bool) string {
	h := sha256.New()
	h.Write([]byte(name))
	h.Write([]byte{0})
	h.Write([]byte(canonArgs(args, strip)))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func canonArgs(raw json.RawMessage, strip map[string]bool) string {
	if len(raw) == 0 {
		return ""
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return strings.TrimSpace(string(raw)) // tolerate malformed args: raw bytes are the signature
	}
	if m, ok := v.(map[string]any); ok && len(strip) > 0 {
		out := make(map[string]any, len(m))
		for k, val := range m {
			if !strip[k] {
				out[k] = val
			}
		}
		v = out
	}
	b, err := json.Marshal(v)
	if err != nil {
		return strings.TrimSpace(string(raw))
	}
	return string(b)
}

// --- State.KV helpers ---------------------------------------------------------

func interventions(lc *agent.LoopContext) int {
	if lc.State == nil {
		return 0
	}
	switch v := lc.State.KV[kvLoopInterventions].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64: // JSON round-trip through the file checkpointer
		return int(v)
	}
	return 0
}

// markedAction reads this step's escalation mark for sig. The map is written
// by AfterModel on the same *State, so live runs see map[string]string and a
// resumed run (after JSON round-trip) sees map[string]any.
func markedAction(lc *agent.LoopContext, sig string) (string, bool) {
	act, ok := loopActions(lc)[sig]
	return act, ok
}

func clearMarkedAction(lc *agent.LoopContext, sig string) {
	if lc.State == nil {
		return
	}
	actions := loopActions(lc)
	delete(actions, sig)
	lc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: kvLoopActions, Value: actions})
}

func loopActions(lc *agent.LoopContext) map[string]string {
	actions := map[string]string{}
	if lc.State == nil {
		return actions
	}
	switch m := lc.State.KV[kvLoopActions].(type) {
	case map[string]string:
		for sig, act := range m {
			actions[sig] = act
		}
	case map[string]any:
		for sig, raw := range m {
			if act, ok := raw.(string); ok {
				actions[sig] = act
			}
		}
	}
	return actions
}

// --- message/text helpers -----------------------------------------------------

func warningMessage(reasons []string) core.Message {
	return core.UserText(WarnMarker + " Stuck-pattern detected: " + strings.Join(reasons, "; ") +
		". Do not repeat identical tool calls — change your approach (different arguments, another tool, " +
		"or gather the missing information) or reply with your final answer without tool calls.")
}

func resultText(tr core.ToolResult) string {
	var b strings.Builder
	for _, p := range tr.Content {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

var _ agent.Middleware = (*loopGuard)(nil)
