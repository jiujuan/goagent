package middleware

import (
	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Context-window eviction. Window bounds one request's message history by
// *evicting* whole message units, as opposed to Compaction, which *summarizes*
// them: eviction costs no model call and gives a hard cap, but loses content.
// The built-in policies are RecentN and SlidingWindow (see WindowStrategy for
// the measure each one uses, and for ImportanceWeighted's scoring).
//
// The whole point of this file is that it drops *units*, never single messages:
// an assistant tool call and the tool results that answer it travel together, or
// the request reaches a provider with an orphan result it cannot match. The unit
// rules and the pairing repair below implement ADR-0031 §3.

// unit is one message or message group that eviction decides about as a whole.
type unit struct {
	start int // first message index, inclusive
	end   int // last message index, exclusive
}

// groupUnits splits msgs into eviction units. An assistant message carrying tool
// calls binds with the tool messages that immediately follow it and report those
// calls; everything else is a unit of its own.
//
// Two facts make this both safe and necessary. Providers copy ToolResult.CallID
// straight into the wire format without checking that the call is present
// (llm/openaicompat/openaicompat.go:178-187, llm/anthropic/anthropic.go:173-185),
// so a result whose call was evicted is a rejected request. And the loop keeps
// the pair adjacent: the assistant message is appended at agent/loop.go:203 and
// its results as one tool message right after it at :256, with the max_tokens
// path at :222 keeping the same shape.
func groupUnits(msgs []core.Message) []unit {
	units := make([]unit, 0, len(msgs))
	for i := 0; i < len(msgs); {
		end := i + 1
		if ids := callIDs(msgs[i]); len(ids) > 0 {
			for j := i + 1; j < len(msgs) && reportsAny(msgs[j], ids); j++ {
				end = j + 1
			}
		}
		units = append(units, unit{start: i, end: end})
		i = end
	}
	return units
}

// callIDs returns the set of tool-call ids m carries, nil for anything but an
// assistant message with calls.
func callIDs(m core.Message) map[string]bool {
	if m.Role != core.RoleAssistant {
		return nil
	}
	calls := m.ToolCalls()
	if len(calls) == 0 {
		return nil
	}
	ids := make(map[string]bool, len(calls))
	for _, c := range calls {
		ids[c.ID] = true
	}
	return ids
}

// reportsAny is true for a tool message carrying at least one result of ids. A
// tool message reporting none of them ends the unit rather than being absorbed:
// it answers a call from further back or one that is gone, and repairPairing
// decides its fate.
func reportsAny(m core.Message, ids map[string]bool) bool {
	if m.Role != core.RoleTool {
		return false
	}
	for _, p := range m.Parts {
		if tr, ok := p.(core.ToolResult); ok && ids[tr.CallID] {
			return true
		}
	}
	return false
}

// keptMessages flattens the chosen units back into a message slice in the
// original order. The fresh slice is deliberate: a window over the input's
// backing array would let a later append overwrite evicted messages.
func keptMessages(msgs []core.Message, keep []unit) []core.Message {
	n := 0
	for _, u := range keep {
		n += u.end - u.start
	}
	out := make([]core.Message, 0, n)
	for _, u := range keep {
		out = append(out, msgs[u.start:u.end]...)
	}
	return out
}

// repairPairing removes tool results whose originating call is not among msgs.
// It is a guard for histories this package did not build — the unit rules
// already keep calls and results together — and it runs once after a strategy
// has chosen what survives.
//
// It deliberately does not synthesize a result for a call that has none. A
// pending human approval checkpoints exactly that shape: agent/loop.go:236-244
// persists the assistant tool-call message together with the still-pending
// calls, and Resume appends the real results afterwards (agent/loop.go:119-128,
// agent/hitl.go:116). A fabricated "result dropped" note would tell the resumed
// run that a call was already answered.
func repairPairing(msgs []core.Message) []core.Message {
	calls := map[string]bool{}
	for _, m := range msgs {
		for _, c := range m.ToolCalls() {
			calls[c.ID] = true
		}
	}
	out := make([]core.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Role != core.RoleTool {
			out = append(out, m)
			continue
		}
		parts := make([]core.Part, 0, len(m.Parts))
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok && !calls[tr.CallID] {
				continue
			}
			parts = append(parts, p)
		}
		if len(parts) == 0 {
			continue
		}
		m.Parts = parts // m is a copy; the input message keeps its own parts
		out = append(out, m)
	}
	return out
}

// --- the middleware -----------------------------------------------------------

// kvWindow holds this middleware's token-calibration factor. It is deliberately
// not kvCompaction: saveCalibAt overwrites the whole value at its key, so two
// writers sharing one key would silently erase each other's factor.
const kvWindow = "_window"

// WindowOptions configures Window.
type WindowOptions struct {
	// Strategy decides what survives the window. Without it Window is a no-op
	// (same posture as RAG with a nil Retriever).
	Strategy WindowStrategy
	// Counter estimates tokens for a message slice. nil uses the built-in
	// rune-aware estimator (estimateTokens). Ignored by RecentN, which measures
	// units, not tokens.
	Counter TokenCounter
	// Persist has the same meaning as in CompactionOptions: rewrite the working
	// history itself via the loop's HistoryCompacter capability, so the eviction
	// is durable and shrinks the checkpointed state, instead of reshaping only
	// the outgoing request every step. When true, ModifyRequest leaves the
	// request untouched.
	Persist bool
}

// Window bounds the message history of one model call by evicting whole message
// units. Two mount points, two modes, mutually exclusive like Compaction's:
//
//   - request mode (default): rewrites only the outgoing request in
//     ModifyRequest; stored State keeps the full history, and the decision is
//     recomputed each step.
//   - persist mode (Persist): rewrites the working history itself in
//     CompactHistory, so what was evicted stays evicted and the checkpoint
//     shrinks.
//
// Both modes scale their token measure by a rolling calibration factor learned
// from each response's real Usage.InputTokens versus the estimate (kept in
// State.KV under _window, so it survives resume), which corrects the estimator
// and folds in the system-prompt and tool-schema overhead.
//
// Eviction is lossy in a way summarization is not: what leaves the window is
// gone from what the model sees. The full history remains recoverable from
// earlier checkpoints (a File checkpointer appends one whole snapshot per step)
// and from working memory if it was written there, but neither is a path the
// model can consult by itself.
func Window(o WindowOptions) agent.Middleware {
	counter := o.Counter
	if counter == nil {
		counter = estimateTokens
	}
	return &window{strategy: o.Strategy, counter: counter, persist: o.Persist}
}

type window struct {
	agent.BaseMiddleware
	strategy WindowStrategy
	counter  TokenCounter
	persist  bool
}

// ModifyRequest performs the request-mode rewrite; in persist mode the durable
// rewrite happens in CompactHistory instead, so the request is left untouched.
func (w *window) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	if w.persist || w.strategy == nil {
		return nil
	}
	in := req.Messages
	out, est, trimmed := w.apply(lc, in)
	if !trimmed {
		return nil
	}
	req.Messages = out
	w.publishTrimmed(lc, in, out, est)
	return nil
}

// CompactHistory is the HistoryCompacter entry point: in persist mode it replaces
// the working history with the evicted form, which the loop then checkpoints. A
// no-op (history unchanged) in request mode.
func (w *window) CompactHistory(lc *agent.LoopContext, history []core.Message) []core.Message {
	if !w.persist || w.strategy == nil {
		return history
	}
	out, est, trimmed := w.apply(lc, history)
	if !trimmed {
		return history
	}
	w.publishTrimmed(lc, history, out, est)
	return out
}

// apply runs the three steps of one eviction decision: split into units, ask the
// strategy which units survive, then repair pairing. est is the calibrated token
// estimate of the input before eviction. trimmed is false when the strategy took
// nothing out, in which case the input slice is returned unchanged — and pairing
// repair is skipped too, so a pre-existing orphan in the stored history is never
// silently rewritten by a step that decided to evict nothing.
func (w *window) apply(lc *agent.LoopContext, msgs []core.Message) (out []core.Message, est int, trimmed bool) {
	factor := loadCalibDefaultAt(lc, kvWindow)
	count := func(m []core.Message) int { return int(float64(w.counter(m)) * factor) }
	est = count(msgs)
	kept := w.strategy.Select(lc, count, msgs)
	if len(kept) >= len(msgs) {
		return msgs, est, false
	}
	return repairPairing(kept), est, true
}

// publishTrimmed emits core.WindowTrimmed for one eviction that actually
// happened. Dropped/Kept count messages (not units), measured against the input
// of this step and after the pairing repair.
func (w *window) publishTrimmed(lc *agent.LoopContext, in, out []core.Message, est int) {
	if lc.Bus == nil {
		return
	}
	lc.Bus.Publish(lc.Topic, core.WindowTrimmed{
		Strategy:  w.strategy.Name(),
		Dropped:   len(in) - len(out),
		Kept:      len(out),
		EstTokens: est,
		Step:      lc.Step,
	})
}

// AfterModel calibrates the estimator against the provider's real input-token
// count, same algorithm as Compaction's (see compaction.go AfterModel). Two
// facts about the plumbing are worth naming here because they make this
// measurement coarser than it looks: lc.Request is the same pointer the request
// middleware rewrote, and Stack.AfterModel runs in reverse registration order,
// so when several window/compaction middleware are installed each one calibrates
// against the FINAL message set that was sent — not against what it produced.
// That is the right target (the estimator must explain the real bytes on the
// wire), but it is not a per-middleware measurement.
func (w *window) AfterModel(lc *agent.LoopContext, resp *llm.Response) (core.Directive, error) {
	if lc.State == nil || resp.Usage == nil || resp.Usage.InputTokens <= 0 || lc.Request == nil || w.strategy == nil {
		return core.Directive{}, nil
	}
	est := w.counter(lc.Request.Messages)
	if est <= 0 {
		return core.Directive{}, nil
	}
	ratio := clamp(float64(resp.Usage.InputTokens)/float64(est), 0.5, 2.0)
	if prev, ok := loadCalibAt(lc, kvWindow); ok {
		ratio = 0.7*prev + 0.3*ratio
	}
	saveCalibAt(lc, kvWindow, ratio)
	return core.Directive{}, nil
}

// --- strategies ---------------------------------------------------------------

// WindowStrategy decides which part of a message history survives the window.
// The three built-in policies differ exactly as follows (ADR-0031 §1); the names
// overlap in the wider world — "sliding window" elsewhere often means "the last
// N messages", which is not what SlidingWindow here does:
//
//   - RecentN: keeps the last N units. Measures units, not tokens. Never calls
//     count. Cheapest, lossiest.
//   - SlidingWindow: keeps a contiguous tail of units whose calibrated tokens fit
//     the budget. Measures tokens; gives a predictable request size.
//   - ImportanceWeighted: keeps a scored, non-contiguous selection within the
//     budget. Measures tokens and scores content.
//
// A strategy may only drop whole units. The middleware flattens the selection,
// repairs pairing, and skips the step when nothing was taken out.
type WindowStrategy interface {
	// Select returns the messages to keep, in their original relative order.
	// Returning msgs (or a slice of the same length) means "evict nothing this
	// step". count is the run's calibrated token measure — never nil, and the
	// only way to read the budget in tokens.
	Select(lc *agent.LoopContext, count TokenCounter, msgs []core.Message) []core.Message
	// Name identifies the policy in core.WindowTrimmed.
	Name() string
}

// RecentN keeps the last keep message units verbatim and drops everything older.
// The measure is units, so "one assistant tool call plus its results" counts as
// one kept item — keeping N messages by count would cut a call off from its
// results. No token counting is involved, so Options.Counter is ignored.
func RecentN(keep int) WindowStrategy {
	if keep <= 0 {
		keep = 1
	}
	return &recentN{keep: keep}
}

type recentN struct{ keep int }

func (r *recentN) Name() string { return "recent_n" }

func (r *recentN) Select(_ *agent.LoopContext, _ TokenCounter, msgs []core.Message) []core.Message {
	units := groupUnits(msgs)
	if len(units) <= r.keep {
		return msgs
	}
	return keptMessages(msgs, units[len(units)-r.keep:])
}

// defaultWindowBudget is the SlidingWindow fallback budget. 4000 tokens is
// deliberately below Compaction's 8000 history threshold: a window asked to be
// the last gate before sending should cap tighter than the summarizer.
const defaultWindowBudget = 4000

// SlidingWindow keeps the longest contiguous run of recent message units whose
// calibrated token estimate fits budgetTokens. Unlike RecentN it measures
// tokens, so one oversized tool result cannot blow up the request size: the run
// simply starts later.
//
// The newest unit is always kept, even when it alone exceeds the budget. A window
// is allowed to send too much; it is not allowed to send nothing, and trimming
// *inside* a unit would mean rewriting a tool call or its result, which is the
// tool layer's job (see ADR-0031 §1). The overrun is visible in the event: a step
// with Dropped 0 and EstTokens above the budget was exactly that case.
func SlidingWindow(budgetTokens int) WindowStrategy {
	if budgetTokens <= 0 {
		budgetTokens = defaultWindowBudget
	}
	return &slidingWindow{budget: budgetTokens}
}

type slidingWindow struct{ budget int }

func (s *slidingWindow) Name() string { return "sliding_window" }

func (s *slidingWindow) Select(_ *agent.LoopContext, count TokenCounter, msgs []core.Message) []core.Message {
	units := groupUnits(msgs)
	used, start := 0, len(units)
	for i := len(units) - 1; i >= 0; i-- {
		size := count(msgs[units[i].start:units[i].end])
		if i == len(units)-1 {
			// The newest unit goes in unconditionally — see SlidingWindow.
			start = i
			used += size
			continue
		}
		if used+size > s.budget {
			break
		}
		start = i
		used += size
	}
	if start <= 0 {
		return msgs
	}
	return keptMessages(msgs, units[start:])
}

var _ agent.Middleware = (*window)(nil)
var _ agent.HistoryCompacter = (*window)(nil)
var _ WindowStrategy = (*recentN)(nil)
var _ WindowStrategy = (*slidingWindow)(nil)
