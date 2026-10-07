package middleware

import (
	"context"
	"strings"
	"unicode"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// TokenCounter estimates the token cost of a message slice. Inject your own
// (a real BPE tokenizer, a provider count-tokens call) to override the built-in
// rune-aware estimate; the calibration multiplier is applied on top of whatever
// counter you give, correcting its systematic bias against reported Usage.
type TokenCounter func(msgs []core.Message) int

// CompactionOptions configures Compaction.
type CompactionOptions struct {
	// Model summarizes the old messages.
	Model llm.Model
	// MaxTokens is the estimated-token threshold above which compaction runs
	// (default 8000). Both modes measure the message history only; the system
	// prompt and tool schemas are absorbed by the Usage calibration.
	MaxTokens int
	// KeepRecent is how many of the most recent messages to keep verbatim
	// (default 6); everything older is replaced by one summary.
	KeepRecent int
	// Counter estimates tokens for a message slice. nil uses the built-in
	// rune-aware estimator (see estimateTokens).
	Counter TokenCounter
	// Persist rewrites the conversation history itself (via the loop's
	// HistoryCompacter capability) instead of only reshaping each request, so a
	// summarized prefix is dropped once, stays dropped, and shrinks the
	// checkpointed state. When true, ModifyRequest leaves the request untouched.
	Persist bool
}

// Compaction keeps the context window bounded: when the message history exceeds
// MaxTokens it summarizes all but the KeepRecent latest messages into a single
// note. Two modes:
//
//   - request mode (default): rewrites only the outgoing request in
//     ModifyRequest; stored State is untouched (the historical behaviour).
//   - persist mode (Persist): rewrites the working history itself in
//     CompactHistory, so the summary is durable — old turns are summarized once
//     rather than re-summarized every step, and the checkpointed
//     State.Messages shrinks.
//
// Both modes scale their threshold check by a rolling calibration factor
// learned from each response's real Usage.InputTokens versus the estimate
// (stored in State.KV, so it survives resume), which corrects the estimator and
// folds in the system-prompt/tool-schema overhead. On summarization failure it
// leaves everything unchanged (best effort).
func Compaction(o CompactionOptions) agent.Middleware {
	if o.MaxTokens <= 0 {
		o.MaxTokens = 8000
	}
	if o.KeepRecent <= 0 {
		o.KeepRecent = 6
	}
	counter := o.Counter
	if counter == nil {
		counter = estimateTokens
	}
	return &compaction{
		model:     o.Model,
		maxTokens: o.MaxTokens,
		keep:      o.KeepRecent,
		persist:   o.Persist,
		counter:   counter,
	}
}

type compaction struct {
	agent.BaseMiddleware
	model     llm.Model
	maxTokens int
	keep      int
	persist   bool
	counter   TokenCounter
}

const (
	kvCompaction = "_compaction"
	summaryMark  = "[earlier conversation summary] "
)

// ModifyRequest performs the request-mode rewrite; in persist mode the durable
// rewrite happens in CompactHistory instead, so the request is left untouched.
func (c *compaction) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	if c.persist {
		return nil
	}
	if out, _, ok := c.compact(lc, req.Messages); ok {
		req.Messages = out
	}
	return nil
}

// CompactHistory is the HistoryCompacter entry point: in persist mode it
// replaces the working history with the summarized form, which the loop then
// checkpoints. A no-op (returns history unchanged) in request mode.
func (c *compaction) CompactHistory(lc *agent.LoopContext, history []core.Message) []core.Message {
	if !c.persist {
		return history
	}
	est := c.calibratedEstimate(lc, history)
	out, cut, ok := c.compact(lc, history)
	if !ok {
		return history
	}
	if lc.Bus != nil {
		lc.Bus.Publish(lc.Topic, core.HistoryCompacted{Dropped: cut, Kept: len(out) - 1, EstTokens: est, Step: lc.Step})
	}
	return out
}

// compact returns the summarized history and the cut index, or ok=false when no
// compaction is warranted (too short, under threshold, unsafe cut, or summary
// failure).
func (c *compaction) compact(lc *agent.LoopContext, msgs []core.Message) ([]core.Message, int, bool) {
	if c.model == nil || len(msgs) <= c.keep+1 {
		return nil, 0, false
	}
	if c.calibratedEstimate(lc, msgs) <= c.maxTokens {
		return nil, 0, false
	}
	cut := safeCut(msgs, c.keep)
	if cut <= 0 {
		return nil, 0, false
	}
	summary, err := c.summarize(lc.Context, msgs[:cut])
	if err != nil || summary == "" {
		return nil, 0, false // best effort: proceed uncompacted
	}
	note := core.Message{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: summaryMark + summary}}}
	return append([]core.Message{note}, msgs[cut:]...), cut, true
}

// safeCut backs the boundary off from len-msgs-keep until it lands on a user or
// assistant message, so the kept segment never starts with a tool result whose
// originating assistant message was dropped (a provider-illegal orphan). Returns
// 0 when no such boundary exists above the keep window.
func safeCut(msgs []core.Message, keep int) int {
	cut := len(msgs) - keep
	for cut > 0 && msgs[cut].Role != core.RoleUser && msgs[cut].Role != core.RoleAssistant {
		cut--
	}
	return cut
}

// AfterModel calibrates the estimator against the provider's real input-token
// count for the request that was just sent (lc.Request carries the exact
// messages, post any request-mode rewrite). The factor is stored per-run in
// State.KV so it survives HITL pauses and resumes.
func (c *compaction) AfterModel(lc *agent.LoopContext, resp *llm.Response) (core.Directive, error) {
	if lc.State == nil || resp.Usage == nil || resp.Usage.InputTokens <= 0 || lc.Request == nil {
		return core.Directive{}, nil
	}
	est := c.counter(lc.Request.Messages)
	if est <= 0 {
		return core.Directive{}, nil
	}
	ratio := float64(resp.Usage.InputTokens) / float64(est)
	ratio = clamp(ratio, 0.5, 2.0)
	if prev, ok := loadCalib(lc); ok {
		ratio = 0.7*prev + 0.3*ratio
	}
	saveCalib(lc, ratio)
	return core.Directive{}, nil
}

func (c *compaction) calibratedEstimate(lc *agent.LoopContext, msgs []core.Message) int {
	return int(float64(c.counter(msgs)) * loadCalibDefault(lc))
}

func (c *compaction) summarize(ctx context.Context, msgs []core.Message) (string, error) {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(string(m.Role))
		b.WriteString(": ")
		b.WriteString(m.Text())
		b.WriteByte('\n')
	}
	req := &llm.Request{
		System:   "Summarize the following conversation concisely, preserving key facts, decisions and open questions.",
		Messages: []core.Message{core.UserText(b.String())},
	}
	var out string
	for resp, err := range c.model.Generate(ctx, req) {
		if err != nil {
			return "", err
		}
		if !resp.Partial {
			out = resp.Message.Text()
		}
	}
	return out, nil
}

// --- calibration state --------------------------------------------------------

// The *At variants take the State.KV key explicitly so more than one middleware
// can keep its own factor: saveCalibAt overwrites the whole value at its key, so
// sharing a key would silently erase the other writer. loadCalibAt reads the
// same shape written by saveCalibAt.

func loadCalibAt(lc *agent.LoopContext, key string) (float64, bool) {
	if lc.State == nil {
		return 0, false
	}
	m, ok := lc.State.KV[key].(map[string]any)
	if !ok {
		return 0, false
	}
	f, ok := m["calib"].(float64)
	if !ok || f <= 0 {
		return 0, false
	}
	return f, true
}

func loadCalibDefaultAt(lc *agent.LoopContext, key string) float64 {
	if f, ok := loadCalibAt(lc, key); ok {
		return f
	}
	return 1.0
}

func saveCalibAt(lc *agent.LoopContext, key string, v float64) {
	lc.State.Apply(core.StateOp{Kind: core.OpSetKV, Key: key, Value: map[string]any{"calib": v}})
}

func loadCalib(lc *agent.LoopContext) (float64, bool) { return loadCalibAt(lc, kvCompaction) }

func loadCalibDefault(lc *agent.LoopContext) float64 { return loadCalibDefaultAt(lc, kvCompaction) }

func saveCalib(lc *agent.LoopContext, v float64) { saveCalibAt(lc, kvCompaction, v) }

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// --- built-in estimator -------------------------------------------------------

// estimateTokens is a rune-aware heuristic: wide (CJK) characters count as ~1
// token each, other characters at ~4 per token, plus per-part and per-message
// framing overhead. It is deliberately conservative and bias-correctable via the
// Usage calibration; unlike the old byte/4 rule it does not under-count Chinese.
func estimateTokens(msgs []core.Message) int {
	n := 0
	for _, m := range msgs {
		n += 4 // role/framing overhead per message
		for _, p := range m.Parts {
			switch v := p.(type) {
			case core.Text:
				n += textTokens(v.Text)
			case core.Thinking:
				n += textTokens(v.Text)
			case core.ToolCall:
				n += textTokens(v.Name) + len(v.Args)/4 + 4
			case core.ToolResult:
				n += 4
				for _, pp := range v.Content {
					if t, ok := pp.(core.Text); ok {
						n += textTokens(t.Text)
					}
				}
			}
		}
	}
	return n
}

func textTokens(s string) int {
	if s == "" {
		return 0
	}
	var wide, total int
	for _, r := range s {
		total++
		if isWideRune(r) {
			wide++
		}
	}
	other := total - wide
	return wide + (other+3)/4
}

func isWideRune(r rune) bool {
	return unicode.Is(unicode.Han, r) ||
		(r >= 0x3040 && r <= 0x30ff) || // hiragana + katakana
		(r >= 0x3130 && r <= 0x318f) || // hangul jamo
		(r >= 0xAC00 && r <= 0xD7AF) || // hangul syllables
		(r >= 0x3000 && r <= 0x303f) // CJK punctuation
}

var _ agent.Middleware = (*compaction)(nil)
var _ agent.HistoryCompacter = (*compaction)(nil)
