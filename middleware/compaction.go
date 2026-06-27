package middleware

import (
	"context"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// CompactionOptions configures Compaction.
type CompactionOptions struct {
	// Model summarizes the old messages.
	Model llm.Model
	// MaxTokens is the estimated-token threshold above which compaction runs
	// (default 8000). Estimation is a rough len/4 heuristic.
	MaxTokens int
	// KeepRecent is how many of the most recent messages to keep verbatim
	// (default 6); everything older is replaced by one summary.
	KeepRecent int
}

// Compaction keeps the context window bounded: when the request's message
// history exceeds MaxTokens, it summarizes all but the KeepRecent latest
// messages into a single note and rewrites the request. Implemented as
// ModifyRequest, so it only reshapes the call — it does not touch stored State.
// On summarization failure it leaves the request unchanged (best effort).
func Compaction(o CompactionOptions) agent.Middleware {
	if o.MaxTokens <= 0 {
		o.MaxTokens = 8000
	}
	if o.KeepRecent <= 0 {
		o.KeepRecent = 6
	}
	return &compaction{model: o.Model, maxTokens: o.MaxTokens, keep: o.KeepRecent}
}

type compaction struct {
	agent.BaseMiddleware
	model     llm.Model
	maxTokens int
	keep      int
}

func (c *compaction) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	if c.model == nil || len(req.Messages) <= c.keep+1 || estimateTokens(req.Messages) <= c.maxTokens {
		return nil
	}
	cut := len(req.Messages) - c.keep
	summary, err := c.summarize(lc.Context, req.Messages[:cut])
	if err != nil || summary == "" {
		return nil // best effort: proceed uncompacted
	}
	note := core.Message{Role: core.RoleUser, Parts: []core.Part{
		core.Text{Text: "[earlier conversation summary] " + summary},
	}}
	req.Messages = append([]core.Message{note}, req.Messages[cut:]...)
	return nil
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

func estimateTokens(msgs []core.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Text()) / 4
	}
	return n
}
