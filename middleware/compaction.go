package middleware

import (
	"context"
	"strings"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// compactor summarizes older conversation history once the estimated context
// size exceeds a threshold, replacing the summarized prefix with a single
// summary message while keeping recent messages intact. This lets long-running
// agents stay within a model's context window. The cut point never strands a
// tool result from its originating tool call.
type compactor struct {
	model     llm.Model
	maxTokens int
	keepTok   int
	estimate  func(core.Message) int
}

// CompactionOptions configures Compaction.
type CompactionOptions struct {
	// MaxTokens triggers compaction when the estimated history size exceeds it.
	MaxTokens int
	// KeepRecentTokens is roughly how many tokens of recent history to retain
	// uncompacted.
	KeepRecentTokens int
	// Estimator estimates the token cost of a message (default: chars/4).
	Estimator func(core.Message) int
}

// Default thresholds, chosen to be conservative; tune per model.
const (
	defaultMaxTokens  = 8000
	defaultKeepTokens = 2000
)

// Compaction builds a context-compaction Middleware. summarizer generates the
// summary (it should be cheap and not require tools). This variant compacts
// each request ephemerally; for a persistent, tree-node summary see Resummarize.
func Compaction(summarizer llm.Model, opts *CompactionOptions) Middleware {
	return BeforeModel(newCompactor(summarizer, opts).apply)
}

// newCompactor builds a compactor from options, applying defaults. Shared by the
// Compaction middleware and the Resummarize driver.
func newCompactor(summarizer llm.Model, opts *CompactionOptions) *compactor {
	c := &compactor{
		model:     summarizer,
		maxTokens: defaultMaxTokens,
		keepTok:   defaultKeepTokens,
		estimate:  estimateMessage,
	}
	if opts != nil {
		if opts.MaxTokens > 0 {
			c.maxTokens = opts.MaxTokens
		}
		if opts.KeepRecentTokens > 0 {
			c.keepTok = opts.KeepRecentTokens
		}
		if opts.Estimator != nil {
			c.estimate = opts.Estimator
		}
	}
	// Ensure there is room to keep recent history below the trigger.
	if c.keepTok >= c.maxTokens {
		c.keepTok = c.maxTokens / 2
	}
	return c
}

// apply rewrites req in place, compacting old history when it grows too large.
func (c *compactor) apply(ctx context.Context, req *llm.Request) error {
	total := 0
	for _, m := range req.Messages {
		total += c.estimate(m)
	}
	if total <= c.maxTokens {
		return nil
	}

	cut := c.findCut(req.Messages)
	if cut <= 0 {
		return nil // nothing safely summarizable
	}

	older := req.Messages[:cut]
	recent := req.Messages[cut:]

	summary, err := c.summarize(ctx, older)
	if err != nil {
		return err
	}

	summaryMsg := core.Message{
		Role:  core.RoleUser,
		Parts: []core.Part{core.Text{Text: summaryPrefix + summary + summarySuffix}},
	}
	compacted := make([]core.Message, 0, len(recent)+1)
	compacted = append(compacted, summaryMsg)
	compacted = append(compacted, recent...)
	req.Messages = compacted
	return nil
}

// findCut returns the index splitting older (to summarize) from recent (to
// keep). It walks backward accumulating tokens until KeepRecentTokens, then
// backs up so a tool-result message is never separated from its tool call.
func (c *compactor) findCut(msgs []core.Message) int {
	acc := 0
	cut := 0
	for i := len(msgs) - 1; i >= 0; i-- {
		acc += c.estimate(msgs[i])
		if acc >= c.keepTok {
			cut = i
			break
		}
	}
	for cut > 0 && msgs[cut].Role == core.RoleTool {
		cut--
	}
	return cut
}

func (c *compactor) summarize(ctx context.Context, older []core.Message) (string, error) {
	req := &llm.Request{
		System:   summarizationSystem,
		Messages: []core.Message{core.UserText(renderConversation(older))},
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

// --- helpers ----------------------------------------------------------------

const (
	summaryPrefix = "[对话历史摘要开始]\n"
	summarySuffix = "\n[对话历史摘要结束]"

	summarizationSystem = `你是一个对话压缩器。请把下面的对话历史压缩成一段结构化摘要，覆盖：
- 目标(Goal)：用户想要达成什么
- 约束(Constraints)：已确立的限制与偏好
- 进展(Progress)：已完成的关键步骤与结论
- 关键决策(Decisions)：做过的重要选择及理由
- 下一步(Next Steps)：尚未完成、需要继续的事项
保留后续对话所需的关键事实（如文件名、ID、数值）。只输出摘要本身。`
)

// estimateMessage approximates a message's token cost as characters / 4.
func estimateMessage(m core.Message) int {
	chars := 0
	for _, p := range m.Parts {
		switch v := p.(type) {
		case core.Text:
			chars += len(v.Text)
		case core.Thinking:
			chars += len(v.Text)
		case core.ToolCall:
			chars += len(v.Name) + len(v.Args)
		case core.ToolResult:
			for _, cp := range v.Content {
				if t, ok := cp.(core.Text); ok {
					chars += len(t.Text)
				}
			}
		}
	}
	return chars/4 + 1
}

// renderConversation flattens messages into a plain-text transcript for the
// summarizer.
func renderConversation(msgs []core.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(string(m.Role))
		b.WriteString(": ")
		switch m.Role {
		case core.RoleTool:
			for _, p := range m.Parts {
				if tr, ok := p.(core.ToolResult); ok {
					b.WriteString("[tool ")
					b.WriteString(tr.Name)
					b.WriteString("] ")
					for _, cp := range tr.Content {
						if t, ok := cp.(core.Text); ok {
							b.WriteString(t.Text)
						}
					}
				}
			}
		default:
			b.WriteString(m.Text())
			for _, c := range m.ToolCalls() {
				b.WriteString(" [call ")
				b.WriteString(c.Name)
				b.WriteString("(")
				b.Write(c.Args)
				b.WriteString(")]")
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}
