// Package memx assembles the layered memory system (ADR 0016) into ready-to-mount
// pieces and provides the short-term -> long-term consolidation bridge. It wires
// rules, project memory, working memory, text memory, and semantic RAG into the
// prompt Sections, model Middleware, and Tools an agent mounts — so callers
// compose one facade instead of five subsystems.
package memx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/memory/textmem"
	"github.com/jiujuan/goagent/session"
)

// consolidatePrompt instructs the model to distill durable facts from a
// transcript and route each to text (curated, readable) or semantic (bulk,
// fuzzy-recalled) memory. The model must answer with a JSON array only.
const consolidatePrompt = `你是记忆固化器。阅读下面的对话记录，抽取值得长期保留的事实。
对每条事实判断去向：
- "text"：用户明确的偏好、项目约定、被纠正的反馈等可读、精确的事实。
- "semantic"：琐碎、海量、供模糊召回的背景知识。
只输出一个 JSON 数组，每个元素形如：
{"target":"text|semantic","name":"短名(text 必填)","desc":"一行摘要(text 必填)","type":"user|feedback|project|reference","content":"事实正文"}
没有值得记住的内容时输出 []。不要输出 JSON 以外的任何文字。`

// item is one extracted memory the model proposes.
type item struct {
	Target  string `json:"target"`
	Name    string `json:"name"`
	Desc    string `json:"desc"`
	Type    string `json:"type"`
	Content string `json:"content"`
}

// Consolidate reads the session transcript, asks the model to extract durable
// facts, and writes them to the text and/or semantic stores (either may be nil
// to skip that target). Facts are de-duplicated by content hash before writing.
// Call it at session end (e.g. after Runner.Run returns). See ADR 0019.
func Consolidate(ctx context.Context, model llm.Model, s *session.Session, text textmem.Store, sem memory.Store) error {
	transcript := renderTranscript(s.Messages())
	if strings.TrimSpace(transcript) == "" {
		return nil
	}

	req := &llm.Request{
		System:   consolidatePrompt,
		Messages: []core.Message{core.UserText(transcript)},
	}
	out, err := generate(ctx, model, req)
	if err != nil {
		return fmt.Errorf("memx: consolidate generate: %w", err)
	}

	items, err := parseItems(out)
	if err != nil {
		return fmt.Errorf("memx: parse consolidation output: %w", err)
	}

	seen := map[string]bool{}
	for _, it := range items {
		content := strings.TrimSpace(it.Content)
		if content == "" {
			continue
		}
		h := contentHash(content)
		if seen[h] {
			continue // intra-batch dedup
		}
		seen[h] = true

		switch it.Target {
		case "text":
			if text == nil || it.Name == "" {
				continue
			}
			if err := text.Save(ctx, textmem.Entry{Name: it.Name, Desc: it.Desc, Type: it.Type, Body: content}); err != nil {
				return fmt.Errorf("memx: save text memory: %w", err)
			}
		case "semantic":
			if sem == nil {
				continue
			}
			if err := sem.Add(ctx, memory.Doc(content)); err != nil {
				return fmt.Errorf("memx: add semantic memory: %w", err)
			}
		}
	}
	return nil
}

// renderTranscript flattens messages into a "role: text" transcript.
func renderTranscript(msgs []core.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		t := strings.TrimSpace(m.Text())
		if t == "" {
			continue
		}
		fmt.Fprintf(&b, "%s: %s\n", m.Role, t)
	}
	return b.String()
}

// generate runs one model call and returns the final (non-partial) assistant
// text.
func generate(ctx context.Context, model llm.Model, req *llm.Request) (string, error) {
	var last string
	for resp, err := range model.Generate(ctx, req) {
		if err != nil {
			return "", err
		}
		if resp == nil || resp.Partial {
			continue
		}
		last = resp.Message.Text()
	}
	return last, nil
}

// parseItems extracts the JSON array from the model output, tolerating
// surrounding prose or code fences by slicing to the outermost brackets.
func parseItems(out string) ([]item, error) {
	out = strings.TrimSpace(out)
	if out == "" {
		return nil, nil
	}
	start := strings.IndexByte(out, '[')
	end := strings.LastIndexByte(out, ']')
	if start < 0 || end < start {
		return nil, nil // no array -> nothing to write
	}
	var items []item
	if err := json.Unmarshal([]byte(out[start:end+1]), &items); err != nil {
		return nil, err
	}
	return items, nil
}

// contentHash returns a stable hash of normalized content for dedup.
func contentHash(s string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.Join(strings.Fields(s), " "))))
	return hex.EncodeToString(sum[:8])
}
