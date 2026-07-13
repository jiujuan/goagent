// Package anthropic implements llm.Model against the Anthropic Messages API
// (native protocol), with optional SSE streaming. It converts goagent's
// internal message model to and from Anthropic content blocks at the call
// boundary.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/internal/sse"
	"github.com/jiujuan/goagent/llm"
)

const (
	defaultBaseURL = "https://api.anthropic.com"
	apiVersion     = "2023-06-01"
	defaultMaxTok  = 4096
)

// Model is an Anthropic-backed llm.Model.
type Model struct {
	apiKey  string
	model   string
	baseURL string
	client  *http.Client
}

// Option configures a Model.
type Option func(*Model)

// WithBaseURL overrides the API base URL (e.g. for a proxy or gateway).
func WithBaseURL(u string) Option { return func(m *Model) { m.baseURL = u } }

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(c *http.Client) Option { return func(m *Model) { m.client = c } }

// New constructs an Anthropic model, e.g. New("claude-opus-4-8", apiKey).
func New(model, apiKey string, opts ...Option) *Model {
	m := &Model{
		apiKey:  apiKey,
		model:   model,
		baseURL: defaultBaseURL,
		client:  &http.Client{Timeout: 5 * time.Minute},
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

func (m *Model) Name() string { return m.model }

// Generate implements llm.Model. With Options.Stream it parses an SSE stream
// and yields incremental partial responses followed by a final one; otherwise
// it yields a single response.
func (m *Model) Generate(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		body, err := m.buildBody(req)
		if err != nil {
			yield(nil, err)
			return
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, m.baseURL+"/v1/messages", bytes.NewReader(body))
		if err != nil {
			yield(nil, err)
			return
		}
		httpReq.Header.Set("content-type", "application/json")
		httpReq.Header.Set("x-api-key", m.apiKey)
		httpReq.Header.Set("anthropic-version", apiVersion)

		resp, err := m.client.Do(httpReq)
		if err != nil {
			yield(nil, err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			yield(nil, &llm.StatusError{Provider: "anthropic", Code: resp.StatusCode, Body: string(data)})
			return
		}

		if req.Options.Stream {
			for r, err := range ParseStream(resp.Body) {
				if !yield(r, err) {
					return
				}
			}
			return
		}

		data, _ := io.ReadAll(resp.Body)
		out, err := parseResponse(data)
		if err != nil {
			yield(nil, err)
			return
		}
		yield(out, nil)
	}
}

// --- wire types -------------------------------------------------------------

type wireRequest struct {
	Model       string        `json:"model"`
	MaxTokens   int           `json:"max_tokens"`
	System      string        `json:"system,omitempty"`
	Messages    []wireMessage `json:"messages"`
	Tools       []wireTool    `json:"tools,omitempty"`
	Temperature *float64      `json:"temperature,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
}

type wireMessage struct {
	Role    string      `json:"role"`
	Content []wireBlock `json:"content"`
}

type wireBlock struct {
	Type string `json:"type"`
	// text
	Text string `json:"text,omitempty"`
	// tool_use
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	// tool_result
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   any    `json:"content,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
}

type wireTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

func (m *Model) buildBody(req *llm.Request) ([]byte, error) {
	maxTok := req.Options.MaxTokens
	if maxTok <= 0 {
		maxTok = defaultMaxTok
	}
	w := wireRequest{
		Model:     pick(req.Options.Model, m.model),
		MaxTokens: maxTok,
		System:    req.System,
		Messages:  toWireMessages(req.Messages),
		Tools:     toWireTools(req.Tools),
		Stream:    req.Options.Stream,
	}
	if req.Options.Temperature != 0 {
		t := req.Options.Temperature
		w.Temperature = &t
	}
	return json.Marshal(w)
}

func toWireMessages(msgs []core.Message) []wireMessage {
	out := make([]wireMessage, 0, len(msgs))
	for _, msg := range msgs {
		switch msg.Role {
		case core.RoleTool:
			// Tool results map to a user message of tool_result blocks.
			var blocks []wireBlock
			for _, p := range msg.Parts {
				if tr, ok := p.(core.ToolResult); ok {
					blocks = append(blocks, wireBlock{
						Type:      "tool_result",
						ToolUseID: tr.CallID,
						Content:   partsToText(tr.Content),
						IsError:   tr.IsError,
					})
				}
			}
			out = append(out, wireMessage{Role: "user", Content: blocks})
		case core.RoleAssistant:
			out = append(out, wireMessage{Role: "assistant", Content: toWireBlocks(msg.Parts)})
		default: // user / system handled via System field; treat as user
			out = append(out, wireMessage{Role: "user", Content: toWireBlocks(msg.Parts)})
		}
	}
	return out
}

func toWireBlocks(parts []core.Part) []wireBlock {
	var blocks []wireBlock
	for _, p := range parts {
		switch v := p.(type) {
		case core.Text:
			blocks = append(blocks, wireBlock{Type: "text", Text: v.Text})
		case core.ToolCall:
			blocks = append(blocks, wireBlock{Type: "tool_use", ID: v.ID, Name: v.Name, Input: v.Args})
		}
	}
	if len(blocks) == 0 {
		blocks = append(blocks, wireBlock{Type: "text", Text: ""})
	}
	return blocks
}

func toWireTools(tools []llm.ToolSchema) []wireTool {
	if len(tools) == 0 {
		return nil
	}
	out := make([]wireTool, len(tools))
	for i, t := range tools {
		out[i] = wireTool{Name: t.Name, Description: t.Description, InputSchema: t.Parameters}
	}
	return out
}

// --- non-streaming response -------------------------------------------------

type wireResponse struct {
	Content    []wireBlock `json:"content"`
	StopReason string      `json:"stop_reason"`
	Usage      wireUsage   `json:"usage"`
}

type wireUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

func parseResponse(data []byte) (*llm.Response, error) {
	var wr wireResponse
	if err := json.Unmarshal(data, &wr); err != nil {
		return nil, fmt.Errorf("anthropic: decode response: %w", err)
	}
	var parts []core.Part
	for _, b := range wr.Content {
		switch b.Type {
		case "text":
			parts = append(parts, core.Text{Text: b.Text})
		case "tool_use":
			parts = append(parts, core.ToolCall{ID: b.ID, Name: b.Name, Args: b.Input})
		}
	}
	return &llm.Response{
		Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
		StopReason: mapStop(wr.StopReason),
		Usage:      &core.Usage{InputTokens: wr.Usage.InputTokens, OutputTokens: wr.Usage.OutputTokens},
	}, nil
}

// --- streaming --------------------------------------------------------------

// ParseStream converts an Anthropic SSE body into a stream of responses: a
// partial response per text delta, then a final aggregated response. It is
// exported so it can be unit-tested with a canned SSE reader.
func ParseStream(r io.Reader) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		agg := &streamAgg{}
		for ev, err := range sse.Scan(r) {
			if err != nil {
				yield(nil, err)
				return
			}
			emit, partial, done, perr := agg.handle(ev)
			if perr != nil {
				yield(nil, perr)
				return
			}
			if emit && !yield(partial, nil) {
				return
			}
			if done {
				yield(agg.final(), nil)
				return
			}
		}
		// Stream ended without an explicit message_stop; emit what we have.
		yield(agg.final(), nil)
	}
}

// streamAgg accumulates Anthropic streaming blocks into a message.
type streamAgg struct {
	texts    map[int]*string // index -> accumulated text
	tools    map[int]*toolAcc
	order    []int
	kinds    map[int]string
	stop     string
	usageIn  int
	usageOut int
}

type toolAcc struct {
	id, name string
	jsonBuf  string
}

func (a *streamAgg) ensure(idx int, kind string) {
	if a.texts == nil {
		a.texts = map[int]*string{}
		a.tools = map[int]*toolAcc{}
		a.kinds = map[int]string{}
	}
	if _, seen := a.kinds[idx]; !seen {
		a.kinds[idx] = kind
		a.order = append(a.order, idx)
		if kind == "text" {
			s := ""
			a.texts[idx] = &s
		} else {
			a.tools[idx] = &toolAcc{}
		}
	}
}

func (a *streamAgg) handle(ev sse.Event) (emit bool, partial *llm.Response, done bool, err error) {
	if ev.Data == "" {
		return false, nil, false, nil
	}
	switch ev.Name {
	case "message_start":
		var d struct {
			Message struct {
				Usage wireUsage `json:"usage"`
			} `json:"message"`
		}
		_ = json.Unmarshal([]byte(ev.Data), &d)
		a.usageIn = d.Message.Usage.InputTokens
	case "content_block_start":
		var d struct {
			Index        int       `json:"index"`
			ContentBlock wireBlock `json:"content_block"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &d); err != nil {
			return false, nil, false, err
		}
		a.ensure(d.Index, d.ContentBlock.Type)
		if d.ContentBlock.Type == "tool_use" {
			t := a.tools[d.Index]
			t.id, t.name = d.ContentBlock.ID, d.ContentBlock.Name
		}
	case "content_block_delta":
		var d struct {
			Index int `json:"index"`
			Delta struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				PartialJSON string `json:"partial_json"`
			} `json:"delta"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &d); err != nil {
			return false, nil, false, err
		}
		switch d.Delta.Type {
		case "text_delta":
			a.ensure(d.Index, "text")
			*a.texts[d.Index] += d.Delta.Text
			return true, a.snapshot(), false, nil // stream text as it arrives
		case "input_json_delta":
			a.ensure(d.Index, "tool_use")
			a.tools[d.Index].jsonBuf += d.Delta.PartialJSON
		}
	case "message_delta":
		var d struct {
			Delta struct {
				StopReason string `json:"stop_reason"`
			} `json:"delta"`
			Usage wireUsage `json:"usage"`
		}
		_ = json.Unmarshal([]byte(ev.Data), &d)
		if d.Delta.StopReason != "" {
			a.stop = d.Delta.StopReason
		}
		if d.Usage.OutputTokens != 0 {
			a.usageOut = d.Usage.OutputTokens
		}
	case "message_stop":
		return false, nil, true, nil
	}
	return false, nil, false, nil
}

// snapshot builds a partial response from accumulated text only (tool calls are
// included only in the final response, once their JSON is complete).
func (a *streamAgg) snapshot() *llm.Response {
	var parts []core.Part
	for _, idx := range a.order {
		if a.kinds[idx] == "text" {
			parts = append(parts, core.Text{Text: *a.texts[idx]})
		}
	}
	return &llm.Response{Message: core.Message{Role: core.RoleAssistant, Parts: parts}, Partial: true}
}

func (a *streamAgg) final() *llm.Response {
	var parts []core.Part
	for _, idx := range a.order {
		switch a.kinds[idx] {
		case "text":
			parts = append(parts, core.Text{Text: *a.texts[idx]})
		case "tool_use":
			t := a.tools[idx]
			args := t.jsonBuf
			if args == "" {
				args = "{}"
			}
			parts = append(parts, core.ToolCall{ID: t.id, Name: t.name, Args: json.RawMessage(args)})
		}
	}
	return &llm.Response{
		Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
		StopReason: mapStop(a.stop),
		Usage:      &core.Usage{InputTokens: a.usageIn, OutputTokens: a.usageOut},
	}
}

func mapStop(reason string) llm.StopReason {
	switch reason {
	case "tool_use":
		return llm.StopToolUse
	case "max_tokens":
		return llm.StopMaxTokens
	default:
		return llm.StopEnd
	}
}

func partsToText(parts []core.Part) string {
	var s string
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			s += t.Text
		}
	}
	return s
}

func pick(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

var _ llm.Model = (*Model)(nil)
