// Package openaicompat implements llm.Model against any OpenAI-compatible
// /chat/completions endpoint, with optional SSE streaming. DeepSeek and many
// Chinese model gateways (e.g. "agnes") speak this protocol, so they share one
// implementation; only the base URL, API key, and model id differ.
package openaicompat

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

// Config configures an OpenAI-compatible model.
type Config struct {
	BaseURL string // e.g. https://api.deepseek.com/v1
	APIKey  string
	Model   string
	HTTP    *http.Client
}

// Model is an OpenAI-compatible llm.Model.
type Model struct {
	cfg Config
}

// New constructs a Model from an explicit Config (use this for custom
// gateways).
func New(cfg Config) *Model {
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 5 * time.Minute}
	}
	return &Model{cfg: cfg}
}

// OpenAI targets api.openai.com.
func OpenAI(model, apiKey string) *Model {
	return New(Config{BaseURL: "https://api.openai.com/v1", APIKey: apiKey, Model: model})
}

// DeepSeek targets api.deepseek.com (e.g. model "deepseek-chat").
func DeepSeek(model, apiKey string) *Model {
	return New(Config{BaseURL: "https://api.deepseek.com/v1", APIKey: apiKey, Model: model})
}

// Agnes targets the agnes gateway. The base URL is configurable because agnes
// is an OpenAI-compatible endpoint whose host is deployment-specific.
func Agnes(baseURL, model, apiKey string) *Model {
	return New(Config{BaseURL: baseURL, APIKey: apiKey, Model: model})
}

func (m *Model) Name() string { return m.cfg.Model }

// Generate implements llm.Model. With Options.Stream it parses an SSE stream
// and yields incremental partial responses followed by a final one; otherwise
// it yields a single response.
func (m *Model) Generate(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		body, err := json.Marshal(m.buildBody(req))
		if err != nil {
			yield(nil, err)
			return
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, m.cfg.BaseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			yield(nil, err)
			return
		}
		httpReq.Header.Set("content-type", "application/json")
		httpReq.Header.Set("authorization", "Bearer "+m.cfg.APIKey)

		resp, err := m.cfg.HTTP.Do(httpReq)
		if err != nil {
			yield(nil, err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			yield(nil, &llm.StatusError{Provider: "openaicompat", Code: resp.StatusCode, Body: string(data)})
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
	Model         string         `json:"model"`
	Messages      []wireMessage  `json:"messages"`
	Tools         []wireTool     `json:"tools,omitempty"`
	Temperature   float64        `json:"temperature,omitempty"`
	MaxTokens     int            `json:"max_tokens,omitempty"`
	Stop          []string       `json:"stop,omitempty"`
	Stream        bool           `json:"stream,omitempty"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type wireMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content,omitempty"`
	ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type wireToolCall struct {
	Index    int    `json:"index,omitempty"`
	ID       string `json:"id,omitempty"`
	Type     string `json:"type,omitempty"`
	Function struct {
		Name      string `json:"name,omitempty"`
		Arguments string `json:"arguments,omitempty"`
	} `json:"function"`
}

type wireTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

func (m *Model) buildBody(req *llm.Request) wireRequest {
	w := wireRequest{
		Model:       pick(req.Options.Model, m.cfg.Model),
		Messages:    toWireMessages(req.System, req.Messages),
		Tools:       toWireTools(req.Tools),
		Temperature: req.Options.Temperature,
		MaxTokens:   req.Options.MaxTokens,
		Stop:        req.Options.Stop,
		Stream:      req.Options.Stream,
	}
	if req.Options.Stream {
		w.StreamOptions = &streamOptions{IncludeUsage: true}
	}
	return w
}

func toWireMessages(system string, msgs []core.Message) []wireMessage {
	out := make([]wireMessage, 0, len(msgs)+1)
	if system != "" {
		out = append(out, wireMessage{Role: "system", Content: system})
	}
	for _, msg := range msgs {
		switch msg.Role {
		case core.RoleTool:
			for _, p := range msg.Parts {
				if tr, ok := p.(core.ToolResult); ok {
					out = append(out, wireMessage{
						Role:       "tool",
						ToolCallID: tr.CallID,
						Content:    partsToText(tr.Content),
					})
				}
			}
		case core.RoleAssistant:
			wm := wireMessage{Role: "assistant", Content: msg.Text()}
			for _, c := range msg.ToolCalls() {
				tc := wireToolCall{ID: c.ID, Type: "function"}
				tc.Function.Name = c.Name
				tc.Function.Arguments = string(c.Args)
				wm.ToolCalls = append(wm.ToolCalls, tc)
			}
			out = append(out, wm)
		default:
			out = append(out, wireMessage{Role: "user", Content: msg.Text()})
		}
	}
	return out
}

func toWireTools(tools []llm.ToolSchema) []wireTool {
	if len(tools) == 0 {
		return nil
	}
	out := make([]wireTool, len(tools))
	for i, t := range tools {
		out[i].Type = "function"
		out[i].Function.Name = t.Name
		out[i].Function.Description = t.Description
		out[i].Function.Parameters = t.Parameters
	}
	return out
}

// --- non-streaming response -------------------------------------------------

type wireUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type wireResponse struct {
	Choices []struct {
		Message struct {
			Content   string         `json:"content"`
			ToolCalls []wireToolCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage wireUsage `json:"usage"`
}

func parseResponse(data []byte) (*llm.Response, error) {
	var wr wireResponse
	if err := json.Unmarshal(data, &wr); err != nil {
		return nil, fmt.Errorf("openaicompat: decode response: %w", err)
	}
	if len(wr.Choices) == 0 {
		return nil, fmt.Errorf("openaicompat: no choices in response")
	}
	choice := wr.Choices[0]
	var parts []core.Part
	if choice.Message.Content != "" {
		parts = append(parts, core.Text{Text: choice.Message.Content})
	}
	for _, tc := range choice.Message.ToolCalls {
		parts = append(parts, core.ToolCall{ID: tc.ID, Name: tc.Function.Name, Args: json.RawMessage(tc.Function.Arguments)})
	}
	return &llm.Response{
		Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
		StopReason: mapStop(choice.FinishReason),
		Usage:      &core.Usage{InputTokens: wr.Usage.PromptTokens, OutputTokens: wr.Usage.CompletionTokens},
	}, nil
}

// --- streaming --------------------------------------------------------------

// ParseStream converts an OpenAI-compatible SSE body into a stream of
// responses: a partial response per content delta, then a final aggregated
// response. Exported for unit testing with a canned SSE reader.
func ParseStream(r io.Reader) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		agg := &streamAgg{tools: map[int]*toolAcc{}}
		for ev, err := range sse.Scan(r) {
			if err != nil {
				yield(nil, err)
				return
			}
			if ev.Data == "" {
				continue
			}
			if ev.Data == "[DONE]" {
				yield(agg.final(), nil)
				return
			}
			emit, partial, perr := agg.handle(ev.Data)
			if perr != nil {
				yield(nil, perr)
				return
			}
			if emit && !yield(partial, nil) {
				return
			}
		}
		yield(agg.final(), nil)
	}
}

type streamAgg struct {
	content  string
	tools    map[int]*toolAcc
	order    []int
	stop     string
	usageIn  int
	usageOut int
}

type toolAcc struct {
	id, name string
	args     string
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content   string         `json:"content"`
			ToolCalls []wireToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *wireUsage `json:"usage"`
}

func (a *streamAgg) handle(data string) (emit bool, partial *llm.Response, err error) {
	var c streamChunk
	if err := json.Unmarshal([]byte(data), &c); err != nil {
		return false, nil, err
	}
	if c.Usage != nil {
		a.usageIn = c.Usage.PromptTokens
		a.usageOut = c.Usage.CompletionTokens
	}
	if len(c.Choices) == 0 {
		return false, nil, nil
	}
	ch := c.Choices[0]
	if ch.FinishReason != "" {
		a.stop = ch.FinishReason
	}
	for _, tc := range ch.Delta.ToolCalls {
		t, seen := a.tools[tc.Index]
		if !seen {
			t = &toolAcc{}
			a.tools[tc.Index] = t
			a.order = append(a.order, tc.Index)
		}
		if tc.ID != "" {
			t.id = tc.ID
		}
		if tc.Function.Name != "" {
			t.name = tc.Function.Name
		}
		t.args += tc.Function.Arguments
	}
	if ch.Delta.Content != "" {
		a.content += ch.Delta.Content
		return true, &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{core.Text{Text: a.content}}},
			Partial: true,
		}, nil
	}
	return false, nil, nil
}

func (a *streamAgg) final() *llm.Response {
	var parts []core.Part
	if a.content != "" {
		parts = append(parts, core.Text{Text: a.content})
	}
	for _, idx := range a.order {
		t := a.tools[idx]
		args := t.args
		if args == "" {
			args = "{}"
		}
		parts = append(parts, core.ToolCall{ID: t.id, Name: t.name, Args: json.RawMessage(args)})
	}
	return &llm.Response{
		Message:    core.Message{Role: core.RoleAssistant, Parts: parts},
		StopReason: mapStop(a.stop),
		Usage:      &core.Usage{InputTokens: a.usageIn, OutputTokens: a.usageOut},
	}
}

func mapStop(reason string) llm.StopReason {
	switch reason {
	case "tool_calls":
		return llm.StopToolUse
	case "length":
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
