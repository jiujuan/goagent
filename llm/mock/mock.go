// Package mock provides a programmable llm.Model for tests and examples. It
// makes no network calls: a Responder function inspects the request and returns
// the assistant message to emit, so you can script multi-turn tool-calling
// flows deterministically.
package mock

import (
	"context"
	"iter"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Responder decides the response for a given request. It receives the full
// request (including accumulated history) so it can branch on prior tool
// results.
type Responder func(req *llm.Request) *llm.Response

// Model is a non-streaming, in-memory llm.Model driven by a Responder.
type Model struct {
	name string
	resp Responder
}

// New builds a mock model with the given name and responder.
func New(name string, resp Responder) *Model {
	return &Model{name: name, resp: resp}
}

// Name implements llm.Model.
func (m *Model) Name() string { return m.name }

// Generate implements llm.Model: it yields exactly one response.
func (m *Model) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		r := m.resp(req)
		if r == nil {
			r = &llm.Response{Message: core.AssistantText(""), StopReason: llm.StopEnd}
		}
		yield(r, nil)
	}
}

// --- helpers for writing responders ----------------------------------------

// Text returns a final text response.
func Text(s string) *llm.Response {
	return &llm.Response{
		Message:    core.AssistantText(s),
		StopReason: llm.StopEnd,
		Usage:      &core.Usage{OutputTokens: len(s) / 4},
	}
}

// CallTool returns a response asking to invoke a single tool.
func CallTool(id, name string, args string) *llm.Response {
	return &llm.Response{
		Message: core.Message{
			Role:  core.RoleAssistant,
			Parts: []core.Part{core.ToolCall{ID: id, Name: name, Args: []byte(args)}},
		},
		StopReason: llm.StopToolUse,
	}
}

// Partial returns a streaming partial text response.
func Partial(s string) *llm.Response {
	return &llm.Response{Message: core.AssistantText(s), Partial: true}
}

// StreamModel is a mock llm.Model that yields a scripted sequence of responses
// (typically several partials followed by a final), for exercising the
// streaming path through the turn engine without a network call.
type StreamModel struct {
	name string
	fn   func(req *llm.Request) []*llm.Response
}

// NewStream builds a streaming mock model.
func NewStream(name string, fn func(req *llm.Request) []*llm.Response) *StreamModel {
	return &StreamModel{name: name, fn: fn}
}

// Name implements llm.Model.
func (m *StreamModel) Name() string { return m.name }

// Generate implements llm.Model: it yields the scripted responses in order.
func (m *StreamModel) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		for _, r := range m.fn(req) {
			if !yield(r, nil) {
				return
			}
		}
	}
}

var _ llm.Model = (*StreamModel)(nil)

// LastToolResult reports the most recent tool result in the history, if any.
// Responders use it to tell "first call" from "after the tool ran".
func LastToolResult(req *llm.Request) (core.ToolResult, bool) {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		for _, p := range req.Messages[i].Parts {
			if tr, ok := p.(core.ToolResult); ok {
				return tr, true
			}
		}
	}
	return core.ToolResult{}, false
}
