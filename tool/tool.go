// Package tool defines the Tool contract and a generic constructor that derives
// a JSON Schema from a typed handler. Tools decide nothing about control flow;
// they are pure capabilities the agent's turn engine invokes by name.
package tool

import (
	"context"
	"encoding/json"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/session"
)

// Tool is the capability contract. It is deliberately small: a name and
// description for the model to reason about, a JSON Schema for its arguments,
// and a Call that executes it.
type Tool interface {
	Name() string
	Description() string
	Schema() json.RawMessage
	Call(ctx *Context, args json.RawMessage) (*Result, error)
}

// Result is a tool's output.
type Result struct {
	// Content is the result rendered as message parts (usually one Text part).
	Content []core.Part
	// IsError marks the result as a failure to report back to the model.
	IsError bool
}

// Context is handed to a tool on invocation. It embeds the request context and
// exposes session state plus an Actions sink, so tools can read/write state and
// request side effects (e.g. confirmation, escalation) declaratively.
type Context struct {
	context.Context
	// State is the live session state.
	State session.State
	// Actions accumulates side effects requested by the tool; the turn engine
	// folds them into the resulting event.
	Actions *core.Actions
	// CallID correlates this invocation to the originating ToolCall.
	CallID string
}

// TextResult is a convenience constructor for a successful text result.
func TextResult(s string) *Result {
	return &Result{Content: []core.Part{core.Text{Text: s}}}
}

// ErrorResult is a convenience constructor for an error result.
func ErrorResult(s string) *Result {
	return &Result{Content: []core.Part{core.Text{Text: s}}, IsError: true}
}
