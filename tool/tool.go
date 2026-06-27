// Package tool defines the Tool contract and a generic constructor that derives
// a JSON Schema from a typed handler. Tools decide nothing about control flow on
// their own; they are capabilities the agent's loop invokes by name. A tool may
// however *request* control (Result.Control) and state mutations (Result.State),
// which the loop applies explicitly.
package tool

import (
	"context"
	"encoding/json"

	"github.com/jiujuan/goagent/core"
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

	// Control, when set, requests a control-flow change after this tool runs
	// (e.g. Escalate to break a Loop, Stop to end the run). The loop folds it
	// with other directives by precedence.
	Control *core.Directive
	// State holds declarative state mutations the loop applies immediately.
	State []core.StateOp
}

// Context is handed to a tool on invocation. It embeds the request context and
// exposes the live run State directly (no session indirection). CallID
// correlates the invocation to its originating ToolCall.
type Context struct {
	context.Context
	State  *core.State
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
