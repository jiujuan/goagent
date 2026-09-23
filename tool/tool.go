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

// SequentialTool is an optional capability: a tool that must not run alongside
// other tools in the same batch (rate-limited API, non-reentrant resource,
// order-sensitive side effects). If any call in a parallel batch targets such a
// tool, the whole batch executes one at a time, in the model's call order.
type SequentialTool interface{ SequentialExecution() bool }

// AsSequential marks any tool as sequential-executing, forcing its batches to
// run one at a time regardless of the agent's ToolExecMode.
func AsSequential(t Tool) Tool { return sequentialTool{t} }

type sequentialTool struct{ Tool }

func (sequentialTool) SequentialExecution() bool { return true }

// TextResult is a convenience constructor for a successful text result.
func TextResult(s string) *Result {
	return &Result{Content: []core.Part{core.Text{Text: s}}}
}

// ErrorResult is a convenience constructor for an error result.
func ErrorResult(s string) *Result {
	return &Result{Content: []core.Part{core.Text{Text: s}}, IsError: true}
}
