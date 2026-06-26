// Package llm defines the provider-agnostic Model interface and the
// Request/Response types exchanged with a large language model. Concrete
// providers live in subpackages (llm/anthropic, llm/openaicompat, llm/mock) so
// the core abstraction carries no provider-specific dependencies.
package llm

import (
	"context"
	"encoding/json"
	"iter"

	"github.com/jiujuan/goagent/core"
)

// Model is the minimal contract a provider must satisfy. Generate returns a
// Stream of Response values: a streaming model yields many partial responses
// followed by a final one, while a non-streaming model yields exactly one.
type Model interface {
	// Name reports the model identifier, e.g. "claude-opus-4-8".
	Name() string

	// Generate runs one model call and streams the result.
	Generate(ctx context.Context, req *Request) iter.Seq2[*Response, error]
}

// Request is one model invocation.
type Request struct {
	// System is the system prompt.
	System string

	// Messages is the conversation history in internal form. Providers convert
	// it to their wire format.
	Messages []core.Message

	// Tools advertises the callable tools for this request.
	Tools []ToolSchema

	// Options carries decoding parameters, populated via functional options.
	Options Options
}

// Response is one increment (or the whole result) of a model call.
type Response struct {
	// Message is the assistant message produced so far. For partial responses
	// it holds the accumulated content; for the final response it is complete.
	Message core.Message

	// Partial marks a streaming increment that should not be persisted.
	Partial bool

	// StopReason explains why generation ended (set on the final response).
	StopReason StopReason

	// Usage reports token consumption (set on the final response when known).
	Usage *core.Usage
}

// StopReason enumerates why a model stopped generating.
type StopReason string

const (
	StopEnd       StopReason = "end"        // natural end of turn
	StopToolUse   StopReason = "tool_use"   // model wants to call tools
	StopMaxTokens StopReason = "max_tokens" // hit the output token cap
	StopError     StopReason = "error"      // provider error
)

// ToolSchema is the provider-neutral description of a callable tool: its name,
// a natural-language description, and a JSON Schema for its parameters.
type ToolSchema struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}
