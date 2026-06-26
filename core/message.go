// Package core holds the shared vocabulary of goagent: the provider-agnostic
// message model, the event type that carries side effects, and the Stream
// streaming primitive. Every other package depends on core and (ideally) not
// on each other, which keeps the dependency graph acyclic.
package core

import "encoding/json"

// Role identifies who authored a Message.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message is the internal, provider-agnostic representation of one turn of
// conversation. Providers convert []Message to and from their wire format at
// the call boundary, so custom part types never leak into the rest of the
// system.
type Message struct {
	Role  Role   `json:"role"`
	Parts []Part `json:"parts"`
}

// Part is a sealed tagged union: only the types defined in this package may
// implement it (via the unexported isPart marker). This gives type-safe
// multi-modal content without a discriminator field.
type Part interface {
	isPart()
}

// Text is a plain-text content part.
type Text struct {
	Text string `json:"text"`
}

// Thinking is reasoning/extended-thinking content emitted by reasoning models.
type Thinking struct {
	Text string `json:"text"`
}

// Image is an image content part, referenced either inline (Data + MIME) or by
// URL.
type Image struct {
	MIME string `json:"mime,omitempty"`
	Data []byte `json:"data,omitempty"`
	URL  string `json:"url,omitempty"`
}

// Video is a video content part, referenced either inline (Data + MIME) or by
// URL. Video-generation models emit it; like every Part it flows through the
// session history and event stream unchanged.
type Video struct {
	MIME       string `json:"mime,omitempty"`
	Data       []byte `json:"data,omitempty"`
	URL        string `json:"url,omitempty"`
	DurationMs int    `json:"duration_ms,omitempty"`
}

// ToolCall is a request by the assistant to invoke a tool.
type ToolCall struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

// ToolResult is the outcome of a tool invocation, correlated to a ToolCall by
// CallID.
type ToolResult struct {
	CallID  string `json:"call_id"`
	Name    string `json:"name"`
	Content []Part `json:"content"`
	IsError bool   `json:"is_error,omitempty"`
}

func (Text) isPart()       {}
func (Thinking) isPart()   {}
func (Image) isPart()      {}
func (Video) isPart()      {}
func (ToolCall) isPart()   {}
func (ToolResult) isPart() {}

// --- Constructors -----------------------------------------------------------

// UserText builds a user message from a single string.
func UserText(s string) Message {
	return Message{Role: RoleUser, Parts: []Part{Text{Text: s}}}
}

// AssistantText builds an assistant message from a single string.
func AssistantText(s string) Message {
	return Message{Role: RoleAssistant, Parts: []Part{Text{Text: s}}}
}

// --- Accessors --------------------------------------------------------------

// Text concatenates all Text parts of the message.
func (m Message) Text() string {
	var s string
	for _, p := range m.Parts {
		if t, ok := p.(Text); ok {
			s += t.Text
		}
	}
	return s
}

// ToolCalls returns every ToolCall part in the message.
func (m Message) ToolCalls() []ToolCall {
	var calls []ToolCall
	for _, p := range m.Parts {
		if c, ok := p.(ToolCall); ok {
			calls = append(calls, c)
		}
	}
	return calls
}
