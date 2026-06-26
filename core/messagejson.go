package core

import "encoding/json"

// Part is a sealed interface, so encoding/json cannot unmarshal a []Part on its
// own — it has no way to pick a concrete type. To make Message round-trip
// through JSON (required for JSONL session persistence), Message marshals each
// part inside a tagged envelope ({"type":"text",...}) and reconstructs the
// concrete type on the way back. tool_result parts nest the same envelope for
// their Content.

type partWire struct {
	Type       string          `json:"type"`
	Text       string          `json:"text,omitempty"`
	MIME       string          `json:"mime,omitempty"`
	Data       []byte          `json:"data,omitempty"`
	URL        string          `json:"url,omitempty"`
	DurationMs int             `json:"duration_ms,omitempty"`
	ID         string          `json:"id,omitempty"`
	Name       string          `json:"name,omitempty"`
	Args       json.RawMessage `json:"args,omitempty"`
	CallID     string          `json:"call_id,omitempty"`
	IsError    bool            `json:"is_error,omitempty"`
	Content    []partWire      `json:"content,omitempty"`
}

type messageWire struct {
	Role  Role       `json:"role"`
	Parts []partWire `json:"parts"`
}

// MarshalJSON implements json.Marshaler.
func (m Message) MarshalJSON() ([]byte, error) {
	w := messageWire{Role: m.Role, Parts: make([]partWire, len(m.Parts))}
	for i, p := range m.Parts {
		w.Parts[i] = partToWire(p)
	}
	return json.Marshal(w)
}

// UnmarshalJSON implements json.Unmarshaler.
func (m *Message) UnmarshalJSON(b []byte) error {
	var w messageWire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	m.Role = w.Role
	m.Parts = make([]Part, len(w.Parts))
	for i, pw := range w.Parts {
		m.Parts[i] = wireToPart(pw)
	}
	return nil
}

func partToWire(p Part) partWire {
	switch v := p.(type) {
	case Text:
		return partWire{Type: "text", Text: v.Text}
	case Thinking:
		return partWire{Type: "thinking", Text: v.Text}
	case Image:
		return partWire{Type: "image", MIME: v.MIME, Data: v.Data, URL: v.URL}
	case Video:
		return partWire{Type: "video", MIME: v.MIME, Data: v.Data, URL: v.URL, DurationMs: v.DurationMs}
	case ToolCall:
		return partWire{Type: "tool_call", ID: v.ID, Name: v.Name, Args: v.Args}
	case ToolResult:
		content := make([]partWire, len(v.Content))
		for i, c := range v.Content {
			content[i] = partToWire(c)
		}
		return partWire{Type: "tool_result", CallID: v.CallID, Name: v.Name, IsError: v.IsError, Content: content}
	default:
		return partWire{Type: "unknown"}
	}
}

func wireToPart(w partWire) Part {
	switch w.Type {
	case "text":
		return Text{Text: w.Text}
	case "thinking":
		return Thinking{Text: w.Text}
	case "image":
		return Image{MIME: w.MIME, Data: w.Data, URL: w.URL}
	case "video":
		return Video{MIME: w.MIME, Data: w.Data, URL: w.URL, DurationMs: w.DurationMs}
	case "tool_call":
		return ToolCall{ID: w.ID, Name: w.Name, Args: w.Args}
	case "tool_result":
		content := make([]Part, len(w.Content))
		for i, c := range w.Content {
			content[i] = wireToPart(c)
		}
		return ToolResult{CallID: w.CallID, Name: w.Name, IsError: w.IsError, Content: content}
	default:
		return Text{Text: ""}
	}
}
