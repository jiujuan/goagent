package core

import "maps"

// CloneMessage returns a detached copy of a Message and every framework-owned
// mutable value reachable from it. Parts are value types, but several of them
// contain byte slices or nested Parts, so copying only Message.Parts would
// still let a caller mutate committed history through an alias.
func CloneMessage(m Message) Message {
	out := Message{Role: m.Role, Parts: make([]Part, len(m.Parts))}
	for i, part := range m.Parts {
		out.Parts[i] = clonePart(part)
	}
	return out
}

// CloneMessages returns detached copies of msgs in their original order.
func CloneMessages(msgs []Message) []Message {
	out := make([]Message, len(msgs))
	for i, msg := range msgs {
		out[i] = CloneMessage(msg)
	}
	return out
}

// CloneEvent returns a detached copy of an Event. StateDelta values are copied
// at the map boundary; values placed in session State must be treated as
// immutable after Set because arbitrary Go values cannot be cloned safely.
func CloneEvent(e *Event) *Event {
	if e == nil {
		return nil
	}
	out := *e
	out.GraphManaged = false
	out.MergeParents = append([]string(nil), e.MergeParents...)
	if e.Message != nil {
		msg := CloneMessage(*e.Message)
		out.Message = &msg
	}
	if e.Actions.StateDelta != nil {
		out.Actions.StateDelta = maps.Clone(e.Actions.StateDelta)
	}
	out.Actions.StateDelete = append([]string(nil), e.Actions.StateDelete...)
	if e.Usage != nil {
		usage := *e.Usage
		out.Usage = &usage
	}
	if e.Progress != nil {
		progress := *e.Progress
		out.Progress = &progress
	}
	return &out
}

func clonePart(part Part) Part {
	switch value := part.(type) {
	case Text:
		return value
	case Thinking:
		return value
	case Image:
		value.Data = append([]byte(nil), value.Data...)
		return value
	case Video:
		value.Data = append([]byte(nil), value.Data...)
		return value
	case ToolCall:
		value.Args = append([]byte(nil), value.Args...)
		return value
	case ToolResult:
		content := value.Content
		value.Content = make([]Part, len(content))
		for i, nested := range content {
			value.Content[i] = clonePart(nested)
		}
		return value
	default:
		return value
	}
}
