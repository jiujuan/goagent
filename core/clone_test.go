package core

import "testing"

func TestCloneEventDetachesNestedMutableParts(t *testing.T) {
	original := &Event{
		Message: &Message{Role: RoleAssistant, Parts: []Part{
			Image{Data: []byte{1, 2, 3}},
			ToolCall{ID: "call", Name: "tool", Args: []byte(`{"value":1}`)},
			ToolResult{CallID: "call", Content: []Part{Image{Data: []byte{4, 5, 6}}}},
		}},
		Actions: Actions{StateDelta: map[string]any{"key": "original"}},
	}

	clone := CloneEvent(original)
	clone.Message.Parts[0].(Image).Data[0] = 9
	clone.Message.Parts[1].(ToolCall).Args[0] = 'x'
	result := clone.Message.Parts[2].(ToolResult)
	result.Content[0].(Image).Data[0] = 8
	clone.Actions.StateDelta["key"] = "clone"

	if got := original.Message.Parts[0].(Image).Data[0]; got != 1 {
		t.Fatalf("original image byte = %d, want 1", got)
	}
	if got := original.Message.Parts[1].(ToolCall).Args[0]; got != '{' {
		t.Fatalf("original tool args first byte = %q, want '{'", got)
	}
	nested := original.Message.Parts[2].(ToolResult).Content[0].(Image)
	if got := nested.Data[0]; got != 4 {
		t.Fatalf("original nested image byte = %d, want 4", got)
	}
	if got := original.Actions.StateDelta["key"]; got != "original" {
		t.Fatalf("original state delta = %v, want original", got)
	}
}
