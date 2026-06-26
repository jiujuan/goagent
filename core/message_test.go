package core

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMessageJSONRoundTrip(t *testing.T) {
	cases := []Message{
		UserText("hello"),
		{Role: RoleAssistant, Parts: []Part{
			Thinking{Text: "let me think"},
			Text{Text: "hi there"},
			ToolCall{ID: "c1", Name: "get_weather", Args: json.RawMessage(`{"city":"NYC"}`)},
		}},
		{Role: RoleTool, Parts: []Part{
			ToolResult{CallID: "c1", Name: "get_weather", IsError: false, Content: []Part{
				Text{Text: "Sunny 25C"},
			}},
		}},
		{Role: RoleUser, Parts: []Part{
			Image{MIME: "image/png", Data: []byte{0x01, 0x02, 0x03}},
		}},
	}

	for i, m := range cases {
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("case %d marshal: %v", i, err)
		}
		var got Message
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("case %d unmarshal: %v (json: %s)", i, err, b)
		}
		if !reflect.DeepEqual(m, got) {
			t.Fatalf("case %d round-trip mismatch:\n want %#v\n got  %#v\n json %s", i, m, got, b)
		}
	}
}

func TestEventJSONRoundTrip(t *testing.T) {
	ev := &Event{
		ID:           "evt_1",
		InvocationID: "inv_1",
		Author:       "assistant",
		Message:      &Message{Role: RoleAssistant, Parts: []Part{Text{Text: "done"}}},
		Usage:        &Usage{InputTokens: 10, OutputTokens: 5},
		Actions:      Actions{StateDelta: map[string]any{"answer": "done"}},
	}
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var got Event
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != ev.ID || got.Author != ev.Author {
		t.Fatalf("scalar fields lost: %+v", got)
	}
	if got.Message == nil || got.Message.Text() != "done" {
		t.Fatalf("message lost: %+v", got.Message)
	}
	if got.Usage == nil || got.Usage.OutputTokens != 5 {
		t.Fatalf("usage lost: %+v", got.Usage)
	}
	if got.Actions.StateDelta["answer"] != "done" {
		t.Fatalf("state delta lost: %+v", got.Actions.StateDelta)
	}
}
