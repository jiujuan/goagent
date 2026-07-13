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
