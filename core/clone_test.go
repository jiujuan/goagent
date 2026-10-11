package core

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

type cloneNamedInt int

type cloneStateChild struct{ Values []int }

type cloneStateRecord struct {
	Labels map[string]string
	Child  *cloneStateChild
	When   time.Time
}

type cloneTestFileStore struct{}

func (*cloneTestFileStore) Read(string) ([]byte, error)   { return nil, nil }
func (*cloneTestFileStore) Write(string, []byte) error    { return nil }
func (*cloneTestFileStore) List(string) ([]string, error) { return nil, nil }

func TestSnapshotConsistencyCloneNestedState(t *testing.T) {
	files := &cloneTestFileStore{}
	when := time.Date(2026, time.October, 11, 8, 30, 0, 123, time.FixedZone("CST", 8*60*60))
	state := State{
		Messages: []Message{{Role: RoleUser, Parts: []Part{Image{Data: []byte{1, 2, 3}}}}},
		Todos:    []Todo{{ID: "todo-1", Title: "before", Status: "pending"}},
		Files:    files,
		KV: map[string]any{
			"int": 7, "named": cloneNamedInt(8),
			"strings": map[string]string{"key": "before"},
			"any":     map[string]any{"items": []any{map[string]any{"value": "before"}}},
			"bytes":   []byte{4, 5, 6}, "raw": json.RawMessage(`{"value":"before"}`),
			"number": json.Number("12.5"),
			"record": cloneStateRecord{Labels: map[string]string{"key": "before"}, Child: &cloneStateChild{Values: []int{1, 2}}, When: when},
			"array":  [1][]byte{{7, 8}}, "nil_map": map[string]any(nil),
			"nil_slice": []string(nil), "nil_ptr": (*cloneStateChild)(nil),
		},
	}

	got, err := CloneStateData(state)
	if err != nil {
		t.Fatalf("CloneStateData: %v", err)
	}
	if got.Files != files {
		t.Fatalf("Files = %T %p, want original handle %p", got.Files, got.Files, files)
	}
	assertCloneType[int](t, "int", got.KV["int"])
	assertCloneType[cloneNamedInt](t, "named", got.KV["named"])
	assertCloneType[map[string]string](t, "strings", got.KV["strings"])
	assertCloneType[map[string]any](t, "any", got.KV["any"])
	assertCloneType[[]byte](t, "bytes", got.KV["bytes"])
	assertCloneType[json.RawMessage](t, "raw", got.KV["raw"])
	assertCloneType[json.Number](t, "number", got.KV["number"])
	if got.KV["nil_map"].(map[string]any) != nil || got.KV["nil_slice"].([]string) != nil || got.KV["nil_ptr"].(*cloneStateChild) != nil {
		t.Fatal("clone did not preserve nil container semantics")
	}

	got.Messages[0].Parts[0].(Image).Data[0] = 99
	got.Todos[0].Title = "changed"
	got.KV["strings"].(map[string]string)["key"] = "changed"
	got.KV["any"].(map[string]any)["items"].([]any)[0].(map[string]any)["value"] = "changed"
	got.KV["bytes"].([]byte)[0] = 99
	got.KV["raw"].(json.RawMessage)[0] = '['
	record := got.KV["record"].(cloneStateRecord)
	record.Labels["key"], record.Child.Values[0] = "changed", 99
	array := got.KV["array"].([1][]byte)
	array[0][0] = 99

	originalRecord := state.KV["record"].(cloneStateRecord)
	if state.Messages[0].Parts[0].(Image).Data[0] != 1 || state.Todos[0].Title != "before" ||
		state.KV["strings"].(map[string]string)["key"] != "before" ||
		state.KV["any"].(map[string]any)["items"].([]any)[0].(map[string]any)["value"] != "before" ||
		state.KV["bytes"].([]byte)[0] != 4 || string(state.KV["raw"].(json.RawMessage)) != `{"value":"before"}` ||
		originalRecord.Labels["key"] != "before" || originalRecord.Child.Values[0] != 1 || state.KV["array"].([1][]byte)[0][0] != 7 {
		t.Fatal("clone still aliases nested state data")
	}
	if !record.When.Equal(when) || record.When.Location() != when.Location() {
		t.Fatalf("time.Time = %v, want %v", record.When, when)
	}
}

func assertCloneType[T any](t *testing.T, key string, value any) {
	t.Helper()
	if _, ok := value.(T); !ok {
		t.Fatalf("%s type = %T, want %T", key, value, *new(T))
	}
}

func TestSnapshotConsistencyCloneMessageParts(t *testing.T) {
	original := Message{Role: RoleAssistant, Parts: []Part{
		Text{Text: "answer"}, Thinking{Text: "reasoning"},
		Image{MIME: "image/png", Data: []byte{1, 2}, URL: "image-url"},
		Video{MIME: "video/mp4", Data: []byte{3, 4}, URL: "video-url", DurationMs: 2500},
		ToolCall{ID: "call-1", Name: "lookup", Args: json.RawMessage(`{"query":"before"}`)},
		ToolResult{CallID: "call-1", Name: "lookup", Content: []Part{
			Text{Text: "result"}, Image{MIME: "image/jpeg", Data: []byte{5, 6}},
			ToolResult{CallID: "nested", Name: "nested", Content: []Part{
				Video{MIME: "video/webm", Data: []byte{7, 8}},
				ToolCall{ID: "call-2", Name: "nested", Args: json.RawMessage(`{"nested":true}`)},
			}},
		}},
		ToolResult{CallID: "empty", Name: "empty", Content: nil},
	}}

	got, err := CloneMessage(original)
	if err != nil {
		t.Fatalf("CloneMessage: %v", err)
	}
	if !reflect.DeepEqual(got, original) {
		t.Fatalf("clone mismatch:\n got  %#v\n want %#v", got, original)
	}
	if got.Parts[6].(ToolResult).Content != nil {
		t.Fatal("nil ToolResult.Content became non-nil")
	}

	got.Parts[2].(Image).Data[0] = 99
	got.Parts[3].(Video).Data[0] = 99
	got.Parts[4].(ToolCall).Args[0] = '['
	result := got.Parts[5].(ToolResult)
	result.Content[1].(Image).Data[0] = 99
	nested := result.Content[2].(ToolResult)
	nested.Content[0].(Video).Data[0] = 99
	nested.Content[1].(ToolCall).Args[0] = '['

	originalResult := original.Parts[5].(ToolResult)
	originalNested := originalResult.Content[2].(ToolResult)
	if original.Parts[2].(Image).Data[0] != 1 || original.Parts[3].(Video).Data[0] != 3 ||
		string(original.Parts[4].(ToolCall).Args) != `{"query":"before"}` || originalResult.Content[1].(Image).Data[0] != 5 ||
		originalNested.Content[0].(Video).Data[0] != 7 || string(originalNested.Content[1].(ToolCall).Args) != `{"nested":true}` {
		t.Fatal("clone still aliases message part data")
	}

	nilParts, err := CloneMessage(Message{Role: RoleUser})
	if err != nil {
		t.Fatalf("CloneMessage nil parts: %v", err)
	}
	if nilParts.Parts != nil {
		t.Fatal("nil Message.Parts became non-nil")
	}

	for _, tc := range []struct {
		name     string
		part     Part
		typeName string
	}{
		{name: "unknown", part: cloneUnsupportedPart{}, typeName: "core.cloneUnsupportedPart"},
		{name: "nil", part: nil, typeName: "<nil>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CloneMessage(Message{Parts: []Part{tc.part}})
			if !errors.Is(err, ErrStateNotCloneable) {
				t.Fatalf("error = %v, want ErrStateNotCloneable", err)
			}
			if !strings.Contains(err.Error(), "message.parts[0]") || !strings.Contains(err.Error(), tc.typeName) {
				t.Fatalf("error = %q, want part path and type %q", err, tc.typeName)
			}
		})
	}
}

type cloneUnsupportedPart struct{}

func (cloneUnsupportedPart) isPart() {}

type clonePrivateState struct {
	Visible string
	hidden  []string
}

type cloneCustomJSON struct{ Value string }

func (v cloneCustomJSON) MarshalJSON() ([]byte, error) { return json.Marshal(v.Value) }

type cloneCustomUnmarshal struct{ Value string }

func (v *cloneCustomUnmarshal) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &v.Value)
}

func TestSnapshotConsistencyCloneInvalidState(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	tests := []struct {
		name     string
		value    any
		typeName string
	}{
		{name: "func", value: func() {}, typeName: "func()"},
		{name: "channel", value: make(chan int), typeName: "chan int"},
		{name: "cycle", value: cycle, typeName: "map[string]interface {}"},
		{name: "non-finite float", value: math.Inf(1), typeName: "float64"},
		{name: "private mutable field", value: clonePrivateState{Visible: "ok", hidden: []string{"secret"}}, typeName: "core.clonePrivateState"},
		{name: "custom JSON marshaler", value: cloneCustomJSON{Value: "custom"}, typeName: "core.cloneCustomJSON"},
		{name: "custom JSON unmarshaler", value: cloneCustomUnmarshal{Value: "custom"}, typeName: "core.cloneCustomUnmarshal"},
		{name: "non-string map key", value: map[int]string{1: "one"}, typeName: "map[int]string"},
		{name: "invalid raw JSON", value: json.RawMessage(`{"broken"`), typeName: "json.RawMessage"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CloneStateData(State{KV: map[string]any{"bad": tc.value}})
			if !errors.Is(err, ErrStateNotCloneable) {
				t.Fatalf("error = %v, want ErrStateNotCloneable", err)
			}
			if !strings.Contains(err.Error(), `state.kv["bad"]`) {
				t.Fatalf("error = %q, want KV path", err)
			}
			if !strings.Contains(err.Error(), tc.typeName) {
				t.Fatalf("error = %q, want concrete type %q", err, tc.typeName)
			}
		})
	}
}
