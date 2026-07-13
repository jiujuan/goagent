package core

import (
	"encoding/json"
	"testing"
)

func TestEventJSONRoundTrip(t *testing.T) {
	events := []Event{
		RunStarted{RunID: "r1", ThreadID: "t1"},
		RunDone{Result: Result{Message: AssistantText("done")}},
		RunFailed{Err: errString("boom")},
		TurnStarted{Step: 2},
		TurnDone{Step: 2},
		MessageDelta{Delta: AssistantText("hi")},
		MessageDone{Message: AssistantText("final"), Usage: &Usage{InputTokens: 3, OutputTokens: 5}},
		ToolStarted{Call: ToolCall{ID: "c1", Name: "get_weather", Args: json.RawMessage(`{"city":"北京"}`)}},
		ToolUpdate{CallID: "c1", Partial: Text{Text: "partial"}},
		ToolDone{Result: ToolResult{CallID: "c1", Name: "get_weather", Content: []Part{Text{Text: "晴"}}, IsError: false}},
		Interrupted{Pending: []ApprovalRequest{{CallID: "c1", Tool: "danger", Args: []byte(`{"x":1}`)}}},
		Progress{Job: ProgressInfo{JobID: "j1", Kind: "video", Status: "running", Percent: 42}},
		PlanNodeStarted{NodeID: "research"},
		PlanNodeDone{NodeID: "research", Status: "failed", Err: errString("nope")},
	}

	for _, ev := range events {
		b, err := MarshalEvent(ev)
		if err != nil {
			t.Fatalf("marshal %T: %v", ev, err)
		}
		got, err := UnmarshalEvent(b)
		if err != nil {
			t.Fatalf("unmarshal %T: %v (json=%s)", ev, err, b)
		}
		// Compare via re-marshal (errors don't compare with DeepEqual).
		b2, err := MarshalEvent(got)
		if err != nil {
			t.Fatalf("re-marshal %T: %v", got, err)
		}
		if string(b) != string(b2) {
			t.Fatalf("%T did not round-trip:\n in:  %s\n out: %s", ev, b, b2)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }
