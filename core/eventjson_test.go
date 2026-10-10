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
		ApprovalDecided{Decision: ApprovalDecision{DecisionID: "decision-1", PauseID: "pause-1", CallID: "c1", Tool: "danger", ParameterDigest: "params", PolicyVersion: "policy-v1", Approved: true, Reason: "reviewed", Digest: "digest"}, Duplicate: true},
		Interrupted{
			Pending:  []ApprovalRequest{{CallID: "c1", Tool: "danger", Args: []byte(`{"x":1}`)}},
			Phase:    "before_tool",
			Reason:   "approval required",
			Recovery: "resume_tools",
		},
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

func TestApprovalSchemaCompatibility(t *testing.T) {
	want := ApprovalDecided{Decision: ApprovalDecision{
		DecisionID:      "decision-1",
		PauseID:         "pause-1",
		CallID:          "call-1",
		Tool:            "danger",
		ParameterDigest: "parameters",
		PolicyVersion:   "policy-v1",
		Approved:        false,
		Reason:          "needs review",
		Digest:          "digest",
	}, Duplicate: true}
	encoded, err := MarshalEvent(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalEvent(encoded)
	if err != nil {
		t.Fatal(err)
	}
	approval, ok := got.(ApprovalDecided)
	if !ok || approval != want {
		t.Fatalf("approval round trip = %#v, want %#v", got, want)
	}
	if _, err := UnmarshalEvent([]byte(`{"type":"approval_decided"}`)); err == nil {
		t.Fatal("approval_decided without payload decoded successfully")
	}
	legacy, err := UnmarshalEvent([]byte(`{"type":"run_started","run_id":"r1","thread_id":"t1"}`))
	if err != nil {
		t.Fatalf("legacy non-approval event failed: %v", err)
	}
	if started, ok := legacy.(RunStarted); !ok || started.RunID != "r1" || started.ThreadID != "t1" {
		t.Fatalf("legacy event = %#v", legacy)
	}
}

func TestInterruptedEventJSONLegacyCompatibility(t *testing.T) {
	legacy := []byte(`{"type":"interrupted","pending":[{"call_id":"c1","tool":"danger","args":"e30="}]}`)
	ev, err := UnmarshalEvent(legacy)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := ev.(Interrupted)
	if !ok {
		t.Fatalf("event = %T, want Interrupted", ev)
	}
	if len(got.Pending) != 1 || got.Pending[0].CallID != "c1" || string(got.Pending[0].Args) != "{}" {
		t.Fatalf("pending = %#v, want legacy request", got.Pending)
	}
	if got.Phase != "" || got.Reason != "" || got.Recovery != "" {
		t.Fatalf("legacy metadata = %#v, want zero values", got)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
