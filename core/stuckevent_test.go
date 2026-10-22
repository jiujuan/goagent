package core

import (
	"encoding/json"
	"slices"
	"testing"
)

func TestStuckDetectedEventRoundTrip(t *testing.T) {
	in := StuckDetected{Rule: "repeat_call", Reason: "tool fetch called twice", Step: 7}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(StuckDetected)
	if !ok || got != in {
		t.Fatalf("round trip = %#v, want %#v", out, in)
	}
}

func TestBudgetExceededEventRoundTrip(t *testing.T) {
	in := BudgetExceeded{Resource: "total_tokens", Step: 3}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(BudgetExceeded)
	if !ok || got != in {
		t.Fatalf("round trip = %#v, want %#v", out, in)
	}
}

func TestHistoryCompactedEventRoundTrip(t *testing.T) {
	in := HistoryCompacted{Dropped: 8, Kept: 6, EstTokens: 8100, Step: 4}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(HistoryCompacted)
	if !ok || got != in {
		t.Fatalf("round trip = %#v, want %#v", out, in)
	}
}

func TestArgRejectedEventRoundTrip(t *testing.T) {
	in := ArgRejected{Tool: "search", Class: "schema_invalid", Count: 3, Step: 5}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(ArgRejected)
	if !ok || got != in {
		t.Fatalf("round trip = %#v, want %#v", out, in)
	}
}

func TestWindowTrimmedEventRoundTrip(t *testing.T) {
	in := WindowTrimmed{Strategy: "importance_weighted", Dropped: 12, Kept: 5, EstTokens: 9400, Step: 6}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(WindowTrimmed)
	if !ok || got != in {
		t.Fatalf("round trip = %#v, want %#v", out, in)
	}
}

func TestStepViewDriftedEventRoundTrip(t *testing.T) {
	in := StepViewDrifted{
		Step:                   0,
		BaselineStep:           3,
		ExpectedToolDigest:     "old-tools",
		ActualToolDigest:       "new-tools",
		ExpectedSettingsDigest: "old-settings",
		ActualSettingsDigest:   "new-settings",
	}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(StepViewDrifted)
	if !ok || got != in {
		t.Fatalf("round trip = %#v, want %#v", out, in)
	}
}

func TestContextOverflowRetriedEventRoundTrip(t *testing.T) {
	in := ContextOverflowRetried{
		Step:           4,
		Attempt:        2,
		Model:          "claude-sonnet",
		Provider:       "anthropic",
		Limit:          200000,
		Requested:      210000,
		MessagesBefore: 12,
		MessagesAfter:  6,
	}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(ContextOverflowRetried)
	if !ok || got != in {
		t.Fatalf("round trip = %#v, want %#v", out, in)
	}
}

func TestContextOverflowRetriedEventOmitsUnknownFields(t *testing.T) {
	in := ContextOverflowRetried{Step: 3}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"attempt", "model", "provider", "limit", "requested",
		"messages_before", "messages_after",
	} {
		if _, ok := raw[key]; ok {
			t.Errorf("zero-valued %q should be omitted from %s", key, data)
		}
	}
}

func TestUnmarshalEventRejectsUnknownType(t *testing.T) {
	if _, err := UnmarshalEvent([]byte(`{"type":"context_overflow_retry"}`)); err == nil {
		t.Fatal("unknown event type should still fail")
	}
}

func TestInputDeliveredEventRoundTrip(t *testing.T) {
	in := InputDelivered{
		Step:       4,
		Source:     InputDurable,
		Phase:      InputNextTurn,
		Count:      2,
		MessageIDs: []string{"msg-a", "msg-b"},
		Preempted:  true,
	}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(InputDelivered)
	if !ok {
		t.Fatalf("round trip type = %T, want InputDelivered", out)
	}
	if got.Step != in.Step || got.Source != in.Source || got.Phase != in.Phase {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
	if got.Count != in.Count || got.Preempted != in.Preempted {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
	if !slices.Equal(got.MessageIDs, in.MessageIDs) {
		t.Fatalf("message IDs = %v, want %v", got.MessageIDs, in.MessageIDs)
	}
}

func TestInputDeliveredEventOmitsEmptyFields(t *testing.T) {
	data, err := MarshalEvent(InputDelivered{Step: 1, Source: InputVolatile, Phase: InputBeforeModel})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"count", "message_ids", "preempted"} {
		if _, ok := raw[key]; ok {
			t.Errorf("zero-valued %q should be omitted from %s", key, data)
		}
	}
}

func TestToolBatchInFlightEventRoundTrip(t *testing.T) {
	in := ToolBatchInFlight{
		Step: 2,
		Calls: []ToolCall{
			{ID: "c1", Name: "rec", Args: json.RawMessage(`{"v":1}`)},
			{ID: "c2", Name: "rec", Args: json.RawMessage(`{}`)},
		},
	}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(ToolBatchInFlight)
	if !ok {
		t.Fatalf("round trip type = %T, want ToolBatchInFlight", out)
	}
	if got.Step != in.Step {
		t.Fatalf("round trip step = %d, want %d", got.Step, in.Step)
	}
	if len(got.Calls) != len(in.Calls) {
		t.Fatalf("round trip calls = %+v, want %+v", got.Calls, in.Calls)
	}
	for i := range in.Calls {
		if got.Calls[i].ID != in.Calls[i].ID || got.Calls[i].Name != in.Calls[i].Name {
			t.Fatalf("round trip call %d = %+v, want %+v", i, got.Calls[i], in.Calls[i])
		}
		if string(got.Calls[i].Args) != string(in.Calls[i].Args) {
			t.Fatalf("round trip call %d args = %s, want %s", i, got.Calls[i].Args, in.Calls[i].Args)
		}
	}
}

func TestToolBatchInFlightEventOmitsEmptyCalls(t *testing.T) {
	data, err := MarshalEvent(ToolBatchInFlight{})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"message", "step"} {
		if _, ok := raw[key]; ok {
			t.Errorf("zero-valued %q should be omitted from %s", key, data)
		}
	}
}

func TestToolBatchRecoveredEventRoundTrip(t *testing.T) {
	in := ToolBatchRecovered{Step: 3, Replayed: 1, Executed: 2, Unknown: 1, Rejected: 1}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(ToolBatchRecovered)
	if !ok {
		t.Fatalf("round trip type = %T, want ToolBatchRecovered", out)
	}
	if got != in {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
}

func TestToolBatchRecoveredEventOmitsZeroCounts(t *testing.T) {
	data, err := MarshalEvent(ToolBatchRecovered{})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"step", "replayed", "executed", "unknown", "rejected"} {
		if _, ok := raw[key]; ok {
			t.Errorf("zero-valued %q should be omitted from %s", key, data)
		}
	}
}

func TestModelPreemptedEventRoundTrip(t *testing.T) {
	in := ModelPreempted{Step: 3, Emitted: true, Count: 2, Reason: "user"}
	data, err := MarshalEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(ModelPreempted)
	if !ok {
		t.Fatalf("round trip type = %T, want ModelPreempted", out)
	}
	if got != in {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
}

func TestModelPreemptedEventOmitsEmptyFields(t *testing.T) {
	data, err := MarshalEvent(ModelPreempted{Step: 1})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"emitted", "count", "reason"} {
		if _, ok := raw[key]; ok {
			t.Errorf("zero-valued %q should be omitted from %s", key, data)
		}
	}
}
