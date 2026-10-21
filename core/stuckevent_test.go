package core

import (
	"encoding/json"
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
