package core

import "testing"

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
