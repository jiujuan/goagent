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
