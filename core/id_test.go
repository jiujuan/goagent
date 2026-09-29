package core

import (
	"strings"
	"testing"
)

func TestCheckThreadIDAccepts(t *testing.T) {
	generated := NewID("thread") // the default id the agent hands out
	ok := []string{
		"a", "h1", "notes", "notes2", "hitl-demo", "settle-A1001", "Mixed_Case-1",
		generated, "thread-" + generated,
		strings.Repeat("x", MaxThreadIDLen),
	}
	for _, id := range ok {
		if err := CheckThreadID(id); err != nil {
			t.Errorf("CheckThreadID(%q) = %v, want nil", id, err)
		}
	}
}

func TestCheckThreadIDRejects(t *testing.T) {
	bad := []string{
		"", " ", "   ", ".", "..", "/", "/abs", "a/b", `x\y`, "weird id:1",
		"会话一", "note.txt", "sess/1", "tenant a",
	}
	for _, id := range bad {
		if err := CheckThreadID(id); err == nil {
			t.Errorf("CheckThreadID(%q) = nil, want an error", id)
		} else if !strings.HasPrefix(err.Error(), "core: thread id") {
			t.Errorf("CheckThreadID(%q) = %v, want a core: thread id error", id, err)
		}
	}
}

func TestCheckThreadIDBoundaryAndWording(t *testing.T) {
	if err := CheckThreadID(strings.Repeat("x", MaxThreadIDLen+1)); err == nil || !strings.Contains(err.Error(), "64") {
		t.Fatalf("over-long id = %v, want an error naming the limit", err)
	}
	if err := CheckThreadID(""); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty id = %v, want it called out as empty", err)
	}
	if err := CheckThreadID("a/b"); err == nil || !strings.Contains(err.Error(), "A-Z") {
		t.Fatalf("illegal char id = %v, want the allowed set named", err)
	}
	// Legal single characters of the allowed set.
	for _, id := range []string{"-", "_", "a1_B-c"} {
		if err := CheckThreadID(id); err != nil {
			t.Errorf("CheckThreadID(%q) = %v, want nil", id, err)
		}
	}
	// Judging is pure: the same id always answers the same way.
	for range 3 {
		if CheckThreadID("tenant/a") == nil || CheckThreadID("tenant_a") != nil {
			t.Fatal("CheckThreadID is not deterministic")
		}
	}
}
