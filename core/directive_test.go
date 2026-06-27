package core

import "testing"

func TestResolvePrecedence(t *testing.T) {
	tests := []struct {
		name string
		in   []Directive
		want DirectiveKind
	}{
		{"empty defaults to continue", nil, Continue},
		{"single", []Directive{{Kind: Transfer}}, Transfer},
		{"stop beats transfer", []Directive{{Kind: Transfer}, {Kind: Stop}}, Stop},
		{"interrupt beats all", []Directive{{Kind: Stop}, {Kind: Interrupt}, {Kind: Escalate}}, Interrupt},
		{"escalate beats transfer", []Directive{{Kind: Transfer}, {Kind: Escalate}}, Escalate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Resolve(tt.in...).Kind; got != tt.want {
				t.Fatalf("Resolve = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveTieKeepsFirst(t *testing.T) {
	got := Resolve(Directive{Kind: Transfer, Target: "a"}, Directive{Kind: Transfer, Target: "b"})
	if got.Target != "a" {
		t.Fatalf("tie should keep first: got %q", got.Target)
	}
}

func TestStateApply(t *testing.T) {
	var s State
	s.Apply(
		StateOp{Kind: OpSetKV, Key: "pref", Value: "concise"},
		StateOp{Kind: OpAddTodo, Value: Todo{ID: "1", Title: "research"}},
	)
	if s.KV["pref"] != "concise" {
		t.Fatalf("OpSetKV not applied: %v", s.KV)
	}
	if len(s.Todos) != 1 || s.Todos[0].Title != "research" {
		t.Fatalf("OpAddTodo not applied: %v", s.Todos)
	}
}
