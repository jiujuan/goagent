package runtime_test

import (
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/runtime"
)

func TestStackOnionOrdering(t *testing.T) {
	var log []string
	s := runtime.NewStack(
		recorder{name: "a", log: &log},
		recorder{name: "b", log: &log},
	)
	lc := &runtime.LoopContext{}

	if _, err := s.BeforeModel(lc); err != nil {
		t.Fatal(err)
	}
	if err := s.ModifyRequest(lc, &llm.Request{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AfterModel(lc, &llm.Response{}); err != nil {
		t.Fatal(err)
	}

	want := []string{"BM:a", "BM:b", "MR:a", "MR:b", "AM:b", "AM:a"}
	if !equal(log, want) {
		t.Fatalf("hook order\n got: %v\nwant: %v", log, want)
	}
}

func TestStackFoldsDirectivesByPrecedence(t *testing.T) {
	var log []string
	s := runtime.NewStack(
		recorder{name: "a", log: &log, beforeModel: core.Directive{Kind: core.Continue}},
		recorder{name: "b", log: &log, beforeModel: core.Directive{Kind: core.Stop}},
	)
	d, err := s.BeforeModel(&runtime.LoopContext{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != core.Stop {
		t.Fatalf("folded directive = %v, want Stop", d.Kind)
	}
}

func TestStackModifyRequestMutates(t *testing.T) {
	var log []string
	s := runtime.NewStack(
		recorder{name: "a", log: &log, modifySys: " [a]"},
		recorder{name: "b", log: &log, modifySys: " [b]"},
	)
	req := &llm.Request{System: "base"}
	if err := s.ModifyRequest(&runtime.LoopContext{}, req); err != nil {
		t.Fatal(err)
	}
	if req.System != "base [a] [b]" {
		t.Fatalf("System = %q, want %q", req.System, "base [a] [b]")
	}
}
