package agent_test

import (
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

type recorder struct {
	agent.BaseMiddleware
	name        string
	log         *[]string
	beforeModel core.Directive
}

func (r recorder) BeforeModel(*agent.LoopContext) (core.Directive, error) {
	*r.log = append(*r.log, "BM:"+r.name)
	return r.beforeModel, nil
}
func (r recorder) AfterModel(*agent.LoopContext, *llm.Response) (core.Directive, error) {
	*r.log = append(*r.log, "AM:"+r.name)
	return core.Directive{}, nil
}

func TestStackOnionOrdering(t *testing.T) {
	var log []string
	s := agent.NewStack(
		recorder{name: "a", log: &log},
		recorder{name: "b", log: &log},
	)
	lc := &agent.LoopContext{}
	if _, err := s.BeforeModel(lc); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AfterModel(lc, &llm.Response{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"BM:a", "BM:b", "AM:b", "AM:a"}
	if len(log) != len(want) {
		t.Fatalf("got %v want %v", log, want)
	}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("order: got %v want %v", log, want)
		}
	}
}

func TestStackFoldsByPrecedence(t *testing.T) {
	var log []string
	s := agent.NewStack(
		recorder{name: "a", log: &log, beforeModel: core.Directive{Kind: core.Continue}},
		recorder{name: "b", log: &log, beforeModel: core.Directive{Kind: core.Stop}},
	)
	d, err := s.BeforeModel(&agent.LoopContext{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != core.Stop {
		t.Fatalf("folded = %v, want Stop", d.Kind)
	}
}
