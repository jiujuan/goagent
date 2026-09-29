package middleware

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
)

func TestSignatureCanonicalization(t *testing.T) {
	sig := func(args string, strip map[string]bool) string {
		return signature("t", json.RawMessage(args), strip)
	}
	if sig(`{"a":1,"b":2}`, nil) != sig(`{"b":2,"a":1}`, nil) {
		t.Fatal("key order must not change the signature")
	}
	if sig(`{"id":"x","a":1}`, map[string]bool{"id": true}) != sig(`{"a":1,"id":"y"}`, map[string]bool{"id": true}) {
		t.Fatal("stripped keys must not change the signature")
	}
	if sig(`{"id":"x","a":1}`, nil) == sig(`{"a":1}`, nil) {
		t.Fatal("without strip the ids must distinguish")
	}
	if sig(`{"a":1}`, nil) == sig(`{"a":2}`, nil) {
		t.Fatal("different values must differ")
	}
	if sig(`broken{`, nil) != sig(`broken{`, nil) || sig(`broken{`, nil) == sig(`other{`, nil) {
		t.Fatal("malformed JSON must sign stably as raw bytes")
	}
	if sig(`{"a":1}`, nil) == signature("u", json.RawMessage(`{"a":1}`), nil) {
		t.Fatal("tool name must be part of the signature")
	}
}

// TestKVResumeShapes checks the guard reads its State.KV entries after a JSON
// round-trip through the file checkpointer (map[string]any / float64).
func TestKVResumeShapes(t *testing.T) {
	g := LoopGuard(LoopGuardOptions{}).(*loopGuard)
	sig := signature("fetch", json.RawMessage(`{}`), nil)

	lc := &agent.LoopContext{RunContext: &agent.RunContext{
		Context: context.Background(),
		State: &core.State{KV: map[string]any{
			kvLoopActions:       map[string]any{sig: "interrupt"},
			kvLoopInterventions: float64(2),
		}},
	}}
	if got := interventions(lc); got != 2 {
		t.Fatalf("interventions = %d, want 2", got)
	}
	d, err := g.BeforeTool(lc, &core.ToolCall{ID: "c1", Name: "fetch", Args: json.RawMessage(`{}`)})
	if err != nil || d.Kind != core.Interrupt {
		t.Fatalf("resumed mark should interrupt: kind=%v err=%v", d.Kind, err)
	}

	lc.State.KV[kvLoopActions] = map[string]any{sig: "stop"}
	if d, _ := g.BeforeTool(lc, &core.ToolCall{ID: "c1", Name: "fetch", Args: json.RawMessage(`{}`)}); d.Kind != core.Stop {
		t.Fatalf("stop mark should stop, got %v", d.Kind)
	}

	// Live (pre-serialization) shapes must work identically.
	lc.State.KV[kvLoopActions] = map[string]string{sig: actionInterrupt}
	lc.State.KV[kvLoopInterventions] = 5
	if got := interventions(lc); got != 5 {
		t.Fatalf("live interventions = %d, want 5", got)
	}

	// Unmarked calls pass.
	lc2 := &agent.LoopContext{RunContext: &agent.RunContext{
		Context: context.Background(),
		State:   &core.State{},
	}}
	if d, err := g.BeforeTool(lc2, &core.ToolCall{ID: "c", Name: "fetch", Args: json.RawMessage(`{}`)}); err != nil || d.Kind != core.Continue {
		t.Fatalf("unmarked call must continue: %v %v", d, err)
	}
}

func TestErrorStreakScan(t *testing.T) {
	g := LoopGuard(LoopGuardOptions{ErrorStreakThreshold: 3}).(*loopGuard)
	errMsg := func(name, text string) core.Message {
		return core.Message{Role: core.RoleTool, Parts: []core.Part{
			core.ToolResult{Name: name, IsError: true, Content: []core.Part{core.Text{Text: text}}},
		}}
	}
	okMsg := func(name string) core.Message {
		return core.Message{Role: core.RoleTool, Parts: []core.Part{
			core.ToolResult{Name: name, Content: []core.Part{core.Text{Text: "fine"}}},
		}}
	}

	h := []core.Message{errMsg("boom", "same line\nstack"), errMsg("boom", "same line\nother stack"), errMsg("boom", "same line")}
	if tool, text, count, _, ok := g.errorStreak(h); !ok || tool != "boom" || count != 3 || text != "same line" {
		t.Fatalf("streak=%d tool=%q text=%q ok=%v", count, tool, text, ok)
	}
	// A different error signature breaks the run.
	h2 := []core.Message{errMsg("boom", "aaa"), errMsg("boom", "bbb"), errMsg("boom", "ccc")}
	if _, _, count, _, _ := g.errorStreak(h2); count != 1 {
		t.Fatalf("distinct errors must not stack, count=%d", count)
	}
	// Success breaks it.
	h3 := []core.Message{errMsg("boom", "aaa"), okMsg("other"), errMsg("boom", "aaa")}
	if _, _, count, _, _ := g.errorStreak(h3); count != 1 {
		t.Fatalf("success must break the streak, count=%d", count)
	}
	// Exempt tools neither break nor extend.
	h4 := []core.Message{errMsg("boom", "aaa"), errMsg("write_todos", "zzz"), errMsg("boom", "aaa")}
	if _, _, count, _, _ := g.errorStreak(h4); count != 2 {
		t.Fatalf("exempt errors must be transparent, count=%d", count)
	}
}
