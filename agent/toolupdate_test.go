package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// TestToolUpdateStreamsWhileToolRuns: core.ToolUpdate is what a long-running
// tool reports progress with, correlated by CallID to its ToolStarted/ToolDone
// pair. The partials are transient — they must not reach the model or the
// checkpointed history.
func TestToolUpdateStreamsWhileToolRuns(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()

	slow := tool.New("render", "long job", func(tctx *tool.Context, _ struct{}) (string, error) {
		tctx.Update(core.Text{Text: "30%"})
		tctx.Update(core.Text{Text: "90%"})
		return "finished", nil
	})
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "render", "{}")
	})
	a, err := agent.New(
		agent.WithModel(model), agent.WithTools(slow), agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	var seq []string
	var updates []string
	run := a.Stream(ctx, "go")
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolStarted:
			seq = append(seq, "start")
		case core.ToolUpdate:
			if e.CallID != "c1" {
				t.Fatalf("ToolUpdate for call %q, want c1", e.CallID)
			}
			seq = append(seq, "update")
			if txt, ok := e.Partial.(core.Text); !ok {
				t.Fatalf("partial is %T, want core.Text", e.Partial)
			} else {
				updates = append(updates, txt.Text)
			}
		case core.ToolDone:
			seq = append(seq, "done")
		}
	}
	res, err := run.Wait()
	if err != nil {
		t.Fatal(err)
	}

	if got := strings.Join(seq, " "); got != "start update update done" {
		t.Fatalf("event sequence = %q", got)
	}
	if strings.Join(updates, ",") != "30%,90%" {
		t.Fatalf("updates = %v", updates)
	}
	// The model answered from the final result only.
	if res.Message.Text() != "saw:finished" {
		t.Fatalf("result = %q", res.Message.Text())
	}
	// And nothing transient was persisted.
	cp, err := store.Latest(ctx, run.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range cp.State.Messages {
		for _, p := range m.Parts {
			if tr, ok := p.(core.ToolResult); ok {
				if strings.Contains(tr.Content[0].(core.Text).Text, "%") {
					t.Fatalf("partial result leaked into history: %+v", tr)
				}
			}
		}
	}
}

// TestToolUpdateParallelBatchStaysCorrelated: parallel tools each stream under
// their own CallID, so an observer can still attribute every partial.
func TestToolUpdateParallelBatchStaysCorrelated(t *testing.T) {
	ctx := context.Background()
	noop := tool.New("noop", "streams once", func(tctx *tool.Context, in struct {
		ID string `json:"id"`
	}) (string, error) {
		tctx.Update(core.Text{Text: "working:" + in.ID})
		return in.ID, nil
	})
	var calls int
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		calls++
		if calls > 1 {
			return mock.Text("done")
		}
		return &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.ToolCall{ID: "a", Name: "noop", Args: []byte(`{"id":"a"}`)},
				core.ToolCall{ID: "b", Name: "noop", Args: []byte(`{"id":"b"}`)},
			}},
			StopReason: llm.StopToolUse,
		}
	})
	a, _ := agent.New(agent.WithModel(model), agent.WithTools(noop))

	byCall := map[string]int{}
	run := a.Stream(ctx, "go")
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if u, ok := ev.(core.ToolUpdate); ok {
			byCall[u.CallID]++
		}
	}
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	if byCall["a"] != 1 || byCall["b"] != 1 {
		t.Fatalf("updates by call = %v, want one each", byCall)
	}
}
