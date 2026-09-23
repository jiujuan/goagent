package agent_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// overlap measures the max number of handlers executing at the same time.
type overlap struct {
	cur, peak atomic.Int32
}

func (o *overlap) wrap(fn func() (string, error)) func() (string, error) {
	return func() (string, error) {
		n := o.cur.Add(1)
		for {
			p := o.peak.Load()
			if n <= p || o.peak.CompareAndSwap(p, n) {
				break
			}
		}
		defer o.cur.Add(-1)
		return fn()
	}
}

func slowTool(t *testing.T, name string, o *overlap) tool.Tool {
	t.Helper()
	return tool.New(name, "sleeps briefly", func(*tool.Context, struct{}) (string, error) {
		res, err := o.wrap(func() (string, error) {
			time.Sleep(40 * time.Millisecond)
			return "ok", nil
		})()
		return res, err
	})
}

func callTwo(n1, n2 string) *llm.Response {
	return &llm.Response{
		Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
			core.ToolCall{ID: "c1", Name: n1, Args: json.RawMessage(`{}`)},
			core.ToolCall{ID: "c2", Name: n2, Args: json.RawMessage(`{}`)},
		}},
		StopReason: llm.StopToolUse,
	}
}

func twoCallModel(once *llm.Response) *mock.Model {
	calls := 0
	return mock.New("two-caller", func(req *llm.Request) *llm.Response {
		calls++
		if calls == 1 {
			return once
		}
		return mock.Text("done")
	})
}

func TestSequentialToolDowngradesWholeBatch(t *testing.T) {
	o := &overlap{}
	plain := slowTool(t, "plain", o)
	base := slowTool(t, "seq", o)
	seq := tool.AsSequential(base)

	// The wrapper must remain fully transparent besides the capability.
	if seq.Name() != base.Name() || seq.Description() != base.Description() || string(seq.Schema()) != string(base.Schema()) {
		t.Fatal("AsSequential wrapper broke delegation")
	}
	if _, ok := seq.(tool.SequentialTool); !ok {
		t.Fatal("AsSequential result does not implement SequentialTool")
	}
	if _, ok := plain.(tool.SequentialTool); ok {
		t.Fatal("plain tool must not implement SequentialTool")
	}

	a, err := agent.New(agent.WithModel(twoCallModel(callTwo("plain", "seq"))), agent.WithTools(plain, seq))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if p := o.peak.Load(); p != 1 {
		t.Fatalf("peak concurrency = %d, want 1 (one sequential tool must serialize the batch)", p)
	}
}

func TestParallelBatchOverlaps(t *testing.T) {
	o := &overlap{}
	a1 := slowTool(t, "a1", o)
	a2 := slowTool(t, "a2", o)

	a, err := agent.New(agent.WithModel(twoCallModel(callTwo("a1", "a2"))), agent.WithTools(a1, a2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if p := o.peak.Load(); p != 2 {
		t.Fatalf("peak concurrency = %d, want 2 (default mode is parallel)", p)
	}
}
