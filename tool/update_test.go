package tool_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// collector is a context that also implements tool.Updater, standing in for the
// agent's run context.
type collector struct {
	context.Context

	mu  sync.Mutex
	got []string
}

func (c *collector) UpdateTool(callID string, p core.Part) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if txt, ok := p.(core.Text); ok {
		c.got = append(c.got, callID+"="+txt.Text)
	}
}

func streamer() tool.Tool {
	return tool.New("stream", "reports progress", func(ctx *tool.Context, _ struct{}) (string, error) {
		ctx.Update(core.Text{Text: "half"})
		return "done", nil
	})
}

// TestUpdateReachesTheRunContext: a tool invoked by a run streams its partials
// to that run's Updater, tagged with the CallID it was invoked under.
func TestUpdateReachesTheRunContext(t *testing.T) {
	c := &collector{Context: context.Background()}
	res, err := streamer().Call(&tool.Context{Context: c, CallID: "c9"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Content[0].(core.Text).Text != "done" {
		t.Fatalf("result = %+v", res)
	}
	if len(c.got) != 1 || c.got[0] != "c9=half" {
		t.Fatalf("updates = %v, want [c9=half]", c.got)
	}
}

// TestUpdateWithoutReceiverIsDropped: outside a run there is no observer. A bare
// context, a missing CallID, and a nil Context must all be safe, because tools
// are also called directly (scripts, tests).
func TestUpdateWithoutReceiverIsDropped(t *testing.T) {
	if _, err := streamer().Call(&tool.Context{Context: context.Background(), CallID: "c1"}, nil); err != nil {
		t.Fatal(err)
	}
	c := &collector{Context: context.Background()}
	if _, err := streamer().Call(&tool.Context{Context: c}, nil); err != nil {
		t.Fatal(err)
	}
	if len(c.got) != 0 {
		t.Fatalf("a call with no CallID streamed %v", c.got)
	}
	var nilCtx *tool.Context
	nilCtx.Update(core.Text{Text: "ignored"})
}
