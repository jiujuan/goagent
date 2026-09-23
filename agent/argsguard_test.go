package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func TestLoopRejectsSchemaInvalidArgsBeforeHandler(t *testing.T) {
	var invoked atomic.Int32
	echo := tool.New("echo", "echo q", func(*tool.Context, struct {
		Q string `json:"q"`
	}) (string, error) {
		invoked.Add(1)
		return "ok", nil
	})

	calls := 0
	var sawTr core.ToolResult
	m := mock.New("bad-args", func(req *llm.Request) *llm.Response {
		calls++
		if calls == 1 {
			// funcTool would happily unmarshal {} into the zero value; only the
			// schema check can catch the missing required field here.
			return mock.CallTool("c1", "echo", `{}`)
		}
		tr, ok := mock.LastToolResult(req)
		if !ok {
			t.Errorf("no tool result after rejected call")
		}
		sawTr = tr
		return mock.Text("done")
	})

	a, err := agent.New(agent.WithModel(m), agent.WithTools(echo))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	if n := invoked.Load(); n != 0 {
		t.Fatalf("handler invoked %d times, want 0", n)
	}
	msg := toolResultText(sawTr)
	if !sawTr.IsError || !strings.Contains(msg, `required argument "q" is missing`) {
		t.Fatalf("tool result = %+v (%q), want schema error about %q", sawTr, msg, "q")
	}
}

// prepTool rewrites a legacy field name before validation.
type prepTool struct {
	schema  json.RawMessage
	gotCity atomic.Value // string: the city the handler actually received
	failOn  string
	handled atomic.Int32
}

func (p *prepTool) Name() string            { return "prep" }
func (p *prepTool) Description() string     { return "prepare and store a city" }
func (p *prepTool) Schema() json.RawMessage { return p.schema }
func (p *prepTool) PrepareArguments(raw json.RawMessage) (json.RawMessage, error) {
	var in struct {
		City     string `json:"city"`
		CityName string `json:"city_name"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err
	}
	if p.failOn != "" && in.City == p.failOn {
		return nil, errors.New("refused by policy")
	}
	if in.City == "" && in.CityName != "" {
		out, _ := json.Marshal(map[string]string{"city": in.CityName})
		return out, nil
	}
	return raw, nil
}
func (p *prepTool) Call(_ *tool.Context, args json.RawMessage) (*tool.Result, error) {
	p.handled.Add(1)
	var in struct {
		City string `json:"city"`
	}
	_ = json.Unmarshal(args, &in)
	p.gotCity.Store(in.City)
	return tool.TextResult("ok"), nil
}

func runWithPrepTool(t *testing.T, p *prepTool, args string) core.ToolResult {
	t.Helper()
	calls := 0
	var sawTr core.ToolResult
	m := mock.New("prep-caller", func(req *llm.Request) *llm.Response {
		calls++
		if calls == 1 {
			return mock.CallTool("c1", "prep", args)
		}
		sawTr, _ = mock.LastToolResult(req)
		return mock.Text("done")
	})
	a, err := agent.New(agent.WithModel(m), agent.WithTools(p))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "go"); err != nil {
		t.Fatal(err)
	}
	return sawTr
}

func TestArgumentPreparerNormalizesBeforeValidation(t *testing.T) {
	p := &prepTool{schema: tool.SchemaFor[struct {
		City string `json:"city"`
	}]()}
	tr := runWithPrepTool(t, p, `{"city_name":"Oslo"}`)
	if tr.IsError {
		t.Fatalf("legacy args rejected: %q", toolResultText(tr))
	}
	if n := p.handled.Load(); n != 1 {
		t.Fatalf("handler ran %d times, want 1", n)
	}
	if got, _ := p.gotCity.Load().(string); got != "Oslo" {
		t.Fatalf("handler saw city %q, want the normalized %q", got, "Oslo")
	}
}

func TestArgumentPreparerRejectionSkipsHandler(t *testing.T) {
	p := &prepTool{
		schema: tool.SchemaFor[struct {
			City string `json:"city"`
		}](),
		failOn: "Oslo",
	}
	tr := runWithPrepTool(t, p, `{"city":"Oslo"}`)
	msg := toolResultText(tr)
	if !tr.IsError || !strings.Contains(msg, "refused by policy") {
		t.Fatalf("tool result = %+v (%q), want preparer rejection", tr, msg)
	}
	if n := p.handled.Load(); n != 0 {
		t.Fatalf("handler ran %d times after rejection, want 0", n)
	}
}
