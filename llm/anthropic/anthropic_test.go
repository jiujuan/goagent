package anthropic

import (
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

const annoStream = `event: message_start
data: {"type":"message_start","message":{"usage":{"input_tokens":10}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tu_1","name":"get_weather"}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"NYC\"}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":5}}

event: message_stop
data: {"type":"message_stop"}

`

func TestParseStream(t *testing.T) {
	var partials []string
	var final *llm.Response
	for r, err := range ParseStream(strings.NewReader(annoStream)) {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
		if r.Partial {
			partials = append(partials, r.Message.Text())
		} else {
			final = r
		}
	}

	wantPartials := []string{"Hello", "Hello world"}
	if strings.Join(partials, "|") != strings.Join(wantPartials, "|") {
		t.Fatalf("partials = %v, want %v", partials, wantPartials)
	}

	if final == nil {
		t.Fatal("no final response")
	}
	if final.Message.Text() != "Hello world" {
		t.Fatalf("final text = %q", final.Message.Text())
	}
	calls := final.Message.ToolCalls()
	if len(calls) != 1 || calls[0].Name != "get_weather" || string(calls[0].Args) != `{"city":"NYC"}` {
		t.Fatalf("tool calls = %+v", calls)
	}
	if final.StopReason != llm.StopToolUse {
		t.Fatalf("stop = %v", final.StopReason)
	}
	if final.Usage == nil || final.Usage.InputTokens != 10 || final.Usage.OutputTokens != 5 {
		t.Fatalf("usage = %+v", final.Usage)
	}
}

func TestRoundTripWireMessages(t *testing.T) {
	// An assistant tool_call followed by a tool result should convert without
	// losing the correlation id.
	msgs := []core.Message{
		core.UserText("hi"),
		{Role: core.RoleAssistant, Parts: []core.Part{core.ToolCall{ID: "x", Name: "t", Args: []byte(`{}`)}}},
		{Role: core.RoleTool, Parts: []core.Part{core.ToolResult{CallID: "x", Name: "t", Content: []core.Part{core.Text{Text: "ok"}}}}},
	}
	wire := toWireMessages(msgs)
	if len(wire) != 3 {
		t.Fatalf("want 3 wire messages, got %d", len(wire))
	}
	if wire[2].Content[0].ToolUseID != "x" {
		t.Fatalf("tool_use_id not preserved: %+v", wire[2].Content[0])
	}
}
