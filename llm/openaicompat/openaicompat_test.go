package openaicompat

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

const oaiStream = `data: {"choices":[{"delta":{"role":"assistant","content":"Hel"},"finish_reason":null}]}

data: {"choices":[{"delta":{"content":"lo"},"finish_reason":null}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":\"NYC\"}"}}]},"finish_reason":null}]}

data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}

data: [DONE]

`

func TestParseStream(t *testing.T) {
	var partials []string
	var final *llm.Response
	for r, err := range ParseStream(strings.NewReader(oaiStream)) {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
		if r.Partial {
			partials = append(partials, r.Message.Text())
		} else {
			final = r
		}
	}

	wantPartials := []string{"Hel", "Hello"}
	if strings.Join(partials, "|") != strings.Join(wantPartials, "|") {
		t.Fatalf("partials = %v, want %v", partials, wantPartials)
	}
	if final == nil {
		t.Fatal("no final response")
	}
	if final.Message.Text() != "Hello" {
		t.Fatalf("final text = %q", final.Message.Text())
	}
	calls := final.Message.ToolCalls()
	if len(calls) != 1 || calls[0].ID != "call_1" || calls[0].Name != "get_weather" || string(calls[0].Args) != `{"city":"NYC"}` {
		t.Fatalf("tool calls = %+v", calls)
	}
	if final.StopReason != llm.StopToolUse {
		t.Fatalf("stop = %v", final.StopReason)
	}
	if final.Usage == nil || final.Usage.InputTokens != 10 || final.Usage.OutputTokens != 5 {
		t.Fatalf("usage = %+v", final.Usage)
	}
}

func TestGenerateClassifiesContextLength(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"message":"This model's maximum context length is 8192 tokens, however you requested 9000 tokens","type":"invalid_request_error","code":"context_length_exceeded"}}`)
	}))
	defer srv.Close()

	m := New(Config{BaseURL: srv.URL, APIKey: "k", Model: "m"})
	var got error
	for _, err := range m.Generate(context.Background(), &llm.Request{Messages: []core.Message{core.UserText("hi")}}) {
		if err != nil {
			got = err
		}
	}
	var cle *llm.ContextLengthError
	if !errors.As(got, &cle) {
		t.Fatalf("error = %v (%T), want *llm.ContextLengthError", got, got)
	}
	if cle.Limit != 8192 || cle.Requested != 9000 {
		t.Fatalf("limit/requested = %d/%d, want 8192/9000", cle.Limit, cle.Requested)
	}
}
