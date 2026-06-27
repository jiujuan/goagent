package mcp

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// mockServer is an in-memory MCP server used to drive the client offline. It
// answers initialize, tools/list, and tools/call, and records the tool names it
// was asked to call so tests can assert prefix stripping.
type mockServer struct {
	mu       sync.Mutex
	called   []string // remote tool names received by tools/call
	failTool string   // a tool name whose result carries isError
}

func (s *mockServer) Open(_ context.Context) (Transport, error) {
	return &mockTransport{srv: s, responses: make(chan json.RawMessage, 16), closed: make(chan struct{})}, nil
}

// mockTransport turns each request into a response synchronously: Send computes
// the reply and enqueues it for Receive to drain.
type mockTransport struct {
	srv       *mockServer
	responses chan json.RawMessage
	closeOnce sync.Once
	closed    chan struct{}
}

func (t *mockTransport) Send(msg json.RawMessage) error {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(msg, &req); err != nil {
		return err
	}
	if len(req.ID) == 0 {
		return nil // notification: no response
	}
	result := t.srv.dispatch(req.Method, req.Params)
	resp, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	select {
	case t.responses <- resp:
	case <-t.closed:
	}
	return nil
}

func (t *mockTransport) Receive() (json.RawMessage, error) {
	select {
	case b := <-t.responses:
		return b, nil
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *mockTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (s *mockServer) dispatch(method string, params json.RawMessage) any {
	switch method {
	case methodInitialize:
		return initializeResult{
			ProtocolVersion: protocolVersion,
			ServerInfo:      implementation{Name: "mock-mcp", Version: "1.2.3"},
		}
	case methodToolsList:
		return listToolsResult{Tools: []toolDescriptor{
			{Name: "echo", Description: "Echo back the message", InputSchema: json.RawMessage(`{"type":"object","properties":{"msg":{"type":"string"}}}`)},
			{Name: "boom", Description: "Always fails"},
		}}
	case methodToolsCall:
		var p callToolParams
		_ = json.Unmarshal(params, &p)
		s.mu.Lock()
		s.called = append(s.called, p.Name)
		s.mu.Unlock()
		if p.Name == "boom" {
			return callToolResult{Content: []contentBlock{{Type: "text", Text: "kaboom"}}, IsError: true}
		}
		return callToolResult{Content: []contentBlock{{Type: "text", Text: "echo:" + string(p.Arguments)}}}
	default:
		return struct{}{}
	}
}

func mustConnect(t *testing.T, srv *mockServer, opts ...Option) *Client {
	t.Helper()
	c, err := Connect(context.Background(), srv, opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func partsText(parts []core.Part) string {
	var s string
	for _, p := range parts {
		if txt, ok := p.(core.Text); ok {
			s += txt.Text
		}
	}
	return s
}

func TestConnectListsToolsAndServerInfo(t *testing.T) {
	c := mustConnect(t, &mockServer{})

	if name, ver := c.ServerInfo(); name != "mock-mcp" || ver != "1.2.3" {
		t.Fatalf("ServerInfo = %q %q", name, ver)
	}
	tools := c.Tools()
	if len(tools) != 2 {
		t.Fatalf("got %d tools, want 2", len(tools))
	}
	if tools[0].Name() != "echo" || tools[1].Name() != "boom" {
		t.Fatalf("tool names = %q, %q", tools[0].Name(), tools[1].Name())
	}
	// inputSchema is passed through verbatim.
	if got := string(tools[0].Schema()); got != `{"type":"object","properties":{"msg":{"type":"string"}}}` {
		t.Fatalf("echo schema = %s", got)
	}
	// Missing inputSchema gets a minimal object default.
	if got := string(tools[1].Schema()); got != `{"type":"object"}` {
		t.Fatalf("boom schema = %s", got)
	}
}

func TestCallToolRoundTrip(t *testing.T) {
	c := mustConnect(t, &mockServer{})
	echo := c.Tools()[0]

	res, err := echo.Call(&tool.Context{Context: context.Background()}, json.RawMessage(`{"msg":"hi"}`))
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if res.IsError {
		t.Fatal("unexpected IsError")
	}
	if got := partsText(res.Content); got != `echo:{"msg":"hi"}` {
		t.Fatalf("result = %q", got)
	}
}

func TestCallToolErrorMapsToIsError(t *testing.T) {
	c := mustConnect(t, &mockServer{})
	boom := c.Tools()[1]

	res, err := boom.Call(&tool.Context{Context: context.Background()}, nil)
	if err != nil {
		t.Fatalf("Call returned Go error: %v", err)
	}
	if !res.IsError {
		t.Fatal("want IsError=true")
	}
	if got := partsText(res.Content); got != "kaboom" {
		t.Fatalf("result = %q", got)
	}
}

func TestToolPrefixStrippedBeforeServerCall(t *testing.T) {
	srv := &mockServer{}
	c := mustConnect(t, srv, WithToolPrefix("fs_"))

	tools := c.Tools()
	if tools[0].Name() != "fs_echo" {
		t.Fatalf("prefixed name = %q", tools[0].Name())
	}
	if _, err := tools[0].Call(&tool.Context{Context: context.Background()}, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Call: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.called) != 1 || srv.called[0] != "echo" {
		t.Fatalf("server saw %v, want [echo] (prefix stripped)", srv.called)
	}
}

func TestConcurrentCalls(t *testing.T) {
	c := mustConnect(t, &mockServer{})
	echo := c.Tools()[0]

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = echo.Call(&tool.Context{Context: context.Background()}, json.RawMessage(`{"msg":"x"}`))
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
}

func TestCallAfterCloseReportsError(t *testing.T) {
	srv := &mockServer{}
	c, err := Connect(context.Background(), srv)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	echo := c.Tools()[0]
	c.Close()

	res, err := echo.Call(&tool.Context{Context: context.Background()}, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Call returned Go error: %v", err)
	}
	if !res.IsError {
		t.Fatal("want IsError after Close")
	}
}
