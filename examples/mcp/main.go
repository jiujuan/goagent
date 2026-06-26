// Command mcp is a tutorial for the tool/mcp package: connecting a goagent
// Agent to an external MCP (Model Context Protocol) server and letting the model
// call the server's tools.
//
// It is fully DETERMINISTIC and needs no network or external server: an
// in-process mock MCP server (mockMCP) implements the mcp.Server/Transport
// interfaces and answers the standard handshake plus a single "add" tool. A
// scripted mock model drives the agent to call it.
//
//	go run ./examples/mcp
//
// To point at a REAL MCP server instead, replace the mcp.Connect line with a
// stdio launch (requires Node / npx and network):
//
//	client, err := mcp.Connect(ctx,
//	    mcp.Stdio("npx", "-y", "@modelcontextprotocol/server-everything"))
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool/mcp"
)

func main() {
	ctx := context.Background()
	fmt.Println("=== tool/mcp 教程：连接 MCP 服务器，扩展 Agent 工具 ===")

	// 1. 连接到一个 MCP 服务器。这里用进程内 mock，离线可跑；换成
	//    mcp.Stdio("npx", "-y", "...") 即可连真实的本地服务器。
	client, err := mcp.Connect(ctx, &mockMCP{},
		mcp.WithClientInfo("goagent-example", "0.1"),
		mcp.WithToolPrefix("calc_"), // 命名空间，避免与其他服务器/本地工具重名
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	name, ver := client.ServerInfo()
	fmt.Printf("已连接 MCP 服务器：%s %s\n", name, ver)
	for _, t := range client.Tools() {
		fmt.Printf("  · 工具 %s — %s\n", t.Name(), t.Description())
	}

	// 2. 一个脚本化 mock 模型：先请求调用 calc_add，拿到结果后给出最终回答。
	model := mock.New("mock-opus", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("算出来了，3 + 4 = " + partsText(tr.Content) + "。")
		}
		return mock.CallTool("call_1", "calc_add", `{"a":3,"b":4}`)
	})

	// 3. 把 MCP 工具直接挂进 agent —— 无需改动 agent 引擎。
	assistant := agent.New(agent.Config{
		Name:        "assistant",
		Description: "会用 MCP 工具的助手",
		Model:       model,
		Instruction: "需要计算时调用工具。",
		Tools:       client.Tools(),
	})

	// 4. 跑一轮，打印事件流。
	r := runner.New(runner.Config{Root: assistant})
	fmt.Println("\n--- 运行 ---")
	for ev, err := range r.Run(ctx, "user-1", "session-1", core.UserText("帮我算 3 加 4")) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}
}

// --- 进程内 mock MCP 服务器 -------------------------------------------------
//
// 实现 mcp.Server（Open）与 mcp.Transport（Send/Receive/Close），用 JSON-RPC
// 2.0 回应 initialize / tools/list / tools/call。真实服务器走的是同一套协议，
// 只是承载在子进程的 stdio 上。

type mockMCP struct{}

func (*mockMCP) Open(_ context.Context) (mcp.Transport, error) {
	return &mockConn{out: make(chan json.RawMessage, 8), done: make(chan struct{})}, nil
}

type mockConn struct {
	out  chan json.RawMessage
	once sync.Once
	done chan struct{}
}

func (c *mockConn) Send(msg json.RawMessage) error {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(msg, &req); err != nil {
		return err
	}
	if len(req.ID) == 0 {
		return nil // 通知无需回应
	}
	resp, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"result":  c.handle(req.Method, req.Params),
	})
	select {
	case c.out <- resp:
	case <-c.done:
	}
	return nil
}

func (c *mockConn) handle(method string, params json.RawMessage) any {
	switch method {
	case "initialize":
		return map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"serverInfo":      map[string]any{"name": "mock-calculator", "version": "0.0.1"},
		}
	case "tools/list":
		return map[string]any{"tools": []map[string]any{{
			"name":        "add",
			"description": "把两个整数相加",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"a": map[string]any{"type": "integer"},
					"b": map[string]any{"type": "integer"},
				},
				"required": []string{"a", "b"},
			},
		}}}
	case "tools/call":
		var p struct {
			Name      string `json:"name"`
			Arguments struct{ A, B int } `json:"arguments"`
		}
		_ = json.Unmarshal(params, &p)
		return map[string]any{"content": []map[string]any{{
			"type": "text",
			"text": fmt.Sprintf("%d", p.Arguments.A+p.Arguments.B),
		}}}
	default:
		return map[string]any{}
	}
}

func (c *mockConn) Receive() (json.RawMessage, error) {
	select {
	case b := <-c.out:
		return b, nil
	case <-c.done:
		return nil, io.EOF
	}
}

func (c *mockConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

// --- 打印辅助（与其他示例一致） --------------------------------------------

func printEvent(ev *core.Event) {
	if ev == nil || ev.Message == nil {
		return
	}
	switch ev.Message.Role {
	case core.RoleUser:
		fmt.Printf("👤 user:      %s\n", ev.Message.Text())
	case core.RoleAssistant:
		if calls := ev.Message.ToolCalls(); len(calls) > 0 {
			for _, c := range calls {
				fmt.Printf("🤖 assistant: →调用 MCP 工具 %s(%s)\n", c.Name, string(c.Args))
			}
			return
		}
		fmt.Printf("🤖 assistant: %s\n", ev.Message.Text())
	case core.RoleTool:
		for _, p := range ev.Message.Parts {
			if tr, ok := p.(core.ToolResult); ok {
				fmt.Printf("🔧 tool:      %s -> %s\n", tr.Name, partsText(tr.Content))
			}
		}
	}
}

func partsText(parts []core.Part) string {
	var s string
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			s += t.Text
		}
	}
	return s
}
