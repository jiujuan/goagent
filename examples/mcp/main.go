// Command mcp is a tutorial for the MCP (Model Context Protocol) client: connect
// to an external MCP server, discover its tools, and adapt each into a
// goagent tool.Tool — zero changes to the agent engine. The discovered tools
// mount straight into agent.New(WithTools(...)).
//
// MCP needs an external server. Pass its launch command, or rely on the default
// (npx filesystem server, which needs Node/npx):
//
//	go run ./examples/mcp                                                  # default: filesystem server
//	go run ./examples/mcp npx -y @modelcontextprotocol/server-filesystem /data
//	go run ./examples/mcp <your-mcp-server-command> [args...]
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jiujuan/goagent/tool/mcp"
)

func main() {
	args := os.Args[1:]
	var server *mcp.StdioServer
	if len(args) > 0 {
		server = mcp.Stdio(args[0], args[1:]...)
	} else {
		dir, _ := os.MkdirTemp("", "mcp-demo-*")
		defer os.RemoveAll(dir)
		fmt.Println("(未指定命令,默认连接 filesystem MCP server,根目录:", dir, ")")
		server = mcp.Stdio("npx", "-y", "@modelcontextprotocol/server-filesystem", dir)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	// Connect: spawn the server, handshake, list its tools (each prefixed).
	client, err := mcp.Connect(ctx, server, mcp.WithToolPrefix("mcp_"), mcp.WithTimeout(30*time.Second))
	if err != nil {
		fmt.Println("连接 MCP server 失败:", err)
		fmt.Println("用法: go run ./examples/mcp <command> [args...]")
		fmt.Println("例:   go run ./examples/mcp npx -y @modelcontextprotocol/server-filesystem /data")
		return
	}
	defer client.Close()

	name, ver := client.ServerInfo()
	fmt.Printf("✅ 已连接 MCP server: %s %s\n", name, ver)

	tools := client.Tools()
	fmt.Printf("🔧 发现 %d 个工具(已适配成 tool.Tool):\n", len(tools))
	for _, t := range tools {
		fmt.Printf("  - %s: %s\n", t.Name(), firstLine(t.Description()))
	}

	fmt.Println("\n挂到 agent(零侵入):")
	fmt.Println("  a, _ := agent.New(agent.WithModel(model), agent.WithTools(client.Tools()...))")
}

func firstLine(s string) string {
	for i, r := range s {
		if r == '\n' {
			return s[:i] + " …"
		}
	}
	return s
}
