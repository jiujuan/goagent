// Command rag demonstrates retrieval-augmented generation two ways over the
// same in-memory knowledge base, using the network-free mock embedder and mock
// model (no API key required):
//
//   - automatic RAG: the memory.NewRAG middleware injects relevant docs into the
//     system prompt before each model call.
//   - LLM-driven retrieval: the model calls the search_memory tool when it
//     decides it needs facts.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
)

func buildKB() memory.Store {
	store := memory.InMemory(embmock.New())
	_ = store.Add(context.Background(),
		memory.Doc("goagent 的核心流式原语是 iter.Seq2[*Event, error]，贯穿 runner、agent 与 turn 引擎。"),
		memory.Doc("goagent 的事件Event 建模为 Event.Actions，由 runner 在提交事件时事务性应用。"),
		memory.Doc("goagent 的会话默认线性 append-only，支持 JSONL 文件后端落盘与恢复。"),
		memory.Doc("goagent 的 provider 隔离为子包：anthropic 原生协议，openaicompat 复用于 DeepSeek/agnes。"),
	)
	return store
}

func main() {
	store := buildKB()
	ctx := context.Background()

	// --- 1. 自动 RAG（middleware）---
	fmt.Println("=== 自动 RAG（middleware 注入背景资料）===")
	autoModel := mock.New("auto", func(req *llm.Request) *llm.Response {
		if strings.Contains(req.System, "事件") && strings.Contains(req.System, "Actions") {
			return mock.Text("goagent 把事件Event 建模为 Event.Actions，由 runner 事务性提交。")
		}
		return mock.Text("（未检索到相关资料）")
	})
	autoBot := agent.New(agent.Config{
		Name:       "auto-rag",
		Model:      autoModel,
		Middleware: []middleware.Middleware{memory.NewRAG(store, &memory.RAGOptions{K: 2})},
	})
	runOnce(ctx, autoBot, "goagent 是怎么处理事件Event 的？")

	// --- 2. LLM 主动检索（tool）---
	fmt.Println("\n=== LLM 主动检索（search_memory 工具）===")
	search := memory.SearchTool(store, 2)
	toolModel := mock.New("tool", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("根据知识库：" + firstLine(partsText(tr.Content)))
		}
		return mock.CallTool("c1", "search_memory", `{"query":"流式原语"}`)
	})
	toolBot := agent.New(agent.Config{
		Name:        "tool-rag",
		Model:       toolModel,
		Instruction: "需要事实时调用 search_memory 工具。",
		Tools:       []tool.Tool{search},
	})
	runOnce(ctx, toolBot, "goagent 的流式原语是什么？")
}

func runOnce(ctx context.Context, ag agent.Agent, question string) {
	r := runner.New(runner.Config{Root: ag})
	for ev, err := range r.Run(ctx, "u", "s", core.UserText(question)) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.Message == nil {
			continue
		}
		switch ev.Message.Role {
		case core.RoleUser:
			fmt.Printf("👤 %s\n", ev.Message.Text())
		case core.RoleAssistant:
			if calls := ev.Message.ToolCalls(); len(calls) > 0 {
				fmt.Printf("🤖 →检索 %s\n", string(calls[0].Args))
				continue
			}
			fmt.Printf("🤖 %s\n", ev.Message.Text())
		case core.RoleTool:
			fmt.Printf("🔧 命中知识库\n")
		}
	}
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
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
