// Command memory-layers shows the layered memory system assembled by memx.New
// and mounted on an agent: working memory (a scratchpad that survives
// compaction, with an update tool + prompt section) plus semantic memory (a
// vector store wired as auto-RAG). Uses the mock provider, so it runs offline.
//
//	go run ./examples/memory-layers
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/memory/memx"
	"github.com/jiujuan/goagent/prompt"
)

func main() {
	ctx := context.Background()

	// A semantic store with some background knowledge.
	sem := memory.InMemory(embmock.New())
	if err := sem.Add(ctx,
		memory.Doc("向量数据库用近似最近邻(ANN)检索来做语义搜索。"),
		memory.Doc("Go 适合写高并发的网络服务。"),
	); err != nil {
		log.Fatal(err)
	}

	// Assemble the layers: working memory (section + tool) + semantic RAG.
	m, err := memx.New(memx.Config{
		EnableWorkingMemory: true,
		Semantic:            sem,
		RAG:                 &memx.RAGConfig{K: 2},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Build the system prompt from Identity + the memory sections.
	b := prompt.New().Add(prompt.Identity("你是一个研究助手。确定目标或关键约束时,用 update_working_memory 记下来。"))
	for _, s := range m.Sections {
		b.Add(s)
	}

	// mock: turn 0 records working memory; turn 1 answers (RAG already injected
	// the relevant doc into the system prompt).
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("已记下目标与待办。据资料:向量数据库用近似最近邻(ANN)做语义检索。")
		}
		return mock.CallTool("c1", "update_working_memory",
			`{"goal":"了解向量数据库","add_todo":"读检索原理"}`)
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithPrompt(b),
		agent.WithMiddleware(m.Middleware...), // 语义 RAG
		agent.WithTools(m.Tools...),           // update_working_memory
	)
	if err != nil {
		log.Fatal(err)
	}

	for ev, err := range a.Stream(ctx, "向量数据库是怎么做语义检索的?").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			fmt.Println("🔧", e.Result.Name, "->", e.Result.Content[0].(core.Text).Text)
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("🤖", t)
			}
		}
	}
}
