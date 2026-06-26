// Command memory-layers demonstrates the layered memory system (ADR 0016)
// assembled with the memx facade and run network-free (mock model + mock
// embedder, no API key):
//
//   - rules (global)        — always-on constraints, highest priority
//   - project memory        — AGENTS.md discovered from the working tree
//   - working memory        — cross-turn scratchpad in session State
//   - text memory           — curated Markdown facts + injected index
//   - semantic memory (RAG) — persistent vector store, auto-injected
//
// It runs one turn (showing which memory layers reached the system prompt),
// then consolidates a conversation into long-term memory and reopens the stores
// from disk to prove persistence.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/memory/memx"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

func main() {
	ctx := context.Background()
	root := setupWorkspace()
	defer os.RemoveAll(root)

	// Persistent semantic store, seeded with one background fact.
	sem, err := memory.File(filepath.Join(root, "semantic"), embmock.New())
	if err != nil {
		log.Fatal(err)
	}
	_ = sem.Add(ctx, memory.Doc("goagent 的会话默认线性 append-only，支持 JSONL 文件后端落盘与恢复。"))

	// Assemble all five layers behind one facade.
	mem, err := memx.New(memx.Config{
		GlobalRulesDir:      filepath.Join(root, "rules"),
		ProjectRoot:         filepath.Join(root, "repo"),
		EnableWorkingMemory: true,
		TextMemDir:          filepath.Join(root, "text"),
		Semantic:            sem,
		RAG:                 &memory.RAGOptions{K: 2},
		SectionBudget:       1500,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Build the system prompt: persona + every memory section (ordered by Order).
	pb := prompt.New().Add(prompt.Identity("你是 goagent 助手。"))
	for _, s := range mem.Sections {
		pb.Add(s)
	}

	// A mock model that reports which memory layers reached its system prompt.
	model := mock.New("demo", func(req *llm.Request) *llm.Response {
		markers := []struct{ label, marker string }{
			{"规则", "# 规则"}, {"项目记忆", "# 项目记忆"}, {"语义RAG", "JSONL"},
		}
		var hits []string
		for _, m := range markers {
			if strings.Contains(req.System, m.marker) {
				hits = append(hits, m.label)
			}
		}
		return mock.Text("（system 命中的记忆层：" + strings.Join(hits, "、") + "）")
	})

	bot := agent.New(agent.Config{
		Name:       "memory-bot",
		Model:      model,
		Prompt:     pb,
		Tools:      mem.Tools,
		Middleware: mem.Middleware,
	})

	fmt.Println("=== 第一次会话（五层记忆已装配）===")
	r := runner.New(runner.Config{Root: bot})
	for ev, err := range r.Run(ctx, "u", "s1", core.UserText("goagent 的会话是怎么持久化的？")) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.Message != nil && ev.Message.Role == core.RoleAssistant {
			fmt.Printf("🤖 %s\n", ev.Message.Text())
		}
	}

	// --- Consolidation: distill a conversation into long-term memory ---
	fmt.Println("\n=== 固化写回 ===")
	consModel := mock.New("consolidator", func(*llm.Request) *llm.Response {
		return mock.Text(`[{"target":"text","name":"persistence","desc":"会话用 JSONL 落盘","type":"reference","content":"goagent 会话以 JSONL append-only 持久化。"}]`)
	})
	if err := memx.Consolidate(ctx, consModel, transcriptSession(ctx, "goagent 用 JSONL 持久化会话。"), mem.TextStore, sem); err != nil {
		log.Fatal(err)
	}
	fmt.Println("已抽取要点并写入文本/语义记忆。")

	// --- Persistence: reopen stores from disk ---
	fmt.Println("\n=== 重新打开存储，验证持久化 ===")
	idx, _ := mem.TextStore.Index(ctx)
	fmt.Printf("文本记忆条目数: %d\n", len(idx))
	for _, e := range idx {
		fmt.Printf("  - %s — %s\n", e.Name, e.Desc)
	}
	reopened, _ := memory.File(filepath.Join(root, "semantic"), embmock.New())
	if hits, _ := reopened.Search(ctx, "持久化", 1); len(hits) > 0 {
		fmt.Printf("语义检索命中: %s\n", hits[0].Content)
	}
}

// setupWorkspace creates a temp tree with a rule file and an AGENTS.md.
func setupWorkspace() string {
	root, err := os.MkdirTemp("", "goagent-memory-layers-*")
	if err != nil {
		log.Fatal(err)
	}
	writeFile(filepath.Join(root, "rules", "tone.md"), "回复保持简洁、使用中文。")
	writeFile(filepath.Join(root, "repo", ".git"), "") // repo boundary marker
	writeFile(filepath.Join(root, "repo", "AGENTS.md"), "# 项目约定\n本项目是 goagent 框架，零外部依赖。")
	return root
}

func writeFile(path, content string) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		log.Fatal(err)
	}
}

// transcriptSession builds an in-memory session holding one user message, to be
// fed to Consolidate as the transcript.
func transcriptSession(ctx context.Context, text string) *session.Session {
	st := session.InMemory()
	s, _ := st.GetOrCreate(ctx, "app", "u", "consolidate")
	msg := core.UserText(text)
	_ = st.Append(ctx, s, &core.Event{Message: &msg})
	return s
}
