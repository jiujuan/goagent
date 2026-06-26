package memory_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

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

func newStore(t *testing.T) memory.Store {
	t.Helper()
	store := memory.InMemory(embmock.New())
	err := store.Add(context.Background(),
		memory.Doc("巴黎是法国的首都，以埃菲尔铁塔闻名。"),
		memory.Doc("苹果公司由史蒂夫·乔布斯创立。"),
		memory.Doc("长城位于中国北方，是世界文化遗产。"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestSearchRanking(t *testing.T) {
	store := newStore(t)
	docs, err := store.Search(context.Background(), "法国的首都是哪里", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("got %d docs, want 2", len(docs))
	}
	if !strings.Contains(docs[0].Content, "巴黎") {
		t.Fatalf("top result should be about Paris, got %q (score %.3f)", docs[0].Content, docs[0].Score)
	}
	if docs[0].Score <= docs[1].Score {
		t.Fatalf("results not ranked by descending score: %.3f <= %.3f", docs[0].Score, docs[1].Score)
	}
}

func TestSearchTool(t *testing.T) {
	store := newStore(t)
	st := memory.SearchTool(store, 1)
	res, err := st.Call(&tool.Context{Context: context.Background()}, json.RawMessage(`{"query":"法国首都"}`))
	if err != nil {
		t.Fatal(err)
	}
	out := res.Content[0].(core.Text).Text
	if !strings.Contains(out, "巴黎") {
		t.Fatalf("search tool output should mention Paris, got %q", out)
	}
}

func TestRAGMiddlewareInjectsContext(t *testing.T) {
	store := newStore(t)

	// Capture the system prompt the model actually receives after the RAG
	// middleware runs.
	var gotSystem string
	base := mock.New("base", func(req *llm.Request) *llm.Response {
		gotSystem = req.System
		return mock.Text("ok")
	})
	model := memory.NewRAG(store, &memory.RAGOptions{K: 1})(base)

	req := &llm.Request{Messages: []core.Message{core.UserText("法国的首都是哪里？")}}
	for _, err := range model.Generate(context.Background(), req) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if !strings.Contains(gotSystem, "巴黎") {
		t.Fatalf("RAG should inject Paris context into system prompt, got %q", gotSystem)
	}
}

// TestRAGEndToEnd wires the RAG middleware into an agent and verifies the
// retrieved context reaches the model (the mock echoes its system prompt).
func TestRAGEndToEnd(t *testing.T) {
	store := newStore(t)
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		// Prove the injected context arrived by answering from it.
		if strings.Contains(req.System, "巴黎") {
			return mock.Text("法国的首都是巴黎。")
		}
		return mock.Text("我不知道。")
	})
	ag := agent.New(agent.Config{
		Name:       "rag-bot",
		Model:      model,
		Middleware: []middleware.Middleware{memory.NewRAG(store, &memory.RAGOptions{K: 2})},
	})
	r := runner.New(runner.Config{Root: ag})

	var final string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("法国的首都是哪里？")) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.IsFinalResponse() {
			final = ev.Message.Text()
		}
	}
	if !strings.Contains(final, "巴黎") {
		t.Fatalf("agent should answer using retrieved context, got %q", final)
	}
}
