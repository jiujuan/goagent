package memory_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/agent"
	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/middleware"
)

func newStore(t *testing.T) memory.Store {
	t.Helper()
	ctx := context.Background()
	s := memory.InMemory(embmock.New())
	if err := s.Add(ctx,
		memory.Doc("巴黎是法国的首都,塞纳河穿城而过。"),
		memory.Doc("Go 是一门由 Google 设计的编译型语言。"),
		memory.Doc("光合作用让植物把阳光转成能量。"),
	); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStoreSearchRanks(t *testing.T) {
	s := newStore(t)
	docs, err := s.Search(context.Background(), "法国的首都是哪", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || !strings.Contains(docs[0].Content, "巴黎") {
		t.Fatalf("top result = %+v, want the Paris doc", docs)
	}
}

func TestRetrieverImplementsRAG(t *testing.T) {
	// memory.NewRetriever must satisfy middleware.Retriever structurally and feed
	// RAG: the model's request system prompt should carry the relevant doc.
	var sawParis bool
	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if strings.Contains(req.System, "巴黎") {
			sawParis = true
		}
		return mock.Text("ok")
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithMiddleware(middleware.RAG(middleware.RAGOptions{
			Retriever: memory.NewRetriever(newStore(t), 0),
			K:         1,
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(context.Background(), "法国的首都"); err != nil {
		t.Fatal(err)
	}
	if !sawParis {
		t.Fatal("RAG did not inject the relevant memory into the system prompt")
	}
}
