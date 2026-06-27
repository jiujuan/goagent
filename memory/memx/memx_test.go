package memx

import (
	"context"
	"testing"

	"github.com/jiujuan/goagent/core"
	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/memory/textmem"
)

func TestConsolidateWritesToStores(t *testing.T) {
	ctx := context.Background()
	// The consolidator extracts one text fact and one semantic fact.
	model := mock.New("m", func(*llm.Request) *llm.Response {
		return mock.Text(`[
			{"target":"text","name":"pref","desc":"用户偏好","type":"user","content":"用户喜欢简洁回答"},
			{"target":"semantic","name":"","desc":"","type":"reference","content":"巴黎是法国首都"}
		]`)
	})
	textStore, err := textmem.File(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sem := memory.InMemory(embmock.New())

	msgs := []core.Message{core.UserText("我喜欢简洁"), core.AssistantText("好的")}
	if err := Consolidate(ctx, model, msgs, textStore, sem); err != nil {
		t.Fatal(err)
	}

	if sem.Len() != 1 {
		t.Fatalf("semantic store len = %d, want 1", sem.Len())
	}
	e, err := textStore.Read(ctx, "pref")
	if err != nil {
		t.Fatalf("text entry not saved: %v", err)
	}
	if e.Body == "" {
		t.Fatalf("text entry body empty: %+v", e)
	}
}

func TestConsolidateEmptyTranscriptNoop(t *testing.T) {
	sem := memory.InMemory(embmock.New())
	model := mock.New("m", func(*llm.Request) *llm.Response { return mock.Text("[]") })
	if err := Consolidate(context.Background(), model, nil, nil, sem); err != nil {
		t.Fatal(err)
	}
	if sem.Len() != 0 {
		t.Fatalf("empty transcript should write nothing, len=%d", sem.Len())
	}
}

func TestNewAssemblesLayers(t *testing.T) {
	sem := memory.InMemory(embmock.New())
	m, err := New(Config{
		EnableWorkingMemory: true,
		TextMemDir:          t.TempDir(),
		Semantic:            sem,
		RAG:                 &RAGConfig{K: 3},
		EnableSearchTool:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Sections) == 0 || len(m.Middleware) == 0 || len(m.Tools) == 0 {
		t.Fatalf("New did not assemble layers: sections=%d mw=%d tools=%d",
			len(m.Sections), len(m.Middleware), len(m.Tools))
	}
	if m.TextStore == nil {
		t.Fatal("TextStore should be constructed")
	}
}
