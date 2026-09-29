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

// Two runs that extract the same facts must not grow either store.
func TestConsolidateIsIdempotentAcrossRuns(t *testing.T) {
	ctx := context.Background()
	model := mock.New("m", func(*llm.Request) *llm.Response {
		return mock.Text(`[
			{"target":"text","name":"tone","desc":"语气偏好","type":"user","content":"回复要简洁"},
			{"target":"semantic","type":"reference","content":"巴黎是法国首都"}
		]`)
	})
	text, err := textmem.File(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sem := memory.InMemory(embmock.New())

	msgs := []core.Message{core.UserText("请简洁点"), core.AssistantText("好的")}
	for i := 0; i < 2; i++ {
		if err := Consolidate(ctx, model, msgs, text, sem); err != nil {
			t.Fatal(err)
		}
	}
	if sem.Len() != 1 {
		t.Fatalf("semantic store grew on the second run: Len = %d, want 1", sem.Len())
	}
	idx, _ := text.Index(ctx)
	if len(idx) != 1 {
		t.Fatalf("text store grew on the second run: %d entries, want 1", len(idx))
	}
}

// The same fact reappearing under another name is still one entry.
func TestConsolidateSkipsRenewedDuplicate(t *testing.T) {
	ctx := context.Background()
	calls := 0
	model := mock.New("m", func(*llm.Request) *llm.Response {
		defer func() { calls++ }()
		if calls == 0 {
			return mock.Text(`[{"target":"text","name":"tone","desc":"语气","type":"user","content":"回复要简洁"}]`)
		}
		return mock.Text(`[{"target":"text","name":"reply-style","desc":"回答风格","type":"user","content":"回复要简洁"}]`)
	})
	text, _ := textmem.File(t.TempDir())
	msgs := []core.Message{core.UserText("请简洁点")}

	if err := Consolidate(ctx, model, msgs, text, nil); err != nil {
		t.Fatal(err)
	}
	if err := Consolidate(ctx, model, msgs, text, nil); err != nil {
		t.Fatal(err)
	}
	idx, _ := text.Index(ctx)
	if len(idx) != 1 {
		t.Fatalf("renamed duplicate stored twice: %+v", idx)
	}
}

// A known name with new content is the update path: the entry is replaced, not
// duplicated.
func TestConsolidateUpdatesChangedPreference(t *testing.T) {
	ctx := context.Background()
	calls := 0
	model := mock.New("m", func(*llm.Request) *llm.Response {
		defer func() { calls++ }()
		if calls == 0 {
			return mock.Text(`[{"target":"text","name":"tone","desc":"语气","type":"user","content":"回复要简洁"}]`)
		}
		return mock.Text(`[{"target":"text","name":"tone","desc":"语气","type":"user","content":"回复要详细展开"}]`)
	})
	text, _ := textmem.File(t.TempDir())
	msgs := []core.Message{core.UserText("请简洁点")}

	if err := Consolidate(ctx, model, msgs, text, nil); err != nil {
		t.Fatal(err)
	}
	if err := Consolidate(ctx, model, msgs, text, nil); err != nil {
		t.Fatal(err)
	}
	idx, _ := text.Index(ctx)
	if len(idx) != 1 {
		t.Fatalf("updated preference duplicated the entry: %+v", idx)
	}
	e, err := text.Read(ctx, "tone")
	if err != nil {
		t.Fatal(err)
	}
	if e.Body != "回复要详细展开" {
		t.Fatalf("body = %q, want the new preference", e.Body)
	}
}

// A store that cannot dedup still gets the facts (Add fallback).
type plainStore struct{ docs []memory.Document }

func (s *plainStore) Add(_ context.Context, docs ...memory.Document) error {
	s.docs = append(s.docs, docs...)
	return nil
}

func (s *plainStore) Search(context.Context, string, int) ([]memory.Document, error) {
	return s.docs, nil
}

func TestConsolidateFallsBackToAdd(t *testing.T) {
	sem := &plainStore{}
	model := mock.New("m", func(*llm.Request) *llm.Response {
		return mock.Text(`[{"target":"semantic","type":"reference","content":"巴黎是法国首都"}]`)
	})
	if err := Consolidate(context.Background(), model,
		[]core.Message{core.UserText("法国首都是什么")}, nil, sem); err != nil {
		t.Fatal(err)
	}
	if len(sem.docs) != 1 {
		t.Fatalf("plain store received %d docs, want 1", len(sem.docs))
	}
}
