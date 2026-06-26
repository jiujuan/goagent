package memx

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/llm"
	llmmock "github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/memory"
	"github.com/jiujuan/goagent/session"
)

func TestNewAssemblesSelectedLayers(t *testing.T) {
	rulesDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(rulesDir, "tone.md"), []byte("简洁"), 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := New(Config{
		GlobalRulesDir:      rulesDir,
		EnableWorkingMemory: true,
		TextMemDir:          t.TempDir(),
		Semantic:            memory.InMemory(mock.New()),
		RAG:                 &memory.RAGOptions{},
		EnableSearchTool:    true,
		SectionBudget:       2000,
	})
	if err != nil {
		t.Fatal(err)
	}

	// rules + working memory + text index = 3 sections.
	if len(m.Sections) != 3 {
		t.Errorf("sections = %d, want 3", len(m.Sections))
	}
	// working update + text save + text read + search = 4 tools.
	if len(m.Tools) != 4 {
		t.Errorf("tools = %d, want 4", len(m.Tools))
	}
	// RAG = 1 middleware.
	if len(m.Middleware) != 1 {
		t.Errorf("middleware = %d, want 1", len(m.Middleware))
	}
	if m.TextStore == nil {
		t.Error("TextStore should be set")
	}
}

func TestConsolidateWritesAndDedups(t *testing.T) {
	ctx := context.Background()

	// A session with some content to consolidate.
	store := session.InMemory()
	s, _ := store.GetOrCreate(ctx, "app", "u", "sess")
	msg := core.UserText("我们项目用 PostgreSQL，我喜欢简洁的回复。")
	_ = store.Append(ctx, s, &core.Event{Message: &msg})

	// Model returns a fixed extraction with a duplicate semantic item.
	const out = `[
{"target":"text","name":"db","desc":"用 pg","type":"project","content":"项目使用 PostgreSQL"},
{"target":"semantic","content":"用户偏好简洁回复"},
{"target":"semantic","content":"用户偏好简洁回复"}
]`
	model := llmmock.New("m", func(*llm.Request) *llm.Response { return llmmock.Text(out) })

	textStore, _ := New(Config{TextMemDir: t.TempDir()})
	sem := memory.InMemory(mock.New())

	if err := Consolidate(ctx, model, s, textStore.TextStore, sem); err != nil {
		t.Fatal(err)
	}

	idx, _ := textStore.TextStore.Index(ctx)
	if len(idx) != 1 || idx[0].Name != "db" {
		t.Errorf("text store = %+v, want 1 entry 'db'", idx)
	}
	if sem.Len() != 1 {
		t.Errorf("semantic len = %d, want 1 (duplicate deduped)", sem.Len())
	}
}

func TestConsolidateEmptyTranscriptNoop(t *testing.T) {
	ctx := context.Background()
	store := session.InMemory()
	s, _ := store.GetOrCreate(ctx, "app", "u", "sess")
	model := llmmock.New("m", func(*llm.Request) *llm.Response { return llmmock.Text("[]") })

	sem := memory.InMemory(mock.New())
	if err := Consolidate(ctx, model, s, nil, sem); err != nil {
		t.Fatal(err)
	}
	if sem.Len() != 0 {
		t.Errorf("nothing should be written for empty transcript")
	}
}
