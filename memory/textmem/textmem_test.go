package textmem

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/tool"
)

func TestSaveReadIndexRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, err := File(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Save(ctx, Entry{Name: "db-choice", Desc: "用 postgres", Type: "project", Body: "项目统一使用 PostgreSQL。"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, Entry{Name: "tone", Desc: "简洁中文", Type: "user", Body: "回复用简洁中文。"}); err != nil {
		t.Fatal(err)
	}

	got, err := store.Read(ctx, "db-choice")
	if err != nil {
		t.Fatal(err)
	}
	if got.Desc != "用 postgres" || got.Type != "project" || !strings.Contains(got.Body, "PostgreSQL") {
		t.Errorf("read = %+v", got)
	}

	idx, err := store.Index(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 2 {
		t.Fatalf("index len = %d, want 2", len(idx))
	}
	// Sorted by name: db-choice before tone.
	if idx[0].Name != "db-choice" || idx[1].Name != "tone" {
		t.Errorf("index order = %s, %s", idx[0].Name, idx[1].Name)
	}
	if idx[0].Body != "" {
		t.Errorf("index entries must omit body, got %q", idx[0].Body)
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	store, _ := File(t.TempDir())
	_ = store.Save(ctx, Entry{Name: "x", Desc: "d", Body: "b"})
	if err := store.Delete(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	idx, _ := store.Index(ctx)
	if len(idx) != 0 {
		t.Errorf("after delete index len = %d", len(idx))
	}
}

func TestIndexSection(t *testing.T) {
	ctx := context.Background()
	store, _ := File(t.TempDir())

	// Empty store -> empty section.
	out, err := IndexSection(store).Render(prompt.Context{Context: ctx})
	if err != nil {
		t.Fatal(err)
	}
	if out != "" {
		t.Errorf("empty store should render empty, got %q", out)
	}

	_ = store.Save(ctx, Entry{Name: "tone", Desc: "简洁中文", Body: "..."})
	out, err = IndexSection(store).Render(prompt.Context{Context: ctx})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "tone — 简洁中文") {
		t.Errorf("section missing entry:\n%s", out)
	}
}

func TestSaveAndReadTools(t *testing.T) {
	ctx := context.Background()
	store, _ := File(t.TempDir())

	st := &tool.Context{Context: ctx}
	if _, err := SaveTool(store).Call(st, []byte(`{"name":"foo","description":"d","type":"user","body":"hello body"}`)); err != nil {
		t.Fatal(err)
	}
	res, err := ReadTool(store).Call(st, []byte(`{"name":"foo"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := partsText(res.Content); !strings.Contains(got, "hello body") {
		t.Errorf("read tool returned %q", got)
	}
}

func partsText(parts []core.Part) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}
