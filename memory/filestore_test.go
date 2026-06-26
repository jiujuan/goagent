package memory

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/embeddings/mock"
)

func TestFileStorePersistsAcrossReload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	emb := mock.New()

	store, err := File(dir, emb)
	if err != nil {
		t.Fatal(err)
	}
	docs := []Document{
		Doc("PostgreSQL 是项目使用的数据库"),
		Doc("缓存层使用 Redis"),
		Doc("前端框架是 React"),
	}
	if err := store.Add(ctx, docs...); err != nil {
		t.Fatal(err)
	}
	if store.Len() != 3 {
		t.Fatalf("len = %d, want 3", store.Len())
	}

	// Reopen from disk with a fresh store: records must reload.
	store2, err := File(dir, emb)
	if err != nil {
		t.Fatal(err)
	}
	if store2.Len() != 3 {
		t.Fatalf("after reload len = %d, want 3", store2.Len())
	}

	got, err := store2.Search(ctx, "数据库用什么", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !strings.Contains(got[0].Content, "PostgreSQL") {
		t.Errorf("search after reload = %+v", got)
	}
}

func TestFileStoreMetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := File(dir, mock.New())

	if err := store.Add(ctx, DocWithMeta("内容", map[string]any{"src": "manual"})); err != nil {
		t.Fatal(err)
	}
	store2, _ := File(dir, mock.New())
	got, _ := store2.Search(ctx, "内容", 1)
	if len(got) != 1 || got[0].Metadata["src"] != "manual" {
		t.Errorf("metadata not persisted: %+v", got)
	}
}

// Ensure FileStore satisfies the same Store contract used by RAG/SearchTool.
func TestFileStoreIsStore(t *testing.T) {
	var _ Store = (*FileStore)(nil)
}
