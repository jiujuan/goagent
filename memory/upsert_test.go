package memory_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/memory"
)

func upsert(t *testing.T, u memory.Upsertable, docs ...memory.Document) int {
	t.Helper()
	n, err := u.Upsert(context.Background(), docs...)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Upsert is idempotent per fact, even when the caller hands out fresh IDs each
// time (which is what a repeated Consolidate does).
func TestUpsertSkipsKnownFacts(t *testing.T) {
	s := memory.InMemory(embmock.New())
	first := upsert(t, s,
		memory.Doc("巴黎是法国的首都"),
		memory.Doc("Go 是编译型语言"),
	)
	if first != 2 {
		t.Fatalf("first upsert wrote %d, want 2", first)
	}
	again := upsert(t, s,
		memory.Document{ID: "other-id", Content: "巴黎是法国的首都"},
		memory.Doc("Go 是编译型语言"),
	)
	if again != 0 {
		t.Fatalf("second upsert wrote %d, want 0 (same facts, different ids)", again)
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s.Len())
	}
	if n := upsert(t, s, memory.Doc("新事实"), memory.Doc("巴黎是法国的首都")); n != 1 {
		t.Fatalf("mixed upsert wrote %d, want only the new fact", n)
	}
	if s.Len() != 3 {
		t.Fatalf("Len = %d, want 3", s.Len())
	}
}

// Whitespace and letter casing are noise, not a new fact.
func TestUpsertNormalizesContent(t *testing.T) {
	s := memory.InMemory(embmock.New())
	upsert(t, s, memory.Doc("项目统一使用 PostgreSQL"))
	if n := upsert(t, s, memory.Doc("项目统一使用   PostgreSQL\n")); n != 0 {
		t.Fatalf("normalized duplicate wrote %d, want 0", n)
	}
}

// Facts written by Add are known to a later Upsert: Add only appends, it does
// not opt a document out of dedup.
func TestUpsertSeesDocumentsWrittenByAdd(t *testing.T) {
	s := memory.InMemory(embmock.New())
	add(t, s, memory.Doc("巴黎是法国的首都"))
	if n := upsert(t, s, memory.Doc("巴黎是法国的首都")); n != 0 {
		t.Fatalf("upsert wrote %d, want 0", n)
	}
	// Add itself keeps its unconditional append semantics.
	add(t, s, memory.Doc("巴黎是法国的首都"))
	if s.Len() != 2 {
		t.Fatalf("Add changed behaviour: Len = %d, want 2 duplicates", s.Len())
	}
}

// A restarted file store remembers the facts it already holds.
func TestFileStoreUpsertSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	fs, err := memory.File(dir, embmock.New())
	if err != nil {
		t.Fatal(err)
	}
	if n := upsert(t, fs, memory.Doc("巴黎是法国的首都")); n != 1 {
		t.Fatalf("first upsert wrote %d, want 1", n)
	}

	reopened, err := memory.File(dir, embmock.New())
	if err != nil {
		t.Fatal(err)
	}
	if n := upsert(t, reopened, memory.Doc("巴黎是法国的首都")); n != 0 {
		t.Fatalf("after reopen upsert wrote %d, want 0", n)
	}
	if n := upsert(t, reopened, memory.Doc("Go 是编译型语言")); n != 1 {
		t.Fatalf("unseen fact wrote %d, want 1", n)
	}
	if reopened.Len() != 2 {
		t.Fatalf("Len = %d, want 2", reopened.Len())
	}
}

// Records written before content keys existed still block a duplicate Upsert:
// their key is derived when the log is replayed.
func TestFileStoreUpsertDedupsKeylessLegacyRecords(t *testing.T) {
	dir := t.TempDir()
	legacy := `{"id":"old1","content":"巴黎是法国的首都","embedding":[0.1,0.2]}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "memory.jsonl"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}

	fs, err := memory.File(dir, embmock.New())
	if err != nil {
		t.Fatal(err)
	}
	if n := upsert(t, fs, memory.Doc("巴黎是法国的首都")); n != 0 {
		t.Fatalf("keyless legacy record did not block the duplicate: wrote %d", n)
	}
	if fs.Len() != 1 {
		t.Fatalf("Len = %d, want 1", fs.Len())
	}
}

// A deleted fact is forgotten, so the same content can be stored again.
func TestUpsertAfterDeleteWritesAgain(t *testing.T) {
	fs, _ := openFileStore(t)
	upsert(t, fs, memory.Document{ID: "a", Content: "巴黎是法国的首都"})
	if err := fs.Delete(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if n := upsert(t, fs, memory.Doc("巴黎是法国的首都")); n != 1 {
		t.Fatalf("after Delete upsert wrote %d, want 1", n)
	}
}
