package memory_test

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	embmock "github.com/jiujuan/goagent/embeddings/mock"
	"github.com/jiujuan/goagent/memory"
)

func openFileStore(t *testing.T) (*memory.FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	fs, err := memory.File(dir, embmock.New())
	if err != nil {
		t.Fatal(err)
	}
	return fs, filepath.Join(dir, "memory.jsonl")
}

func add(t *testing.T, s memory.Store, docs ...memory.Document) {
	t.Helper()
	if err := s.Add(context.Background(), docs...); err != nil {
		t.Fatal(err)
	}
}

func contains(docs []memory.Document, want string) bool {
	for _, d := range docs {
		if strings.Contains(d.Content, want) {
			return true
		}
	}
	return false
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		if len(strings.TrimSpace(string(sc.Bytes()))) > 0 {
			n++
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}

// A damaged line (hand edit) or a torn tail (crash mid-append) must not make the
// whole store unusable: it is skipped and reported by Dropped.
func TestFileStoreLoadToleratesDamagedLines(t *testing.T) {
	fs, path := openFileStore(t)
	add(t, fs,
		memory.Document{ID: "a", Content: "巴黎是法国的首都"},
		memory.Document{ID: "b", Content: "Go 是编译型语言"},
	)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("not json at all\n{\"id\":\"c\",\"content\":\"截断的"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	reopened, err := memory.File(filepath.Dir(path), embmock.New())
	if err != nil {
		t.Fatalf("store with damaged lines must still open: %v", err)
	}
	if reopened.Len() != 2 {
		t.Fatalf("Len = %d, want the 2 intact records", reopened.Len())
	}
	if reopened.Dropped() != 2 {
		t.Fatalf("Dropped = %d, want 2", reopened.Dropped())
	}
	docs, err := reopened.Search(context.Background(), "法国的首都", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(docs, "巴黎") {
		t.Fatalf("intact record not retrievable: %+v", docs)
	}
}

// Delete drops the document from retrieval and survives a restart via its
// tombstone; deleting an unknown ID writes nothing.
func TestFileStoreDeletePersistsAsTombstone(t *testing.T) {
	fs, path := openFileStore(t)
	add(t, fs,
		memory.Document{ID: "a", Content: "巴黎是法国的首都"},
		memory.Document{ID: "b", Content: "Go 是编译型语言"},
	)
	before := countLines(t, path)

	if err := fs.Delete(context.Background(), "zzz"); err != nil {
		t.Fatal(err)
	}
	if got := countLines(t, path); got != before {
		t.Fatalf("unknown id appended %d lines, want 0", got-before)
	}

	if err := fs.Delete(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if fs.Len() != 1 {
		t.Fatalf("Len = %d, want 1", fs.Len())
	}
	docs, err := fs.Search(context.Background(), "法国的首都", 5)
	if err != nil {
		t.Fatal(err)
	}
	if contains(docs, "巴黎") {
		t.Fatalf("deleted document still retrieved: %+v", docs)
	}

	reopened, err := memory.File(filepath.Dir(path), embmock.New())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Len() != 1 {
		t.Fatalf("after reload Len = %d, want 1 (tombstone not replayed)", reopened.Len())
	}
}

// Compact rewrites the log down to the live set; NeedsCompaction is a report,
// never an implicit rewrite.
func TestFileStoreCompactRewritesLiveSet(t *testing.T) {
	fs, path := openFileStore(t)
	if fs.NeedsCompaction() {
		t.Fatal("fresh store should not need compaction")
	}
	add(t, fs,
		memory.Document{ID: "a", Content: "巴黎是法国的首都"},
		memory.Document{ID: "b", Content: "Go 是编译型语言"},
		memory.Document{ID: "c", Content: "光合作用把阳光转成能量"},
	)
	if err := fs.Delete(context.Background(), "a", "b"); err != nil {
		t.Fatal(err)
	}
	if !fs.NeedsCompaction() {
		t.Fatalf("3 puts + 2 tombstones for 1 live doc should request compaction")
	}
	if fs.Len() != 1 {
		t.Fatalf("Len = %d, want 1", fs.Len())
	}

	if err := fs.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := countLines(t, path); got != 1 {
		t.Fatalf("file has %d lines after compact, want 1", got)
	}
	if fs.NeedsCompaction() {
		t.Fatal("compacted store should not need compaction")
	}
	if _, err := os.Stat(path + ".compact"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind: %v", err)
	}

	reopened, err := memory.File(filepath.Dir(path), embmock.New())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Len() != 1 {
		t.Fatalf("after reload Len = %d, want 1", reopened.Len())
	}
	docs, err := reopened.Search(context.Background(), "光合作用", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(docs, "光合作用") || docs[0].Score <= 0 {
		t.Fatalf("compaction lost the embedding: %+v", docs)
	}
}

// A failed append must not leave the in-memory index ahead of the file.
func TestFileStoreAddFailureLeavesIndexUntouched(t *testing.T) {
	fs, path := openFileStore(t)
	add(t, fs, memory.Document{ID: "a", Content: "巴黎是法国的首都"})

	if err := os.Chmod(path, 0o444); err != nil {
		t.Skipf("cannot make the file read-only here: %v", err)
	}
	// Restore the bit so t.TempDir can remove the file.
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

	if err := fs.Add(context.Background(), memory.Document{ID: "b", Content: "Go 是编译型语言"}); err == nil {
		t.Skip("platform ignored the read-only bit, cannot exercise the write-failure path")
	}
	if fs.Len() != 1 {
		t.Fatalf("Len = %d after a failed Add, want the index to stay unchanged", fs.Len())
	}
	docs, err := fs.Search(context.Background(), "编译型语言", 3)
	if err != nil {
		t.Fatal(err)
	}
	if contains(docs, "Go") {
		t.Fatalf("document that was not persisted is in the index: %+v", docs)
	}
}

// Concurrent writes and reads are the normal case for a shared store.
func TestFileStoreConcurrentUse(t *testing.T) {
	fs, path := openFileStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := fs.Add(context.Background(), memory.Doc("记忆条目 "+string(rune('a'+i)))); err != nil {
				t.Error(err)
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := fs.Search(context.Background(), "记忆条目", 4); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if fs.Len() != 8 {
		t.Fatalf("Len = %d, want 8", fs.Len())
	}
	if err := fs.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := countLines(t, path); got != 8 {
		t.Fatalf("file has %d lines after compact, want 8", got)
	}
}

// The in-memory backend satisfies Mutable without a log to reclaim.
func TestInMemoryMutable(t *testing.T) {
	s := memory.InMemory(embmock.New())
	add(t, s,
		memory.Document{ID: "a", Content: "巴黎是法国的首都"},
		memory.Document{ID: "b", Content: "Go 是编译型语言"},
	)
	m, ok := memory.Store(s).(memory.Mutable)
	if !ok {
		t.Fatal("InMemoryStore should implement Mutable")
	}
	if err := m.Delete(context.Background(), "a", "nope"); err != nil {
		t.Fatal(err)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}
	if m.NeedsCompaction() {
		t.Fatal("in-memory store has nothing to compact")
	}
	if err := m.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	docs, err := s.Search(context.Background(), "法国的首都", 3)
	if err != nil {
		t.Fatal(err)
	}
	if contains(docs, "巴黎") {
		t.Fatalf("deleted document still retrieved: %+v", docs)
	}
}
