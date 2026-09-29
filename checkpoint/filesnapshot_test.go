package checkpoint_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

func cpWith(thread, id string, files map[string][]byte) *checkpoint.Checkpoint {
	st := core.State{}
	if files != nil {
		fs := vfs.NewInState()
		for p, b := range files {
			_ = fs.Write(p, b)
		}
		st.Files = fs
	}
	return &checkpoint.Checkpoint{ID: id, ThreadID: thread, Step: 1, State: st}
}

func blobLines(t *testing.T, dir, thread string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, thread+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" && strings.Contains(line, `"blob":`) {
			n++
		}
	}
	return n
}

func TestFileSavesAndRehydratesFileSnapshot(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	f, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"a.txt": []byte("one"), "b.bin": {0, 1, 2, 255}}
	if err := f.Save(ctx, cpWith("t1", "c1", files)); err != nil {
		t.Fatal(err)
	}

	cp, err := f.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.FileSnapshot) != 2 || string(cp.FileSnapshot["a.txt"]) != "one" ||
		string(cp.FileSnapshot["b.bin"]) != string(files["b.bin"]) {
		t.Fatalf("snapshot = %v", cp.FileSnapshot)
	}
	if cp.State.Files != nil {
		t.Fatal("reader must not fabricate a FileStore handle; handoff is the snapshot map")
	}

	// Same contents again → new index line, zero new blob lines (dedup).
	before := blobLines(t, dir, "t1")
	if err := f.Save(ctx, cpWith("t1", "c2", files)); err != nil {
		t.Fatal(err)
	}
	if got := blobLines(t, dir, "t1"); got != before {
		t.Fatalf("blob lines grew %d → %d; identical content must not be rewritten", before, got)
	}

	// One new file: only its blob is appended.
	files2 := map[string][]byte{"a.txt": []byte("one"), "b.bin": {0, 1, 2, 255}, "c.txt": []byte("two")}
	if err := f.Save(ctx, cpWith("t1", "c3", files2)); err != nil {
		t.Fatal(err)
	}
	if got := blobLines(t, dir, "t1"); got != before+1 {
		t.Fatalf("blob lines = %d, want %d (one new content)", got, before+1)
	}
	cp, err = f.Latest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.FileSnapshot) != 3 || string(cp.FileSnapshot["c.txt"]) != "two" {
		t.Fatalf("latest snapshot = %v", cp.FileSnapshot)
	}
}

func TestFileWithoutFilesUnchanged(t *testing.T) {
	dir := t.TempDir()
	f, _ := checkpoint.NewFile(dir)
	ctx := context.Background()
	if err := f.Save(ctx, cpWith("t2", "c1", nil)); err != nil {
		t.Fatal(err)
	}
	cp, err := f.Latest(ctx, "t2")
	if err != nil {
		t.Fatal(err)
	}
	if cp.FileSnapshot != nil {
		t.Fatalf("no files → nil snapshot, got %v", cp.FileSnapshot)
	}
	if n := blobLines(t, dir, "t2"); n != 0 {
		t.Fatalf("no files → no blob lines, got %d", n)
	}
}

// TestFileLegacyLineReadable pins backward compatibility: a checkpoint line
// written before the file-snapshot mechanism must still load.
func TestFileLegacyLineReadable(t *testing.T) {
	dir := t.TempDir()
	legacy, err := json.Marshal(map[string]any{
		"id": "old", "thread_id": "t3", "step": 0,
		"state": map[string]any{"messages": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "hi"}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "t3.jsonl"), append(legacy, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	f, _ := checkpoint.NewFile(dir)
	cp, err := f.Latest(context.Background(), "t3")
	if err != nil {
		t.Fatal(err)
	}
	if cp.ID != "old" || cp.FileSnapshot != nil {
		t.Fatalf("legacy read = %+v", cp)
	}
	if len(cp.State.Messages) != 1 {
		t.Fatalf("legacy state lost: %v", cp.State.Messages)
	}
}

func TestFileMissingBlobIsError(t *testing.T) {
	dir := t.TempDir()
	f, _ := checkpoint.NewFile(dir)
	ctx := context.Background()
	if err := f.Save(ctx, cpWith("t4", "c1", map[string][]byte{"a": []byte("x")})); err != nil {
		t.Fatal(err)
	}
	// Corrupt: drop the blob line, keep the checkpoint line that references it.
	data, _ := os.ReadFile(filepath.Join(dir, "t4.jsonl"))
	var keep []string
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" && !strings.Contains(line, `"blob":`) {
			keep = append(keep, line)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "t4.jsonl"), []byte(strings.Join(keep, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Latest(ctx, "t4"); err == nil || !strings.Contains(err.Error(), "missing blob") {
		t.Fatalf("want missing-blob error, got %v", err)
	}
}

// notSnap is a FileStore without the snapshot capabilities (externally managed).
type notSnap struct{ data map[string][]byte }

func (n *notSnap) Read(p string) ([]byte, error)  { return n.data[p], nil }
func (n *notSnap) Write(p string, b []byte) error { n.data[p] = b; return nil }
func (n *notSnap) List(string) ([]string, error)  { return nil, nil }

func TestFileExternalBackendNotPersisted(t *testing.T) {
	dir := t.TempDir()
	f, _ := checkpoint.NewFile(dir)
	ctx := context.Background()
	st := core.State{Files: &notSnap{data: map[string][]byte{"k": []byte("v")}}}
	cp := &checkpoint.Checkpoint{ID: "c1", ThreadID: "t5", Step: 0, State: st}
	if err := f.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	got, err := f.Latest(ctx, "t5")
	if err != nil {
		t.Fatal(err)
	}
	if got.FileSnapshot != nil || blobLines(t, dir, "t5") != 0 {
		t.Fatalf("non-snapshottable backend must be skipped: snap=%v", got.FileSnapshot)
	}
}
