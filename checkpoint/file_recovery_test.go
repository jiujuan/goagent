package checkpoint

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

func fileRecoveryCheckpoint(thread, id string) *Checkpoint {
	files := vfs.NewInState()
	if err := files.Write("note.txt", []byte("durable")); err != nil {
		panic(err)
	}
	return &Checkpoint{ID: id, ThreadID: thread, State: core.State{Files: files}}
}

func TestFileTailRecoveryAndBlobRetry(t *testing.T) {
	ctx := context.Background()
	normal := defaultFileOps()

	// A short write leaves only a partial blob line. The retry must repair that
	// tail and write both blob and checkpoint, rather than trusting a stale cache.
	f, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	short := false
	f.ops = fileOps{
		write: func(fh *os.File, b []byte) (int, error) {
			if !short {
				short = true
				return 1, io.ErrShortWrite
			}
			return normal.write(fh, b)
		},
		sync: normal.sync,
	}
	cp := fileRecoveryCheckpoint("short", "cp-short")
	if err := f.Save(ctx, cp); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error = %v, want io.ErrShortWrite", err)
	}
	f.ops = normal
	if err := f.Save(ctx, cp); err != nil {
		t.Fatalf("retry after short write: %v", err)
	}
	got, err := f.Latest(ctx, "short")
	if err != nil || got == nil || string(got.FileSnapshot["note.txt"]) != "durable" {
		t.Fatalf("retry did not restore blob snapshot: checkpoint=%+v err=%v", got, err)
	}

	// Sync can fail after all bytes reach the journal. A retry of the same
	// checkpoint must perform another Sync before reporting success.
	syncStore, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	syncs := 0
	syncStore.ops = fileOps{
		write: normal.write,
		sync: func(fh *os.File) error {
			syncs++
			if syncs == 1 {
				return errors.New("injected sync failure")
			}
			return normal.sync(fh)
		},
	}
	syncCP := fileRecoveryCheckpoint("sync", "cp-sync")
	if err := syncStore.Save(ctx, syncCP); err == nil || !strings.Contains(err.Error(), "injected sync failure") {
		t.Fatalf("first sync failure = %v", err)
	}
	if err := syncStore.Save(ctx, syncCP); err != nil {
		t.Fatalf("sync retry failed: %v", err)
	}
	if syncs != 2 {
		t.Fatalf("sync retry calls = %d, want 2", syncs)
	}

	// An incomplete final record is recovered under the writer lock, leaving the
	// last complete checkpoint visible. A complete corrupt line is not recoverable.
	tailStore, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := tailStore.Save(ctx, fileRecoveryCheckpoint("tail", "cp-tail")); err != nil {
		t.Fatal(err)
	}
	tailPath := filepath.Join(tailStore.dir, "tail.jsonl")
	completeJournal := mustReadFile(t, tailPath)
	if fh, err := os.OpenFile(tailPath, os.O_APPEND|os.O_WRONLY, 0o644); err != nil {
		t.Fatal(err)
	} else {
		if _, err := fh.WriteString(`{"version":1`); err != nil {
			_ = fh.Close()
			t.Fatal(err)
		}
		_ = fh.Close()
	}
	if got, err := tailStore.Latest(ctx, "tail"); err != nil || got == nil || got.ID != "cp-tail" {
		t.Fatalf("tail recovery checkpoint=%+v err=%v", got, err)
	}
	data, err := os.ReadFile(tailPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(completeJournal) {
		t.Fatalf("recovered journal = %q, want original complete journal %q", data, completeJournal)
	}

	middleStore, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := middleStore.Save(ctx, fileRecoveryCheckpoint("middle", "cp-middle")); err != nil {
		t.Fatal(err)
	}
	middlePath := filepath.Join(middleStore.dir, "middle.jsonl")
	if err := os.WriteFile(middlePath, append(mustReadFile(t, middlePath), []byte("not-json\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := middleStore.Latest(ctx, "middle"); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("middle corruption error = %v", err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
