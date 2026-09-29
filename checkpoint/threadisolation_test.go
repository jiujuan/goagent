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
)

func note(text string) core.State {
	return core.State{Messages: []core.Message{core.UserText(text)}}
}

// line marshals a checkpoint as it would appear in a thread file. Marshaling
// directly is what lets a test plant a record whose own ThreadID differs from
// the file it lands in — the shape a store written before ADR-0028 has.
func line(t *testing.T, cp *checkpoint.Checkpoint) string {
	t.Helper()
	b, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A file that holds records for two thread ids must answer each query with only
// its own records: Latest must not hand back the other conversation, History must
// not list it, and its checkpoint id must not be loadable under this thread.
func TestFileMixedThreadFileServesOnlyItsOwnThread(t *testing.T) {
	dir := t.TempDir()
	foreign := line(t, &checkpoint.Checkpoint{ID: "c-foreign", ThreadID: "sess/1", Step: 1, State: note("conversation A")})
	own := line(t, &checkpoint.Checkpoint{ID: "c-own", ThreadID: "sess_1", Step: 1, State: note("conversation B")})
	// Two records that would fold onto one file name: "sess/1" and "sess_1". The
	// foreign one is written last, so an unfixed Latest would resume it.
	if err := os.WriteFile(filepath.Join(dir, "sess_1.jsonl"), []byte(own+"\n"+foreign+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	cp, err := f.Latest(ctx, "sess_1")
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil || cp.ID != "c-own" || cp.ThreadID != "sess_1" {
		t.Fatalf("Latest returned a foreign record: %+v", cp)
	}
	if got, err := f.History(ctx, "sess_1"); err != nil {
		t.Fatal(err)
	} else if len(got) != 1 || got[0].ID != "c-own" {
		t.Fatalf("History = %d records, want only this thread's: %+v", len(got), got)
	}
	if _, err := f.Load(ctx, "sess_1", "c-foreign"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Load of another thread's checkpoint = %v, want not found", err)
	}
}

// The same guarantee for a store produced by Save itself: two distinct ids whose
// sanitized file names collide (the pre-ADR-0028 folding) still resume
// independently instead of continuing each other's conversation.
func TestFileFoldedIdsKeepSeparateHistories(t *testing.T) {
	dir := t.TempDir()
	w, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	slashed := &checkpoint.Checkpoint{ID: "c1", ThreadID: "tenant/a", Step: 1, State: note("conversation A")}
	under := &checkpoint.Checkpoint{ID: "c2", ThreadID: "tenant_a", Step: 1, State: note("conversation B")}
	for _, cp := range []*checkpoint.Checkpoint{slashed, under} {
		if err := w.Save(ctx, cp); err != nil {
			t.Fatal(err)
		}
	}
	// Both ids share one file today: the sanitizer maps them to the same name.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".jsonl") {
			files = append(files, e.Name())
		}
	}
	if len(files) != 1 || files[0] != "tenant_a.jsonl" {
		t.Fatalf("thread files = %v, want the folded tenant_a.jsonl", files)
	}

	r, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"tenant/a": "c1", "tenant_a": "c2"} {
		cp, err := r.Latest(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if cp == nil || cp.ID != want {
			t.Fatalf("Latest(%q) = %+v, want checkpoint %s", id, cp, want)
		}
		if got := cp.State.Messages[0].Text(); !strings.Contains(got, "conversation") {
			t.Fatalf("Latest(%q) lost its own message: %q", id, got)
		}
	}
}

// A single-thread store behaves exactly as before: every record belongs to the
// queried id, so the new filter drops nothing.
func TestFileUnfoldedThreadUnaffected(t *testing.T) {
	dir := t.TempDir()
	f, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i, id := range []string{"c1", "c2", "c3"} {
		cp := &checkpoint.Checkpoint{ID: id, ThreadID: "plain", Step: i, State: note("step")}
		if err := f.Save(ctx, cp); err != nil {
			t.Fatal(err)
		}
	}
	got, err := f.History(ctx, "plain")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("History = %d records, want all 3 kept", len(got))
	}
	latest, err := f.Latest(ctx, "plain")
	if err != nil {
		t.Fatal(err)
	}
	if latest.ID != "c3" || latest.Step != 2 {
		t.Fatalf("Latest = %s/%d, want c3/2", latest.ID, latest.Step)
	}
}
