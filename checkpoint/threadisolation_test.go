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

// An id that cannot name a file is refused on write and on read (ADR-0028): it
// is no longer rewritten into a shareable name, and it cannot point outside the
// store directory either.
func TestFileRejectsUnsafeThreadIDs(t *testing.T) {
	dir := t.TempDir()
	f, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for _, bad := range []string{"tenant/a", `x\y`, "weird id:1", "..", "", "sess/1"} {
		cp := &checkpoint.Checkpoint{ID: "c1", ThreadID: bad, Step: 1, State: note("x")}
		if err := f.Save(ctx, cp); err == nil {
			t.Errorf("Save(%q) succeeded, want rejection", bad)
		}
		if _, err := f.Latest(ctx, bad); err == nil {
			t.Errorf("Latest(%q) succeeded, want rejection", bad)
		}
		if _, err := f.History(ctx, bad); err == nil {
			t.Errorf("History(%q) succeeded, want rejection", bad)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("rejected writes left files behind: %v", entries)
	}

	// A legal id is unaffected, and its file is named after the id verbatim.
	if err := f.Save(ctx, &checkpoint.Checkpoint{ID: "c2", ThreadID: "tenant_a", Step: 1, State: note("ok")}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "tenant_a.jsonl")); err != nil {
		t.Fatalf("thread file not named after the id: %v", err)
	}
	cp, err := f.Latest(ctx, "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	if cp.ID != "c2" {
		t.Fatalf("Latest = %+v, want c2", cp)
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

// Foreign records are dropped before their file snapshots are rehydrated, so a
// record belonging to another thread cannot fail this thread's read by pointing
// at a blob that is not in the file.
func TestFileForeignBrokenRecordDoesNotBreakOwnRead(t *testing.T) {
	dir := t.TempDir()
	own := line(t, &checkpoint.Checkpoint{ID: "c-own", ThreadID: "sess_1", Step: 1, State: note("conversation B")})
	broken := `{"id":"c-broken","thread_id":"sess/1","step":1,"state":{},"files":{"gone.md":"deadbeefdeadbeef"}}`
	if err := os.WriteFile(filepath.Join(dir, "sess_1.jsonl"), []byte(broken+"\n"+own+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := f.Latest(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("another thread's damaged record broke this read: %v", err)
	}
	if cp.ID != "c-own" {
		t.Fatalf("Latest = %s, want c-own", cp.ID)
	}
}
