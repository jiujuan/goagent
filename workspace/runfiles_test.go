package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
)

// listingBelow walks dir and reports every regular file under it,
// slash-separated and relative to dir, skipping the .goagent subtree so a test
// can assert that nothing landed in the workspace outside that directory.
func listingBelow(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if info.IsDir() {
			if rel == filepath.ToSlash(userDirName) {
				return filepath.SkipDir
			}
			return nil
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Artifacts go under the workspace's own config directory, one folder per
// thread, and nowhere else in the root.
func TestRunFilesLocation(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "keep.txt"), "source tree")
	w := newAt(t, Config{Dir: dir})

	files, err := w.RunFiles("t1")
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.Write("notes/a.md", []byte("# report")); err != nil {
		t.Fatal(err)
	}

	want := filepath.Join(dir, ".goagent", "files", "t1", "notes", "a.md")
	if got, err := os.ReadFile(want); err != nil {
		t.Fatalf("artifact missing at %s: %v", want, err)
	} else if string(got) != "# report" {
		t.Fatalf("artifact on disk = %q", got)
	}
	if left := strings.Join(listingBelow(t, dir), ","); left != "keep.txt" {
		t.Errorf("files outside .goagent = %q, want only keep.txt", left)
	}
	// The same root handle the model's file tools read through reaches it.
	if got, err := w.FS().ReadFile(".goagent/files/t1/notes/a.md"); err != nil {
		t.Errorf("root handle cannot read the artifact: %v", err)
	} else if string(got) != "# report" {
		t.Errorf("root handle read %q", got)
	}
}

// A thread id names the artifact directory verbatim (ADR-0028), so an id that
// could add a level, escape the files directory, or collide with another id's
// sanitized name is refused — and a refusal creates nothing at all.
func TestRunFilesRejectsThread(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir})

	for _, id := range []string{"a/b/../c", `x\y`, "weird id:1", "", "   ", "..", ".", "会话一"} {
		files, err := w.RunFiles(id)
		if err == nil {
			files.Close()
			t.Errorf("RunFiles(%q) succeeded, want rejection", id)
			continue
		}
		if !strings.Contains(err.Error(), "thread id") {
			t.Errorf("RunFiles(%q) = %v, want the thread id error", id, err)
		}
	}
	// Rejection happens before the directory is created, so the workspace stays
	// as it was: no .goagent directory appeared.
	if _, err := os.Stat(filepath.Join(dir, userDirName)); !os.IsNotExist(err) {
		t.Errorf("%s exists after rejected ids (stat err: %v)", filepath.Join(dir, userDirName), err)
	}

	// The sanitized pair from ADR-0028 no longer shares a directory: one is a
	// name, the other is not an id at all. The handle must be closed, or its open
	// directory handle blocks the temporary directory's cleanup on Windows.
	legal, err := w.RunFiles("a_b")
	if err != nil {
		t.Fatalf("RunFiles(a_b): %v", err)
	}
	if err := legal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.RunFiles("a/b"); err == nil {
		t.Fatal("RunFiles(a/b) succeeded, want rejection")
	}
}

// Two threads of one workspace keep separate artifact directories.
func TestRunFilesDistinctThreadsIsolated(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir})

	one, err := w.RunFiles("t1")
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	two, err := w.RunFiles("t2")
	if err != nil {
		t.Fatal(err)
	}
	defer two.Close()

	if err := one.Write("report.md", []byte("thread one")); err != nil {
		t.Fatal(err)
	}
	if err := two.Write("report.md", []byte("thread two")); err != nil {
		t.Fatal(err)
	}
	if got, err := one.Read("report.md"); err != nil {
		t.Fatal(err)
	} else if string(got) != "thread one" {
		t.Fatalf("t1 read %q", got)
	}
	if got, err := two.Read("report.md"); err != nil {
		t.Fatal(err)
	} else if string(got) != "thread two" {
		t.Fatalf("t2 read %q", got)
	}
}

// The assembly layer hands out a store the checkpoint must not copy (ADR-0027):
// if this ever gains Snapshottable, artifacts go into the checkpoint line and
// the File checkpointer's size limit applies to them again.
func TestRunFilesIsNotSnapshottable(t *testing.T) {
	w := newAt(t, Config{Dir: t.TempDir()})
	files, err := w.RunFiles("t1")
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()

	if _, ok := any(files).(core.Snapshottable); ok {
		t.Fatal("RunFiles must not hand out a snapshottable backend")
	}
	if _, ok := any(files).(core.Restorable); ok {
		t.Fatal("RunFiles must not hand out a restorable backend")
	}
}

// The store owns a second root handle, so it outlives the Workspace that handed
// it out — closing the workspace must not invalidate a run's artifacts.
func TestRunFilesHandleOutlivesWorkspace(t *testing.T) {
	dir := t.TempDir()
	w, err := New(Config{
		Dir:             dir,
		GlobalRulesDir:  filepath.Join(t.TempDir(), "no-global-rules"),
		GlobalSkillsDir: filepath.Join(t.TempDir(), "no-global-skills"),
	})
	if err != nil {
		t.Fatal(err)
	}
	files, err := w.RunFiles("t1")
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()

	if err := files.Write("a.md", []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := files.Read("a.md"); err != nil {
		t.Fatalf("read after the workspace closed: %v", err)
	} else if string(got) != "kept" {
		t.Fatalf("read %q after close", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
