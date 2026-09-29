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

// A thread id is a directory name, so anything that could add a level or escape
// the files directory is mapped out.
func TestRunFilesSanitizesThread(t *testing.T) {
	dir := t.TempDir()
	w := newAt(t, Config{Dir: dir})

	base := filepath.Join(dir, userDirName, "files")
	for _, id := range []string{"a/b/../c", `x\y`, "weird id:1", "", "   "} {
		files, err := w.RunFiles(id)
		if err != nil {
			t.Fatalf("RunFiles(%q): %v", id, err)
		}
		name := safeThread(id)
		if strings.ContainsAny(name, `/\:`) || name == "." || name == ".." {
			t.Fatalf("safeThread(%q) = %q, want one flat directory name", id, name)
		}
		if err := files.Write("a.md", []byte(id)); err != nil {
			t.Fatal(err)
		}
		files.Close()

		got, err := os.ReadFile(filepath.Join(base, name, "a.md"))
		if err != nil {
			t.Fatalf("RunFiles(%q) wrote outside %s/%s: %v", id, base, name, err)
		}
		if string(got) != id {
			t.Fatalf("artifact = %q, want %q", got, id)
		}
	}
	if name := safeThread(""); name != "thread" {
		t.Errorf(`safeThread("") = %q, want "thread"`, name)
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
