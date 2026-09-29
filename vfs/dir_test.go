package vfs_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

// storeAt opens a DirStore over a fresh temporary directory.
func storeAt(t *testing.T) (string, *vfs.DirStore) {
	t.Helper()
	dir := t.TempDir()
	d, err := vfs.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return dir, d
}

// filesBelow lists every regular file under dir, slash-separated and sorted, so
// tests can assert what actually reached the disk.
func filesBelow(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDirStoreRoundTrip(t *testing.T) {
	dir, d := storeAt(t)

	if err := d.Write("notes/a.md", []byte("# alpha")); err != nil {
		t.Fatal(err)
	}
	bin := []byte{0, 1, 2, 255}
	if err := d.Write("raw.bin", bin); err != nil {
		t.Fatal(err)
	}

	got, err := d.Read("notes/a.md")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "# alpha" {
		t.Fatalf("read text = %q", got)
	}
	if got, err := d.Read("raw.bin"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(got, bin) {
		t.Fatalf("binary round trip = %v", got)
	}
	// The bytes really are on disk, at the path the store is rooted at.
	if onDisk, err := os.ReadFile(filepath.Join(dir, "notes", "a.md")); err != nil {
		t.Fatal(err)
	} else if string(onDisk) != "# alpha" {
		t.Fatalf("disk content = %q", onDisk)
	}

	all, err := d.List("")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"notes/a.md", "raw.bin"}; !reflect.DeepEqual(all, want) {
		t.Fatalf("List(\"\") = %v, want %v", all, want)
	}
	if under, err := d.List("notes"); err != nil {
		t.Fatal(err)
	} else if want := []string{"notes/a.md"}; !reflect.DeepEqual(under, want) {
		t.Fatalf("List(notes) = %v, want %v", under, want)
	}

	if _, err := d.Read("missing.md"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing read = %v, want a not-found error", err)
	}
}

func TestDirStoreListPrefixSemantics(t *testing.T) {
	_, d := storeAt(t)

	for _, name := range []string{"notes/a.md", "notes/b.md", "top.txt"} {
		if err := d.Write(name, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"notes/a.md", "notes/b.md", "top.txt"}

	// "." and "" both mean everything.
	for _, prefix := range []string{"", "."} {
		if got, err := d.List(prefix); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(got, want) {
			t.Fatalf("List(%q) = %v, want %v", prefix, got, want)
		}
	}
	if got, err := d.List("notes/"); err != nil {
		t.Fatal(err)
	} else if want := []string{"notes/a.md", "notes/b.md"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("List(notes/) = %v, want %v", got, want)
	}
	if _, err := d.List("../notes"); err == nil {
		t.Fatal("List with an escaping prefix succeeded")
	}
}

func TestDirStoreRejectsPaths(t *testing.T) {
	dir, d := storeAt(t)

	bad := []string{"", "   ", ".", "/", "/abs", "..", "../x", "a/../../x", "./.."}
	if runtime.GOOS == "windows" {
		// A drive letter is only a volume name on Windows; on POSIX "C:\x" is an
		// ordinary file name inside the root, which the store takes.
		bad = append(bad, `C:\x`, "C:/x")
	}
	for _, p := range bad {
		before := filesBelow(t, dir)
		if err := d.Write(p, []byte("x")); err == nil {
			t.Errorf("Write(%q) succeeded, want rejection", p)
		}
		if _, err := d.Read(p); err == nil || strings.Contains(err.Error(), "not found") {
			t.Errorf("Read(%q) = %v, want a path error, not not-found", p, err)
		}
		if after := filesBelow(t, dir); !reflect.DeepEqual(before, after) {
			t.Errorf("Write(%q) touched the disk: %v", p, after)
		}
	}
	// Naming the root is its own error, not an escape: the two are different
	// mistakes for whoever supplied the path.
	if _, err := d.Read("."); err == nil || !strings.Contains(err.Error(), "names the store root itself") {
		t.Fatalf(`Read(".") = %v, want the root-naming error`, err)
	}
}

func TestDirStoreAtomicOverwrite(t *testing.T) {
	dir, d := storeAt(t)

	if err := d.Write("a.md", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := d.Write("a.md", []byte("second")); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Read("a.md"); err != nil {
		t.Fatal(err)
	} else if string(got) != "second" {
		t.Fatalf("after overwrite = %q", got)
	}
	for _, name := range filesBelow(t, dir) {
		if strings.Contains(name, ".tmp-") {
			t.Fatalf("intermediate file left behind: %s", name)
		}
	}
}

func TestDirStoreSharedAcrossHandles(t *testing.T) {
	dir := t.TempDir()

	d1, err := vfs.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d1.Close()
	if err := d1.Write("shared/note.md", []byte("from handle one")); err != nil {
		t.Fatal(err)
	}

	d2, err := vfs.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if got, err := d2.Read("shared/note.md"); err != nil {
		t.Fatal(err)
	} else if string(got) != "from handle one" {
		t.Fatalf("second handle read %q", got)
	}
	// One directory is one fact: writing through either handle is visible to both.
	if err := d2.Write("shared/note.md", []byte("from handle two")); err != nil {
		t.Fatal(err)
	}
	if got, err := d1.Read("shared/note.md"); err != nil {
		t.Fatal(err)
	} else if string(got) != "from handle two" {
		t.Fatalf("first handle read %q", got)
	}
}

// DirStore is externally managed on purpose (ADR-0027): the checkpoint must not
// copy its bytes. These assertions are the design premise, not an oversight.
func TestDirStoreIsNotSnapshottable(t *testing.T) {
	_, d := storeAt(t)

	if _, ok := any(d).(core.Snapshottable); ok {
		t.Fatal("DirStore must not implement core.Snapshottable")
	}
	if _, ok := any(d).(core.Restorable); ok {
		t.Fatal("DirStore must not implement core.Restorable")
	}
	var _ core.FileStore = d
}

func TestDirStoreListSkipsTempAndSorts(t *testing.T) {
	dir, d := storeAt(t)

	for _, name := range []string{"z.md", "m.md", "a/b.md", "a/c.md"} {
		if err := d.Write(name, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	// An interrupted write leaves its intermediate file behind; List must hide it.
	if err := os.WriteFile(filepath.Join(dir, "x.tmp-w1"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	want := []string{"a/b.md", "a/c.md", "m.md", "z.md"}
	for range 3 {
		got, err := d.List("")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("List = %v, want %v", got, want)
		}
	}
}

func TestDirStoreClose(t *testing.T) {
	dir := t.TempDir()
	d, err := vfs.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Write("a.md", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Closed means an error, not a panic, and the file on disk is untouched.
	if _, err := d.Read("a.md"); err == nil {
		t.Fatal("read after Close succeeded")
	}
	if got, err := os.ReadFile(filepath.Join(dir, "a.md")); err != nil {
		t.Fatal(err)
	} else if string(got) != "one" {
		t.Fatalf("disk content after Close = %q", got)
	}
}
