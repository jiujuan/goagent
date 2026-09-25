package reporoot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveStopsAtGitDir(t *testing.T) {
	base := tempTree(t, "a/b/c", "dir")
	leaf := filepath.Join(base, "a", "b", "c")

	dirs, err := Resolve(leaf)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(dirs) != 1 || dirs[0] != leaf {
		t.Fatalf("got %v, want just the leaf %q", dirs, leaf)
	}
	if got, _ := Root(leaf); got != leaf {
		t.Fatalf("Root = %q, want %q", got, leaf)
	}
}

func TestResolveWalksUpToBoundary(t *testing.T) {
	base := tempTree(t, "a/b/c", "")
	boundary := filepath.Join(base, "a")
	if err := os.Mkdir(filepath.Join(boundary, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(base, "a", "b", "c")

	dirs, err := Resolve(leaf)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := []string{leaf, filepath.Join(base, "a", "b"), boundary}
	if len(dirs) != len(want) {
		t.Fatalf("got %v, want %v", dirs, want)
	}
	for i := range want {
		if dirs[i] != want[i] {
			t.Errorf("dirs[%d] = %q, want %q", i, dirs[i], want[i])
		}
	}
	if got, _ := Root(leaf); got != boundary {
		t.Fatalf("Root = %q, want %q", got, boundary)
	}
}

// A linked worktree keeps .git as a file holding "gitdir: ...", so a Stat is
// enough to recognise the boundary.
func TestResolveTreatsGitFileAsBoundary(t *testing.T) {
	base := tempTree(t, "a/b", "file")
	leaf := filepath.Join(base, "a", "b")

	if got, _ := Root(leaf); got != leaf {
		t.Fatalf("Root = %q, want %q", got, leaf)
	}
}

func TestResolveWithoutBoundaryReachesFileSystemRoot(t *testing.T) {
	base := tempTree(t, "a/b", "")
	leaf := filepath.Join(base, "a", "b")

	// A dotfiles repo in $HOME (or any other ancestor of the temp dir) would
	// end the walk early, which is correct behaviour but not what this case
	// exercises.
	for dir := leaf; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			t.Skipf("%s has a .git ancestor; cannot test the boundary-free walk", leaf)
		}
		if parent := filepath.Dir(dir); parent == dir {
			break
		}
	}

	dirs, err := Resolve(leaf)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	last := dirs[len(dirs)-1]
	if filepath.Dir(last) != last {
		t.Fatalf("walk stopped at %q, want a fixed point (the filesystem root)", last)
	}
	// Root must not widen a non-repo tree to the filesystem root.
	if got, _ := Root(leaf); got != leaf {
		t.Fatalf("Root = %q, want the leaf %q", got, leaf)
	}
}

func TestResolveMissingStartDirIsLexical(t *testing.T) {
	leaf := filepath.Join(t.TempDir(), "a", "b", "c") // never created

	dirs, err := Resolve(leaf)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if dirs[0] != leaf {
		t.Fatalf("dirs[0] = %q, want %q", dirs[0], leaf)
	}
}

func TestResolveMakesRelativeStartAbsolute(t *testing.T) {
	dirs, err := Resolve(".")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !filepath.IsAbs(dirs[0]) {
		t.Fatalf("dirs[0] = %q, want absolute", dirs[0])
	}
}

// tempTree creates a temp directory containing the given slash-separated dir
// leaf (parents included) and, when gitKind is non-empty, a .git entry inside
// that leaf: a directory for "dir", a file for "file". It returns the temp
// directory, so callers join the leaf themselves.
func tempTree(t *testing.T, leaf, gitKind string) string {
	t.Helper()
	base := t.TempDir()
	full := filepath.Join(base, filepath.FromSlash(leaf))
	if err := os.MkdirAll(full, 0o755); err != nil {
		t.Fatal(err)
	}
	switch gitKind {
	case "":
	case "dir":
		if err := os.Mkdir(filepath.Join(full, ".git"), 0o755); err != nil {
			t.Fatal(err)
		}
	case "file":
		if err := os.WriteFile(filepath.Join(full, ".git"), []byte("gitdir: /elsewhere\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown gitKind %q", gitKind)
	}
	return base
}
