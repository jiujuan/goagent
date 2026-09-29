package vfs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jiujuan/goagent/core"
)

// tempInfix marks Write's intermediate files, which List hides.
const tempInfix = ".tmp-"

// DirStore is a disk-backed FileStore: the contents are ordinary files in a real
// directory, and os.Root confines every operation to that directory.
//
// It deliberately does not implement core.Snapshottable or core.Restorable
// (ADR-0027). The bytes are already durable, so copying them into a checkpoint
// would bring back both costs DirStore exists to avoid: the full re-copy on
// every save and the File checkpointer's per-line size limit. The consequences
// a caller accepts, per core's "externally managed" contract:
//
//   - checkpoints carry no file state, so each process resuming the thread must
//     re-attach an equivalent handle via agent.WithRunFiles;
//   - files are not versioned, so time travel and forks do not roll them back.
//
// Safe for concurrent use in one process. Concurrent writers across processes
// are undefined: there is no locking, and on Windows a rename onto a file
// another process holds open can fail.
type DirStore struct {
	root *os.Root
}

// NewDirStore opens dir as a file store root, creating it if absent: the caller
// named this path as the store's home.
func NewDirStore(dir string) (*DirStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("vfs: create %s: %w", dir, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("vfs: open root %s: %w", dir, err)
	}
	return &DirStore{root: root}, nil
}

// Read returns the file at path, or an error if absent.
func (d *DirStore) Read(p string) ([]byte, error) {
	name, err := cleanPath(p)
	if err != nil {
		return nil, err
	}
	b, err := d.root.ReadFile(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("vfs: %s not found", p)
		}
		return nil, fmt.Errorf("vfs: read %s: %w", p, err)
	}
	return b, nil
}

// Write stores data at path, replacing any existing file. Missing parent
// directories are created. The data goes to a temporary file that is then
// renamed, so a reader — including one outside this process — never observes a
// half-written file.
func (d *DirStore) Write(p string, data []byte) error {
	name, err := cleanPath(p)
	if err != nil {
		return err
	}
	if parent := path.Dir(name); parent != "." {
		if err := d.root.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("vfs: create directory for %s: %w", p, err)
		}
	}
	tmp := name + tempInfix + core.NewID("w")
	if err := d.root.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("vfs: write %s: %w", p, err)
	}
	if err := d.root.Rename(tmp, name); err != nil {
		return fmt.Errorf("vfs: replace %s: %w", p, err)
	}
	return nil
}

// List returns the paths under prefix, sorted; an empty prefix (or ".") lists
// everything. Intermediate files left by Write are hidden, so the listing is
// only committed content.
func (d *DirStore) List(prefix string) ([]string, error) {
	clean := ""
	if trimmed := strings.TrimSpace(prefix); trimmed != "" && trimmed != "." {
		var err error
		if clean, err = cleanPath(trimmed); err != nil {
			return nil, err
		}
	}
	var out []string
	err := fs.WalkDir(d.root.FS(), ".", func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || strings.Contains(entry.Name(), tempInfix) {
			return nil
		}
		if strings.HasPrefix(p, clean) {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("vfs: list %s: %w", prefix, err)
	}
	sort.Strings(out)
	return out, nil
}

// Close releases the directory handle. The store is unusable afterwards.
func (d *DirStore) Close() error {
	if err := d.root.Close(); err != nil {
		return fmt.Errorf("vfs: close store: %w", err)
	}
	return nil
}

// cleanPath validates a caller-supplied path and renders it in the form os.Root
// expects (slash-separated, relative to the store root, the root itself
// excluded).
//
// The checks are for legible errors, not for safety: os.Root already refuses
// out-of-root resolution, including through symlinks.
func cleanPath(p string) (string, error) {
	slashed := filepath.ToSlash(strings.TrimSpace(p))
	if slashed == "" {
		return "", fmt.Errorf("vfs: missing path; pass one relative to the store root")
	}
	if strings.HasPrefix(slashed, "/") || filepath.VolumeName(slashed) != "" {
		return "", fmt.Errorf("vfs: path %q is absolute; pass one relative to the store root", p)
	}
	clean := path.Clean(slashed)
	if clean == "." {
		return "", fmt.Errorf("vfs: path %q names the store root itself; pass a file path inside it", p)
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("vfs: path %q escapes the store root; stay inside it", p)
	}
	return clean, nil
}

var _ core.FileStore = (*DirStore)(nil)
