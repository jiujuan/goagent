package textmem

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

// indexFile is the human-readable index regenerated on every write. It is for
// humans browsing the directory; Index() scans the .md files directly so it
// never drifts from this sidecar.
const indexFile = "INDEX.md"

// fileStore is a Store backed by one <name>.md file per entry under dir.
type fileStore struct {
	dir string
	mu  sync.Mutex
}

// File returns a file-backed text-memory Store rooted at dir, creating the
// directory if needed.
func File(dir string) (Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("textmem: create dir: %w", err)
	}
	return &fileStore{dir: dir}, nil
}

// Save implements Store.
func (s *fileStore) Save(_ context.Context, e Entry) error {
	name := slug(e.Name)
	if name == "" {
		return fmt.Errorf("textmem: empty entry name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	body := strings.TrimRight(e.Body, "\n")
	content := fmt.Sprintf("---\nname: %s\ndescription: %s\ntype: %s\n---\n%s\n",
		name, e.Desc, e.Type, body)
	if err := os.WriteFile(s.path(name), []byte(content), 0o644); err != nil {
		return fmt.Errorf("textmem: write %s: %w", name, err)
	}
	return s.writeIndex()
}

// Read implements Store.
func (s *fileStore) Read(_ context.Context, name string) (Entry, error) {
	name = slug(name)
	b, err := os.ReadFile(s.path(name))
	if err != nil {
		return Entry{}, fmt.Errorf("textmem: read %s: %w", name, err)
	}
	return decode(name, b), nil
}

// Index implements Store by scanning the directory's .md files (the source of
// truth), returning Name/Desc/Type without Body.
func (s *fileStore) Index(_ context.Context) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.index()
}

// Delete implements Store.
func (s *fileStore) Delete(_ context.Context, name string) error {
	name = slug(name)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.path(name)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("textmem: delete %s: %w", name, err)
	}
	return s.writeIndex()
}

func (s *fileStore) path(name string) string { return filepath.Join(s.dir, name+".md") }

// index reads all entries (header only); caller holds the lock.
func (s *fileStore) index() ([]Entry, error) {
	files, err := filepath.Glob(filepath.Join(s.dir, "*.md"))
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, f := range files {
		if filepath.Base(f) == indexFile {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		e := decode(strings.TrimSuffix(filepath.Base(f), ".md"), b)
		e.Body = "" // index carries no body
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b Entry) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// writeIndex regenerates INDEX.md; caller holds the lock.
func (s *fileStore) writeIndex() error {
	entries, err := s.index()
	if err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("# 记忆索引\n\n")
	for _, e := range entries {
		fmt.Fprintf(&b, "- %s — %s\n", e.Name, e.Desc)
	}
	return os.WriteFile(filepath.Join(s.dir, indexFile), []byte(b.String()), 0o644)
}

// slug sanitizes a name into a safe single-segment filename: lowercased, with
// spaces and path separators replaced by '-'.
func slug(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		case r == ' ' || r == '/' || r == '\\' || r == '.':
			return '-'
		default:
			return r // keep e.g. CJK characters
		}
	}, name)
	return strings.Trim(name, "-")
}

var _ Store = (*fileStore)(nil)
