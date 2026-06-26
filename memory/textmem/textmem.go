// Package textmem provides long-term TEXT memory: one curated fact per Markdown
// file, plus a compact index that is injected into the system prompt. Unlike the
// semantic store (package memory), text memory is human-readable, editable, and
// git-diffable — it is the home for explicit user preferences, project
// conventions, and corrected feedback, not bulk fuzzy-recalled knowledge. The
// two are complementary and can be mounted together. See ADR 0018.
package textmem

import "context"

// Entry is one unit of text memory. Name is a kebab-case slug used as the file
// stem; Desc is a one-line summary that goes into the index; Type categorizes
// the entry (user|feedback|project|reference); Body is the full Markdown.
type Entry struct {
	Name string
	Desc string
	Type string
	Body string
}

// Store persists text-memory entries. Implementations may be file-backed or
// in-memory; integrations depend only on this interface.
type Store interface {
	// Save writes (or overwrites) an entry by Name.
	Save(ctx context.Context, e Entry) error
	// Read returns the full entry for a name.
	Read(ctx context.Context, name string) (Entry, error)
	// Index returns every entry's Name/Desc/Type (without Body), for the
	// prompt-injected index and discovery.
	Index(ctx context.Context) ([]Entry, error)
	// Delete removes an entry by name (no error if absent).
	Delete(ctx context.Context, name string) error
}
