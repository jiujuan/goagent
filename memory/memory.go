// Package memory provides long-term memory / retrieval-augmented generation
// (RAG) for goagent. A Store holds Documents and retrieves the ones most
// relevant to a query (by embedding similarity). Two integrations expose a
// Store to an agent: SearchTool (the model decides when to retrieve) and the
// RAG middleware (relevant context is injected automatically before each model
// call).
package memory

import (
	"context"

	"github.com/jiujuan/goagent/core"
)

// Document is a unit of retrievable knowledge.
type Document struct {
	ID       string         `json:"id"`
	Content  string         `json:"content"`
	Metadata map[string]any `json:"metadata,omitempty"`
	// Score is the similarity to the query, set on retrieval (higher = closer).
	Score float64 `json:"score,omitempty"`
}

// Store is a long-term memory / vector store.
type Store interface {
	// Add inserts documents (assigning IDs to any that lack one).
	Add(ctx context.Context, docs ...Document) error
	// Search returns up to k documents most relevant to the query, ranked by
	// descending Score.
	Search(ctx context.Context, query string, k int) ([]Document, error)
}

// Mutable is an optional capability of a Store that can forget documents and
// reclaim its storage. Callers probe it with a type assertion, so the core Store
// contract stays minimal:
//
//	if m, ok := store.(memory.Mutable); ok {
//	    _ = m.Delete(ctx, ids...)
//	}
type Mutable interface {
	// Delete removes the documents with the given IDs (IDs unknown to the
	// store are ignored). It is not a hard removal in every backend: a
	// file-backed store records a tombstone and needs Compact to reclaim space.
	Delete(ctx context.Context, ids ...string) error
	// Compact reclaims storage for deleted or superseded records.
	Compact(ctx context.Context) error
	// NeedsCompaction reports whether deleted/superseded records have piled up
	// enough that a Compact is worth doing. Backends never compact implicitly.
	NeedsCompaction() bool
}

// Doc is a convenience constructor for a Document with just content.
func Doc(content string) Document { return Document{Content: content} }

// DocWithMeta builds a Document with content and metadata.
func DocWithMeta(content string, meta map[string]any) Document {
	return Document{Content: content, Metadata: meta}
}

// ensureID assigns a random ID if the document lacks one.
func ensureID(d Document) Document {
	if d.ID == "" {
		d.ID = core.NewID("doc")
	}
	return d
}
