// Package memory provides long-term memory / retrieval-augmented generation
// (RAG) for goagent. A Store holds Documents and retrieves the ones most
// relevant to a query (by embedding similarity). Two integrations expose a
// Store to an agent: SearchTool (the model decides when to retrieve) and the
// RAG middleware (relevant context is injected automatically before each model
// call).
package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/jiujuan/goagent/core"
)

// Document is a unit of retrievable knowledge.
type Document struct {
	ID string `json:"id"`
	// Key is the fact's identity, used by Upsertable to decide whether an equal
	// document is already stored. Empty means "derive it from Content" (see
	// ContentKey). Unlike the random ID, it is stable across runs.
	Key      string         `json:"key,omitempty"`
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

// Upsertable is an optional capability of a Store that writes a fact only when
// that fact is not already stored. Where Add appends unconditionally, Upsert is
// idempotent: replaying the same documents changes nothing. Probed with a type
// assertion, so the Store contract stays minimal:
//
//	if u, ok := store.(memory.Upsertable); ok {
//	    n, err := u.Upsert(ctx, docs...)
//	}
type Upsertable interface {
	// Upsert writes the documents whose Key (or content-derived key) is not yet
	// stored and returns how many were written. The check and the write are not
	// atomic (embedding happens outside the store lock), so two concurrent
	// Upserts of the same unseen fact may both write.
	Upsert(ctx context.Context, docs ...Document) (int, error)
}

// ContentKey returns the stable identity of a fact: its content lower-cased and
// whitespace-normalized, then hashed. Two runs that extract the same sentence
// get the same key, which is what lets Upsert deduplicate across runs.
func ContentKey(content string) string {
	norm := strings.ToLower(strings.Join(strings.Fields(content), " "))
	sum := sha256.Sum256([]byte(norm))
	return hex.EncodeToString(sum[:8])
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

// ensureKey derives the content key when the document lacks one, so records
// written by Add are still recognized by a later Upsert.
func ensureKey(d Document) Document {
	if d.Key == "" {
		d.Key = ContentKey(d.Content)
	}
	return d
}
