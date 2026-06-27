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
