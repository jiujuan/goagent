// Package embeddings defines the provider-agnostic Embedder interface used by
// the memory/vector-store layer for retrieval. Concrete embedders live in
// subpackages (embeddings/mock, embeddings/openaicompat) so the core stays
// dependency-free.
package embeddings

import "context"

// Embedder turns texts into dense vectors. The same method embeds both stored
// documents and queries; cosine similarity between the two drives retrieval.
type Embedder interface {
	// Embed returns one vector per input text, in the same order. All returned
	// vectors must share the same dimension.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}
