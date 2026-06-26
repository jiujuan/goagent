package memory

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/jiujuan/goagent/embeddings"
)

// InMemoryStore is a brute-force vector store: it embeds documents on Add and
// ranks them against the query embedding by cosine similarity on Search. It is
// suitable for development and modest corpora; swap in a real vector database
// behind the Store interface for scale.
type InMemoryStore struct {
	embedder embeddings.Embedder
	mu       sync.RWMutex
	docs     []storedDoc
}

type storedDoc struct {
	doc Document
	vec []float32
}

// InMemory builds an in-memory vector store backed by the given embedder.
func InMemory(e embeddings.Embedder) *InMemoryStore {
	return &InMemoryStore{embedder: e}
}

// Add implements Store.
func (s *InMemoryStore) Add(ctx context.Context, docs ...Document) error {
	if len(docs) == 0 {
		return nil
	}
	texts := make([]string, len(docs))
	for i, d := range docs {
		texts[i] = d.Content
	}
	vecs, err := s.embedder.Embed(ctx, texts)
	if err != nil {
		return fmt.Errorf("memory: embed documents: %w", err)
	}
	if len(vecs) != len(docs) {
		return fmt.Errorf("memory: embedder returned %d vectors for %d docs", len(vecs), len(docs))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for i, d := range docs {
		s.docs = append(s.docs, storedDoc{doc: ensureID(d), vec: vecs[i]})
	}
	return nil
}

// Search implements Store.
func (s *InMemoryStore) Search(ctx context.Context, query string, k int) ([]Document, error) {
	if k <= 0 {
		k = 4
	}
	qv, err := s.embedder.Embed(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("memory: embed query: %w", err)
	}
	if len(qv) == 0 {
		return nil, nil
	}

	s.mu.RLock()
	scored := rank(qv[0], s.docs, k)
	s.mu.RUnlock()
	return scored, nil
}

// Len reports the number of stored documents.
func (s *InMemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.docs)
}

// rank scores every stored doc against the query vector by cosine similarity,
// sorts descending, and returns the top k (k<=0 defaults to 4). Shared by
// InMemoryStore and FileStore, which both hold []storedDoc.
func rank(qv []float32, docs []storedDoc, k int) []Document {
	if k <= 0 {
		k = 4
	}
	scored := make([]Document, 0, len(docs))
	for _, sd := range docs {
		d := sd.doc
		d.Score = cosine(qv, sd.vec)
		scored = append(scored, d)
	}
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })
	if len(scored) > k {
		scored = scored[:k]
	}
	return scored
}

// cosine returns the cosine similarity of two equal-length vectors (0 if either
// is zero or lengths differ).
func cosine(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

var _ Store = (*InMemoryStore)(nil)
