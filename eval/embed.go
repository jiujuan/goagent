package eval

import (
	"context"

	"github.com/jiujuan/goagent/embeddings"
)

// embed.go scores semantic closeness between Output and Reference via an
// Embedder (cosine similarity, mapped to [0,1]). (Stub; filled in step 2/3.)

// SemanticSimilarity scores cosine similarity between Output and Reference
// embeddings, normalized to [0,1]. Passes at >= 0.8 by default.
func SemanticSimilarity(e embeddings.Embedder) Scorer {
	return newScorer("semantic_similarity", func(ctx context.Context, s Sample) (Score, error) {
		return Score{Name: "semantic_similarity"}, errTODO
	})
}
