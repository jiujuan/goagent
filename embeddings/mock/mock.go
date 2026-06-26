// Package mock provides a deterministic, network-free Embedder for tests and
// examples. It uses a hashed bag-of-words model: each token increments a
// component of a fixed-dimension vector, which is then L2-normalized. Texts
// that share tokens get higher cosine similarity, so retrieval behaves
// meaningfully without any API. It tokenizes ASCII words and individual CJK
// characters, so both English and Chinese work.
package mock

import (
	"context"
	"hash/fnv"
	"math"
	"strings"
	"unicode"

	"github.com/jiujuan/goagent/embeddings"
)

// Embedder is a deterministic hashing embedder.
type Embedder struct {
	dim int
}

// New returns a mock embedder with a default dimension.
func New() *Embedder { return &Embedder{dim: 256} }

// NewDim returns a mock embedder with a given dimension.
func NewDim(dim int) *Embedder {
	if dim <= 0 {
		dim = 256
	}
	return &Embedder{dim: dim}
}

// Embed implements embeddings.Embedder.
func (e *Embedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = e.vec(t)
	}
	return out, nil
}

func (e *Embedder) vec(text string) []float32 {
	v := make([]float32, e.dim)
	for _, tok := range tokenize(text) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		v[h.Sum32()%uint32(e.dim)]++
	}
	// L2-normalize so cosine similarity reduces to a dot product.
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm > 0 {
		inv := float32(1 / math.Sqrt(norm))
		for i := range v {
			v[i] *= inv
		}
	}
	return v
}

func tokenize(s string) []string {
	s = strings.ToLower(s)
	var toks []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			toks = append(toks, cur.String())
			cur.Reset()
		}
	}
	for _, r := range s {
		switch {
		case unicode.Is(unicode.Han, r):
			flush()
			toks = append(toks, string(r))
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			cur.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return toks
}

var _ embeddings.Embedder = (*Embedder)(nil)
