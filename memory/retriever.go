package memory

import (
	"context"
	"strings"
)

// Retriever adapts a Store to the structural retriever interface that
// middleware.RAG expects — Retrieve(ctx, query, k) ([]string, error) — so
// long-term memory plugs straight into automatic RAG:
//
//	store := memory.InMemory(embmock.New())
//	store.Add(ctx, memory.Doc("巴黎是法国的首都。"))
//	a, _ := agent.New(
//	    agent.WithModel(model),
//	    agent.WithMiddleware(middleware.RAG(middleware.RAGOptions{
//	        Retriever: memory.NewRetriever(store, 0),
//	    })),
//	)
//
// (It satisfies middleware.Retriever structurally, so memory does not import
// middleware — no dependency cycle.)
type Retriever struct {
	store    Store
	minScore float64
}

// NewRetriever wraps a Store; retrieved documents scoring below minScore are
// dropped (0 keeps all).
func NewRetriever(store Store, minScore float64) *Retriever {
	return &Retriever{store: store, minScore: minScore}
}

// Retrieve returns up to k documents' content most relevant to the query.
func (r *Retriever) Retrieve(ctx context.Context, query string, k int) ([]string, error) {
	docs, err := r.store.Search(ctx, query, k)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		if d.Score < r.minScore {
			continue
		}
		out = append(out, strings.TrimSpace(d.Content))
	}
	return out, nil
}
