package middleware

import (
	"context"
	"sort"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Retriever fetches up to k snippets relevant to a query.
type Retriever interface {
	Retrieve(ctx context.Context, query string, k int) ([]string, error)
}

// RAGOptions configures RAG.
type RAGOptions struct {
	Retriever Retriever
	K         int    // snippets to inject (default 4)
	Header    string // intro line before the snippets
}

// RAG injects retrieved context into the system prompt before each model call,
// using the latest user message as the query (ModifyRequest). Retrieval errors
// or empty results leave the request unchanged.
func RAG(o RAGOptions) agent.Middleware {
	if o.K <= 0 {
		o.K = 4
	}
	if o.Header == "" {
		o.Header = "Relevant context (use it if helpful):"
	}
	return &rag{r: o.Retriever, k: o.K, header: o.Header}
}

type rag struct {
	agent.BaseMiddleware
	r      Retriever
	k      int
	header string
}

func (g *rag) ModifyRequest(lc *agent.LoopContext, req *llm.Request) error {
	if g.r == nil {
		return nil
	}
	q := lastUserText(req.Messages)
	if q == "" {
		return nil
	}
	docs, err := g.r.Retrieve(lc.Context, q, g.k)
	if err != nil || len(docs) == 0 {
		return nil
	}
	block := g.header + "\n- " + strings.Join(docs, "\n- ")
	if req.System == "" {
		req.System = block
	} else {
		req.System = req.System + "\n\n" + block
	}
	return nil
}

func lastUserText(msgs []core.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == core.RoleUser {
			return msgs[i].Text()
		}
	}
	return ""
}

// InMemory is a trivial keyword Retriever for examples and tests. Production
// setups plug in a vector store instead.
type InMemory struct {
	docs []string
}

// NewInMemory builds an in-memory keyword retriever over the given documents.
func NewInMemory(docs ...string) *InMemory { return &InMemory{docs: docs} }

// Retrieve scores documents by how many lower-cased query terms they contain.
func (m *InMemory) Retrieve(_ context.Context, query string, k int) ([]string, error) {
	terms := strings.Fields(strings.ToLower(query))
	type scored struct {
		doc string
		n   int
	}
	var hits []scored
	for _, d := range m.docs {
		ld := strings.ToLower(d)
		n := 0
		for _, t := range terms {
			if strings.Contains(ld, t) {
				n++
			}
		}
		if n > 0 {
			hits = append(hits, scored{d, n})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].n > hits[j].n })
	out := make([]string, 0, k)
	for i := 0; i < len(hits) && i < k; i++ {
		out = append(out, hits[i].doc)
	}
	return out, nil
}
