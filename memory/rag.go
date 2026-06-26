package memory

import (
	"context"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/middleware"
)

// RAGOptions configures the RAG middleware.
type RAGOptions struct {
	// K is the number of documents to retrieve (default 4).
	K int
	// MinScore drops retrieved documents below this similarity (default 0).
	MinScore float64
	// Header is the preamble placed before injected context.
	Header string
}

const defaultRAGHeader = "以下是可能与用户问题相关的背景资料，请在作答时参考（如不相关可忽略）："

// NewRAG builds a retrieval-augmented-generation Middleware: before each model
// call it searches the store with the latest user message and injects the most
// relevant documents into the system prompt. Unlike SearchTool, the model does
// not have to decide to retrieve.
func NewRAG(store Store, opts *RAGOptions) middleware.Middleware {
	r := &rag{store: store, k: 4, header: defaultRAGHeader}
	if opts != nil {
		if opts.K > 0 {
			r.k = opts.K
		}
		r.minScore = opts.MinScore
		if opts.Header != "" {
			r.header = opts.Header
		}
	}
	return middleware.BeforeModel(r.inject)
}

type rag struct {
	store    Store
	k        int
	minScore float64
	header   string
}

// inject searches and rewrites req.System with the retrieved context.
func (r *rag) inject(ctx context.Context, req *llm.Request) error {
	query := lastUserText(req.Messages)
	if query == "" {
		return nil
	}
	docs, err := r.store.Search(ctx, query, r.k)
	if err != nil {
		return fmt.Errorf("memory: RAG search: %w", err)
	}

	var b strings.Builder
	n := 0
	for _, d := range docs {
		if d.Score < r.minScore {
			continue
		}
		n++
		fmt.Fprintf(&b, "[%d] %s\n", n, d.Content)
	}
	if n == 0 {
		return nil
	}

	block := r.header + "\n" + strings.TrimRight(b.String(), "\n")
	if req.System != "" {
		req.System += "\n\n" + block
	} else {
		req.System = block
	}
	return nil
}

// lastUserText returns the text of the most recent user message, or "".
func lastUserText(msgs []core.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == core.RoleUser {
			return msgs[i].Text()
		}
	}
	return ""
}
