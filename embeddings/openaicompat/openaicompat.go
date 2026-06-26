// Package openaicompat implements embeddings.Embedder against any
// OpenAI-compatible /embeddings endpoint (OpenAI, Azure, compatible gateways).
package openaicompat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/jiujuan/goagent/embeddings"
)

// Config configures an OpenAI-compatible embedder.
type Config struct {
	BaseURL string // e.g. https://api.openai.com/v1
	APIKey  string
	Model   string // e.g. text-embedding-3-small
	HTTP    *http.Client
}

// Embedder is an OpenAI-compatible embeddings.Embedder.
type Embedder struct {
	cfg Config
}

// New builds an Embedder from an explicit Config.
func New(cfg Config) *Embedder {
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 2 * time.Minute}
	}
	return &Embedder{cfg: cfg}
}

// OpenAI targets api.openai.com (e.g. model "text-embedding-3-small").
func OpenAI(model, apiKey string) *Embedder {
	return New(Config{BaseURL: "https://api.openai.com/v1", APIKey: apiKey, Model: model})
}

// Embed implements embeddings.Embedder.
func (e *Embedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(map[string]any{"model": e.cfg.Model, "input": texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.BaseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+e.cfg.APIKey)

	resp, err := e.cfg.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embeddings: status %d: %s", resp.StatusCode, data)
	}

	var wr struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &wr); err != nil {
		return nil, fmt.Errorf("embeddings: decode: %w", err)
	}
	// The API may return embeddings out of order; sort by index.
	sort.Slice(wr.Data, func(i, j int) bool { return wr.Data[i].Index < wr.Data[j].Index })
	out := make([][]float32, len(wr.Data))
	for i, d := range wr.Data {
		out[i] = d.Embedding
	}
	return out, nil
}

var _ embeddings.Embedder = (*Embedder)(nil)
