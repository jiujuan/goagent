package core

// Usage reports token consumption for a model call. Providers populate it on
// the final response; middleware uses it to anchor context-size estimation.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// ProgressInfo mirrors a provider-neutral async job lifecycle, carried on a
// Progress event for long-running work (media generation, background jobs).
type ProgressInfo struct {
	JobID   string `json:"job_id,omitempty"`
	Kind    string `json:"kind,omitempty"` // "image" | "video" | ...
	Status  string `json:"status,omitempty"`
	Percent int    `json:"percent,omitempty"`
}
