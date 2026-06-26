package llm

// Options carries per-call decoding parameters. It is populated with the
// functional-options pattern (Option) so callers override only what they need
// without breaking call signatures as the struct grows.
type Options struct {
	Model       string
	Temperature float64
	MaxTokens   int
	TopP        float64
	Stop        []string
	Stream      bool

	// ToolChoice hints how the model should use tools: "" (auto), "none",
	// "required", or a specific tool name.
	ToolChoice string

	// Metadata is free-form provider passthrough.
	Metadata map[string]any
}

// Option mutates Options.
type Option func(*Options)

// Apply folds a list of options into o.
func (o *Options) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

// WithModel overrides the model id for a request.
func WithModel(m string) Option { return func(o *Options) { o.Model = m } }

// WithTemperature sets the sampling temperature.
func WithTemperature(t float64) Option { return func(o *Options) { o.Temperature = t } }

// WithMaxTokens caps the output length.
func WithMaxTokens(n int) Option { return func(o *Options) { o.MaxTokens = n } }

// WithTopP sets nucleus sampling.
func WithTopP(p float64) Option { return func(o *Options) { o.TopP = p } }

// WithStop sets stop sequences.
func WithStop(words ...string) Option { return func(o *Options) { o.Stop = words } }

// WithStream toggles streaming.
func WithStream(on bool) Option { return func(o *Options) { o.Stream = on } }

// WithToolChoice constrains tool usage.
func WithToolChoice(choice string) Option { return func(o *Options) { o.ToolChoice = choice } }

// WithMetadata attaches a provider-passthrough key/value.
func WithMetadata(k string, v any) Option {
	return func(o *Options) {
		if o.Metadata == nil {
			o.Metadata = map[string]any{}
		}
		o.Metadata[k] = v
	}
}
