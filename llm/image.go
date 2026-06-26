package llm

import (
	"context"
	"iter"

	"github.com/jiujuan/goagent/core"
)

// ImageModel is the capability contract for image generation. It is deliberately
// separate from Model: image generation has a different request/response shape
// (a prompt plus optional input images, yielding pictures) and does not fit the
// chat-completion loop. A single provider may implement both interfaces.
//
// GenerateImage returns a Stream of ImageResponse values so it mirrors Model:
// a synchronous provider yields exactly one response, while an asynchronous
// (job-polling) provider may yield Partial progress responses before the final.
type ImageModel interface {
	// Name reports the model identifier, e.g. "agnes-image-2.1-flash".
	Name() string

	// GenerateImage runs one image-generation call and streams the result.
	GenerateImage(ctx context.Context, req *ImageRequest) iter.Seq2[*ImageResponse, error]
}

// ImageRequest is one image-generation invocation.
type ImageRequest struct {
	// Prompt describes the image to generate (text-to-image) or the edit to
	// apply (image-to-image).
	Prompt string

	// InputImages, when non-empty, switches the call to image-to-image: the
	// provider conditions generation on these references. Each is passed by URL
	// or inline data per the provider's support.
	InputImages []core.Image

	// Options carries generation parameters, populated via ImageOption.
	Options ImageOptions
}

// ImageResponse is one increment (or the whole result) of an image call.
type ImageResponse struct {
	// Images holds the generated pictures. Empty on a Partial progress update.
	Images []core.Image

	// Partial marks a streaming progress update that should not be persisted.
	Partial bool

	// Usage reports consumption when the provider returns it.
	Usage *core.Usage
}

// ImageOptions carries per-call image parameters. Like Options it is populated
// with functional options so the struct can grow without breaking callers.
type ImageOptions struct {
	Model       string
	Size        string // e.g. "1024x768"
	N           int    // number of images to generate
	Quality     string // provider-specific, e.g. "standard"/"hd"
	Seed        *int64 // fixed seed for reproducibility (pointer: 0 is meaningful)
	AspectRatio string // providers that take an aspect ratio instead of size
	Metadata    map[string]any
}

// ImageOption mutates ImageOptions.
type ImageOption func(*ImageOptions)

// Apply folds a list of options into o.
func (o *ImageOptions) Apply(opts ...ImageOption) {
	for _, opt := range opts {
		opt(o)
	}
}

// WithImageModel overrides the model id for a request.
func WithImageModel(m string) ImageOption { return func(o *ImageOptions) { o.Model = m } }

// WithImageSize sets the output size, e.g. "1024x768".
func WithImageSize(s string) ImageOption { return func(o *ImageOptions) { o.Size = s } }

// WithImageCount sets how many images to generate.
func WithImageCount(n int) ImageOption { return func(o *ImageOptions) { o.N = n } }

// WithImageQuality sets a provider-specific quality level.
func WithImageQuality(q string) ImageOption { return func(o *ImageOptions) { o.Quality = q } }

// WithImageSeed fixes the random seed for reproducible output.
func WithImageSeed(seed int64) ImageOption {
	return func(o *ImageOptions) { o.Seed = &seed }
}

// WithImageAspectRatio sets an aspect ratio for providers that take one.
func WithImageAspectRatio(r string) ImageOption { return func(o *ImageOptions) { o.AspectRatio = r } }

// WithImageMetadata attaches a provider-passthrough key/value.
func WithImageMetadata(k string, v any) ImageOption {
	return func(o *ImageOptions) {
		if o.Metadata == nil {
			o.Metadata = map[string]any{}
		}
		o.Metadata[k] = v
	}
}
