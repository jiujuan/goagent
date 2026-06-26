package llm

import (
	"context"
	"iter"

	"github.com/jiujuan/goagent/core"
)

// VideoModel is the capability contract for video generation. Video generation
// is almost always asynchronous and long-running (submit a job, poll until it
// finishes), so GenerateVideo returns a Stream of VideoProgress values rather
// than a single result. Modeling the poll loop as a Stream reuses goagent's one
// streaming primitive: context cancellation stops polling, and a consumer that
// breaks early stops production — exactly like SSE token streaming.
type VideoModel interface {
	// Name reports the model identifier, e.g. "agnes-video-v2.0".
	Name() string

	// GenerateVideo submits a generation job and streams its progress, ending
	// with a VideoProgress whose Status is JobSucceeded (Video populated) or
	// JobFailed.
	GenerateVideo(ctx context.Context, req *VideoRequest) iter.Seq2[*VideoProgress, error]
}

// ResumableVideoModel is an optional capability: a provider that can reattach to
// an already-submitted job by its id. Callers persist the JobID from the first
// VideoProgress and, after a restart, resume polling without re-running (and
// re-paying for) the job. Detect support with a type assertion.
type ResumableVideoModel interface {
	VideoModel

	// ResumeVideo streams progress for an existing job id.
	ResumeVideo(ctx context.Context, jobID string) iter.Seq2[*VideoProgress, error]
}

// JobStatus is the provider-neutral lifecycle of an async generation job.
type JobStatus string

const (
	JobQueued    JobStatus = "queued"    // accepted, not yet running
	JobRunning   JobStatus = "running"   // generation in progress
	JobSucceeded JobStatus = "succeeded" // finished; result available
	JobFailed    JobStatus = "failed"    // terminal failure
)

// VideoRequest is one video-generation invocation.
type VideoRequest struct {
	// Prompt describes the video to generate.
	Prompt string

	// InputImages drives image-to-video and keyframe workflows. One image is
	// image-to-video; several are multi-image or keyframe interpolation (the
	// provider routes them per Options.Mode).
	InputImages []core.Image

	// Options carries generation parameters, populated via VideoOption.
	Options VideoOptions
}

// VideoProgress is one update from a running generation job.
type VideoProgress struct {
	// JobID identifies the job; stable across the stream. Persist it to resume.
	JobID string

	// Status is the current lifecycle state.
	Status JobStatus

	// Percent is the completion percentage when the provider reports it.
	Percent int

	// Video is the finished result, set only when Status is JobSucceeded.
	Video *core.Video

	// Err carries provider-side failure detail when Status is JobFailed.
	Err string
}

// VideoOptions carries per-call video parameters.
type VideoOptions struct {
	Model          string
	Width, Height  int
	NumFrames      int     // total frames (provider may constrain, e.g. 8n+1)
	FrameRate      float64 // frames per second
	Seed           *int64
	NegativePrompt string
	Mode           string // "", "keyframes", etc. (provider-specific)
	Metadata       map[string]any
}

// VideoOption mutates VideoOptions.
type VideoOption func(*VideoOptions)

// Apply folds a list of options into o.
func (o *VideoOptions) Apply(opts ...VideoOption) {
	for _, opt := range opts {
		opt(o)
	}
}

// WithVideoModel overrides the model id.
func WithVideoModel(m string) VideoOption { return func(o *VideoOptions) { o.Model = m } }

// WithVideoSize sets width and height in pixels.
func WithVideoSize(w, h int) VideoOption {
	return func(o *VideoOptions) { o.Width, o.Height = w, h }
}

// WithVideoFrames sets the total number of frames.
func WithVideoFrames(n int) VideoOption { return func(o *VideoOptions) { o.NumFrames = n } }

// WithVideoFrameRate sets the frame rate (fps).
func WithVideoFrameRate(fps float64) VideoOption { return func(o *VideoOptions) { o.FrameRate = fps } }

// WithVideoSeed fixes the random seed.
func WithVideoSeed(seed int64) VideoOption {
	return func(o *VideoOptions) { o.Seed = &seed }
}

// WithVideoNegativePrompt sets content to avoid.
func WithVideoNegativePrompt(p string) VideoOption {
	return func(o *VideoOptions) { o.NegativePrompt = p }
}

// WithVideoMode sets a provider-specific generation mode (e.g. "keyframes").
func WithVideoMode(m string) VideoOption { return func(o *VideoOptions) { o.Mode = m } }

// WithVideoMetadata attaches a provider-passthrough key/value.
func WithVideoMetadata(k string, v any) VideoOption {
	return func(o *VideoOptions) {
		if o.Metadata == nil {
			o.Metadata = map[string]any{}
		}
		o.Metadata[k] = v
	}
}
