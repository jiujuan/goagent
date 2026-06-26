package agent

import (
	"errors"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// ImageAgent is a media-generation agent: it wraps an llm.ImageModel the way
// LLMAgent wraps an llm.Model. Its Run takes the prompt from the invocation's
// user content, calls the model, and streams a "running" progress event followed
// by a final event carrying the generated image(s). Being an Agent, it composes
// in workflows (Sequential/Parallel/Loop), can be a transfer target, or run as a
// root — and its progress streams natively over core.Stream, no tool or queue
// required for the synchronous path.
type ImageAgent struct {
	name     string
	desc     string
	model    llm.ImageModel
	defaults []llm.ImageOption
}

// Image constructs an ImageAgent. Defaults apply to every generation.
func Image(name string, m llm.ImageModel, defaults ...llm.ImageOption) *ImageAgent {
	return &ImageAgent{
		name:     name,
		desc:     "generates images from a text prompt",
		model:    m,
		defaults: defaults,
	}
}

func (a *ImageAgent) Name() string        { return a.name }
func (a *ImageAgent) Description() string { return a.desc }
func (a *ImageAgent) SubAgents() []Agent  { return nil }

// Run implements Agent.
func (a *ImageAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		req := &llm.ImageRequest{Prompt: ictx.UserContent.Text()}
		req.Options.Apply(a.defaults...)

		if !yield(a.progress(ictx, "running", 0), nil) {
			return
		}

		var last *llm.ImageResponse
		for r, err := range a.model.GenerateImage(ictx, req) {
			if err != nil {
				yield(a.errEvent(ictx, err), err)
				return
			}
			if !r.Partial {
				last = r
			}
		}
		if last == nil || len(last.Images) == 0 {
			err := errors.New("no image was generated")
			yield(a.errEvent(ictx, err), err)
			return
		}

		parts := make([]core.Part, len(last.Images))
		for i, img := range last.Images {
			parts[i] = img
		}
		yield(&core.Event{
			ID:           core.NewID("evt"),
			InvocationID: ictx.InvocationID,
			Author:       a.name,
			Branch:       ictx.Branch,
			Message:      &core.Message{Role: core.RoleAssistant, Parts: parts},
			Progress:     &core.Progress{Kind: "image", Status: "succeeded", Percent: 100},
		}, nil)
	}
}

func (a *ImageAgent) progress(ictx InvocationContext, status string, pct int) *core.Event {
	return &core.Event{
		ID:           core.NewID("evt"),
		InvocationID: ictx.InvocationID,
		Author:       a.name,
		Branch:       ictx.Branch,
		Partial:      true,
		Progress:     &core.Progress{Kind: "image", Status: status, Percent: pct},
	}
}

func (a *ImageAgent) errEvent(ictx InvocationContext, err error) *core.Event {
	return &core.Event{
		ID:           core.NewID("evt"),
		InvocationID: ictx.InvocationID,
		Author:       a.name,
		Branch:       ictx.Branch,
		Err:          err,
	}
}

var _ Agent = (*ImageAgent)(nil)
