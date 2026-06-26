package agent

import (
	"errors"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// VideoAgent wraps an llm.VideoModel as an Agent, the video counterpart of
// ImageAgent. Video generation is long-running and asynchronous; VideoAgent
// drives the model's progress stream and re-emits each update as a Partial
// progress event, ending with a final event that carries the finished video.
// Run directly (the caller streams progress live over the runner) or enqueue it
// for background execution via runner.EnqueueAgent.
type VideoAgent struct {
	name     string
	desc     string
	model    llm.VideoModel
	defaults []llm.VideoOption
}

// Video constructs a VideoAgent. Defaults apply to every generation.
func Video(name string, m llm.VideoModel, defaults ...llm.VideoOption) *VideoAgent {
	return &VideoAgent{
		name:     name,
		desc:     "generates a video from a text prompt",
		model:    m,
		defaults: defaults,
	}
}

func (a *VideoAgent) Name() string        { return a.name }
func (a *VideoAgent) Description() string { return a.desc }
func (a *VideoAgent) SubAgents() []Agent  { return nil }

// Run implements Agent.
func (a *VideoAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		req := &llm.VideoRequest{Prompt: ictx.UserContent.Text()}
		req.Options.Apply(a.defaults...)

		var done *llm.VideoProgress
		for p, err := range a.model.GenerateVideo(ictx, req) {
			if err != nil {
				yield(a.errEvent(ictx, err), err)
				return
			}
			if !yield(&core.Event{
				ID:           core.NewID("evt"),
				InvocationID: ictx.InvocationID,
				Author:       a.name,
				Branch:       ictx.Branch,
				Partial:      true,
				Progress:     &core.Progress{Kind: "video", Status: string(p.Status), Percent: p.Percent},
			}, nil) {
				return
			}
			done = p
		}

		switch {
		case done == nil:
			yield(a.errEvent(ictx, errors.New("video generation produced no result")), errors.New("video generation produced no result"))
			return
		case done.Status == llm.JobFailed:
			msg := done.Err
			if msg == "" {
				msg = "video generation failed"
			}
			err := errors.New(msg)
			yield(a.errEvent(ictx, err), err)
			return
		case done.Status != llm.JobSucceeded || done.Video == nil:
			err := errors.New("video generation did not complete")
			yield(a.errEvent(ictx, err), err)
			return
		}

		yield(&core.Event{
			ID:           core.NewID("evt"),
			InvocationID: ictx.InvocationID,
			Author:       a.name,
			Branch:       ictx.Branch,
			Message:      &core.Message{Role: core.RoleAssistant, Parts: []core.Part{*done.Video}},
			Progress:     &core.Progress{Kind: "video", Status: "succeeded", Percent: 100},
		}, nil)
	}
}

func (a *VideoAgent) errEvent(ictx InvocationContext, err error) *core.Event {
	return &core.Event{
		ID:           core.NewID("evt"),
		InvocationID: ictx.InvocationID,
		Author:       a.name,
		Branch:       ictx.Branch,
		Err:          err,
	}
}

var _ Agent = (*VideoAgent)(nil)
