package agent

import (
	"context"
	"iter"
	"testing"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

type mockImageModel struct{ imgs []core.Image }

func (m mockImageModel) Name() string { return "mock-image" }
func (m mockImageModel) GenerateImage(_ context.Context, _ *llm.ImageRequest) iter.Seq2[*llm.ImageResponse, error] {
	return func(yield func(*llm.ImageResponse, error) bool) {
		yield(&llm.ImageResponse{Images: m.imgs}, nil)
	}
}

type mockVideoModel struct{ steps []llm.VideoProgress }

func (m mockVideoModel) Name() string { return "mock-video" }
func (m mockVideoModel) GenerateVideo(_ context.Context, _ *llm.VideoRequest) iter.Seq2[*llm.VideoProgress, error] {
	return func(yield func(*llm.VideoProgress, error) bool) {
		for i := range m.steps {
			if !yield(&m.steps[i], nil) {
				return
			}
		}
	}
}

func TestImageAgentStreamsProgressThenImage(t *testing.T) {
	a := Image("img", mockImageModel{imgs: []core.Image{{URL: "https://x/1.png"}}})
	ictx := InvocationContext{Context: context.Background(), InvocationID: "inv", UserContent: core.UserText("a cat")}

	var running, final *core.Event
	for ev, err := range a.Run(ictx) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.Partial {
			running = ev
		} else {
			final = ev
		}
	}
	if running == nil || running.Progress == nil || running.Progress.Status != "running" {
		t.Fatalf("expected a running progress event, got %+v", running)
	}
	if final == nil || final.Message == nil {
		t.Fatal("no final event")
	}
	img, ok := final.Message.Parts[0].(core.Image)
	if !ok || img.URL != "https://x/1.png" {
		t.Fatalf("final missing image: %+v", final.Message)
	}
	if final.Progress.Status != "succeeded" {
		t.Errorf("final status = %q", final.Progress.Status)
	}
}

func TestImageAgentErrorsWhenEmpty(t *testing.T) {
	a := Image("img", mockImageModel{})
	ictx := InvocationContext{Context: context.Background(), InvocationID: "inv", UserContent: core.UserText("x")}
	var gotErr bool
	for _, err := range a.Run(ictx) {
		if err != nil {
			gotErr = true
		}
	}
	if !gotErr {
		t.Error("expected an error when no image is produced")
	}
}

func TestVideoAgentStreamsProgressThenVideo(t *testing.T) {
	a := Video("vid", mockVideoModel{steps: []llm.VideoProgress{
		{Status: llm.JobQueued},
		{Status: llm.JobRunning, Percent: 50},
		{Status: llm.JobSucceeded, Percent: 100, Video: &core.Video{URL: "https://x/out.mp4", DurationMs: 5000}},
	}})
	ictx := InvocationContext{Context: context.Background(), InvocationID: "inv", UserContent: core.UserText("x")}

	var progress int
	var final *core.Event
	for ev, err := range a.Run(ictx) {
		if err != nil {
			t.Fatal(err)
		}
		if ev.Partial {
			progress++
		} else {
			final = ev
		}
	}
	if progress != 3 {
		t.Errorf("expected 3 progress events, got %d", progress)
	}
	vid, ok := final.Message.Parts[0].(core.Video)
	if !ok || vid.URL != "https://x/out.mp4" {
		t.Fatalf("final missing video: %+v", final.Message)
	}
}

func TestVideoAgentReportsFailure(t *testing.T) {
	a := Video("vid", mockVideoModel{steps: []llm.VideoProgress{
		{Status: llm.JobFailed, Err: "out of capacity"},
	}})
	ictx := InvocationContext{Context: context.Background(), InvocationID: "inv", UserContent: core.UserText("x")}
	var gotErr bool
	for _, err := range a.Run(ictx) {
		if err != nil {
			gotErr = true
		}
	}
	if !gotErr {
		t.Error("expected failure to surface as an error")
	}
}
