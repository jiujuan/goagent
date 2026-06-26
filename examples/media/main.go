// Command media demonstrates non-blocking, queue-backed media generation: a
// VideoAgent is enqueued for background execution, and its progress and final
// result are streamed to a frontend via the queue Bus — the enqueue call returns
// immediately without blocking. It uses an in-process mock video model so it runs
// offline; swap in agnes.Video(...) for the real gateway.
package main

import (
	"context"
	"fmt"
	"iter"
	"log"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/queue"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

// mockVideo is a fake llm.VideoModel that emits a few progress steps then a
// finished video, so the example needs no API key or network.
type mockVideo struct{}

func (mockVideo) Name() string { return "mock-video" }

func (mockVideo) GenerateVideo(ctx context.Context, _ *llm.VideoRequest) iter.Seq2[*llm.VideoProgress, error] {
	return func(yield func(*llm.VideoProgress, error) bool) {
		steps := []llm.VideoProgress{
			{Status: llm.JobQueued, Percent: 0},
			{Status: llm.JobRunning, Percent: 40},
			{Status: llm.JobRunning, Percent: 80},
			{Status: llm.JobSucceeded, Percent: 100, Video: &core.Video{MIME: "video/mp4", URL: "https://example.com/out.mp4", DurationMs: 5000}},
		}
		for i := range steps {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			case <-time.After(300 * time.Millisecond):
			}
			if !yield(&steps[i], nil) {
				return
			}
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Background execution infrastructure (the independent queue package).
	store := session.InMemory()
	q := queue.NewMemQueue(16)
	bus := queue.NewMemBus()
	go queue.NewWorker(queue.Config{Consumer: q, Bus: bus}).Run(ctx)

	// Media model as an Agent — peer of an LLMAgent, not a tool.
	//   real gateway: agent.Video("video", agnes.Video("agnes-video-v2.0", apiKey))
	vid := agent.Video("video", mockVideo{})

	app, user, sid := "demo", "u1", "s1"

	// The frontend subscribes to the session's event stream.
	ch, unsub := bus.Subscribe(runner.BusKey(app, user, sid))
	defer unsub()

	// Enqueue — returns immediately, does NOT block on generation.
	jobID, err := runner.EnqueueAgent(ctx, q, store, app, user, sid, vid, core.UserText("a cat walking on the beach at sunset"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued %s (non-blocking); streaming events:\n", jobID)

	for ev := range ch {
		switch {
		case ev.Message != nil: // final result
			for _, p := range ev.Message.Parts {
				if v, ok := p.(core.Video); ok {
					fmt.Printf("  ✓ done: %s (%dms)\n", v.URL, v.DurationMs)
				}
			}
			return
		case ev.Progress != nil:
			fmt.Printf("  [%s] %s %d%%\n", ev.Progress.Kind, ev.Progress.Status, ev.Progress.Percent)
		}
	}
}
