package runner

import (
	"context"
	"iter"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/queue"
	"github.com/jiujuan/goagent/session"
)

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

func TestEnqueueAgentBackground(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := session.InMemory()
	q := queue.NewMemQueue(8)
	bus := queue.NewMemBus()
	go queue.NewWorker(queue.Config{Consumer: q, Bus: bus}).Run(ctx)

	vid := agent.Video("vid", mockVideoModel{steps: []llm.VideoProgress{
		{Status: llm.JobRunning, Percent: 50},
		{Status: llm.JobSucceeded, Percent: 100, Video: &core.Video{URL: "https://x/out.mp4", DurationMs: 5000}},
	}})

	app, user, sid := "app", "u", "s"
	ch, unsub := bus.Subscribe(BusKey(app, user, sid))
	defer unsub()

	jobID, err := EnqueueAgent(ctx, q, store, app, user, sid, vid, core.UserText("a cat"))
	if err != nil {
		t.Fatal(err)
	}
	if jobID == "" {
		t.Fatal("expected a job id")
	}

	// Drain the bus to the committed final event.
	var final *core.Event
	timeout := time.After(2 * time.Second)
	for final == nil {
		select {
		case ev := <-ch:
			if !ev.Partial && ev.Message != nil {
				final = ev
			}
		case <-timeout:
			t.Fatal("timed out waiting for final event")
		}
	}
	if _, ok := final.Message.Parts[0].(core.Video); !ok {
		t.Fatalf("final not a video: %+v", final.Message)
	}

	// The result must be persisted to the session (append precedes final publish).
	sess, _ := store.GetOrCreate(ctx, app, user, sid)
	if !hasVideo(sess.Events()) {
		t.Error("final video was not persisted to the session")
	}
}

func hasVideo(events []*core.Event) bool {
	for _, ev := range events {
		if ev.Message == nil {
			continue
		}
		for _, p := range ev.Message.Parts {
			if _, ok := p.(core.Video); ok {
				return true
			}
		}
	}
	return false
}
