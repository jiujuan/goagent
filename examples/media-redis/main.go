// Command media-redis is the Redis-backed twin of the media example: the same
// background VideoAgent, but the queue is a Redis Stream and the event Bus is
// Redis Pub/Sub, so the producer (enqueue) and the worker can live in different
// processes and survive a restart.
//
// The one API difference from the in-process version is forced by Redis: a
// Job.Run closure cannot be serialized onto a stream, so work is enqueued as a
// Type + Payload and the worker rebuilds it from a Registry. This file shows that
// end to end against a real redis-server.
//
//	# start a redis first, e.g.:  docker run -p 6379:6379 redis:7
//	REDIS_URL=redis://localhost:6379/0 go run ./examples/media-redis
//
// Only Redis is external — the video model is the same in-process mock as the
// media example, so no API key or network model call is needed.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/queue"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

// mockVideo is a fake llm.VideoModel that emits a few progress steps then a
// finished video, so the example needs no API key or model network call.
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

// agentPayload is the serializable stand-in for the closure the in-process
// version captures. It names the agent and carries the call's inputs; the
// handler rehydrates everything else on the worker side.
type agentPayload struct {
	App     string `json:"app"`
	User    string `json:"user"`
	Session string `json:"session"`
	Agent   string `json:"agent"`
	Prompt  string `json:"prompt"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}
	pingRedis(ctx, redisURL)

	// Agents available to the worker, looked up by name from the payload. In a
	// real deployment every worker process registers the same agents at startup.
	store := session.InMemory()
	agents := map[string]agent.Agent{
		"video": agent.Video("video", mockVideo{}),
	}

	// The registry rebuilds a job from its Type + Payload. This "agent" handler
	// is the cross-process equivalent of runner.EnqueueAgent's closure: it runs
	// the named agent, forwards Partial events to the bus (via emit) and commits
	// the final one to the store.
	reg := queue.Registry{
		"agent": func(ctx context.Context, payload []byte, emit func(*core.Event)) (*core.Event, error) {
			var p agentPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				return nil, err
			}
			ag, ok := agents[p.Agent]
			if !ok {
				return nil, fmt.Errorf("unknown agent %q", p.Agent)
			}
			sess, err := store.GetOrCreate(ctx, p.App, p.User, p.Session)
			if err != nil {
				return nil, err
			}
			// This Redis handler runs the Agent directly instead of going through
			// Runner, so it must establish the same per-session invocation boundary.
			// Jobs for one session are serialized; jobs for other sessions still run
			// concurrently on the worker pool.
			release, err := sess.BeginInvocation(ctx)
			if err != nil {
				return nil, err
			}
			defer release()

			// Bind history and prompt state to the revision observed after acquiring
			// the gate. Supplying the snapshot explicitly also covers custom Agent
			// implementations that do not have LLMAgent's snapshot fallback.
			snapshot := sess.Snapshot()
			// NOTE: Redis is at-least-once — a job can be redelivered after a crash,
			// so a production handler should dedupe on the job id before appending.
			ictx := agent.InvocationContext{
				Context:         ctx,
				Agent:           ag,
				Root:            ag,
				Session:         sess,
				SessionSnapshot: &snapshot,
				UserContent:     core.UserText(p.Prompt),
			}
			var final *core.Event
			for ev, err := range ag.Run(ictx) {
				if err != nil {
					return nil, err
				}
				if ev == nil {
					continue
				}
				if ev.Partial {
					emit(ev)
					continue
				}
				if err := store.Append(ctx, sess, ev); err != nil {
					return nil, err
				}
				final = ev
			}
			return final, nil
		},
	}

	// One WithRedis switches both the queue (Stream) and the bus (Pub/Sub).
	opts := []queue.Option{
		queue.WithRedis(redisURL),
		queue.WithStream("goagent:jobs"),
		queue.WithGroup("workers"),
		queue.WithRegistry(reg),
		queue.WithIdleThreshold(30 * time.Second), // > this short job's runtime
		queue.WithMaxDeliveries(3),                // poison cap -> DLQ
	}
	q, consumer, err := queue.New(opts...)
	if err != nil {
		log.Fatalf("queue.New: %v", err)
	}
	bus, err := queue.NewBus(opts...)
	if err != nil {
		log.Fatalf("queue.NewBus: %v", err)
	}
	go queue.NewWorker(queue.Config{Consumer: consumer, Bus: bus, Registry: reg}).Run(ctx)

	app, user, sid := "demo", "u1", "s1"

	// The frontend subscribes to the session's stream over Redis Pub/Sub.
	ch, unsub := bus.Subscribe(runner.BusKey(app, user, sid))
	defer unsub()

	// Enqueue a serialized job — returns immediately, does NOT block on generation.
	payload, _ := json.Marshal(agentPayload{
		App: app, User: user, Session: sid, Agent: "video",
		Prompt: "a cat walking on the beach at sunset",
	})
	jobID := core.NewID("job")
	if err := q.Enqueue(ctx, queue.Job{
		ID:      jobID,
		Key:     runner.BusKey(app, user, sid),
		Type:    "agent",
		Payload: payload,
	}); err != nil {
		log.Fatalf("enqueue: %v", err)
	}
	fmt.Printf("enqueued %s on Redis (non-blocking); streaming events:\n", jobID)

	// Guard against a hung run so the demo always exits.
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			log.Fatal("timed out waiting for result")
		case ev, ok := <-ch:
			if !ok {
				return
			}
			switch {
			case ev.Message != nil: // final result
				for _, p := range ev.Message.Parts {
					if v, ok := p.(core.Video); ok {
						fmt.Printf("  ✓ done: %s (%dms)\n", v.URL, v.DurationMs)
					}
				}
				return
			case ev.Progress != nil:
				if ev.Progress.Status == "failed" {
					fmt.Printf("  ✗ failed: %s\n", ev.Progress.Err)
					return
				}
				fmt.Printf("  [%s] %s %d%%\n", ev.Progress.Kind, ev.Progress.Status, ev.Progress.Percent)
			}
		}
	}
}

// pingRedis fails fast with a clear message if no server is reachable, rather
// than letting the first XADD surface a cryptic dial error.
func pingRedis(ctx context.Context, url string) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		log.Fatalf("bad REDIS_URL %q: %v", url, err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pctx).Err(); err != nil {
		log.Fatalf("cannot reach Redis at %s: %v\n(start one with: docker run -p 6379:6379 redis:7)", url, err)
	}
}
