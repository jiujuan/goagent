// Command chain demonstrates that the three agent kinds — LLMAgent (text),
// ImageAgent, and VideoAgent — are NOT three separate APIs but three
// implementations of one Agent contract, driven through one Runner and composed
// with the same workflow primitives.
//
// The pipeline is a single Sequential:
//
//	Sequential「studio」
//	├─ expander  LLMAgent   把用户一句话扩写成丰富的视觉 prompt → state[art.prompt]
//	├─ painter   ImageAgent 读 art.prompt 文生图（流式进度 + 成品图）
//	└─ director  VideoAgent 读 art.prompt 文生视频（轮询进度 + 成品视频）
//
// The same r.Run(...) loop streams text tokens, image progress, and video
// progress uniformly — the consumer never special-cases the agent kind. It runs
// fully offline on mock models; swap in agnes.Image / agnes.Video for the real
// gateway (see ../image and ../video).
package main

import (
	"context"
	"fmt"
	"iter"
	"log"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

const promptKey = "art.prompt"

func main() {
	// Stage 1 — a text LLM that expands a brief into a rich visual prompt and
	// publishes it to session state via OutputKey. (Mock, so no API key needed.)
	expander := agent.New(agent.Config{
		Name:            "expander",
		Description:     "expands a short brief into a detailed visual prompt",
		Instruction:     "Rewrite the user's brief as a vivid, detailed image/video prompt.",
		OutputKey:       promptKey, // final text → state[art.prompt]
		DisableTransfer: true,
		Model:           mock.New("expander-llm", expand),
	})

	// Stage 2 & 3 — media agents. They read their prompt from UserContent, so we
	// wrap each in a tiny adapter that swaps in the expanded prompt from state.
	// This is the only "glue": it shows media agents are ordinary Agents whose
	// input you can route, exactly like any other.
	painter := fromState(promptKey, agent.Image("painter", mockImage{}))
	director := fromState(promptKey, agent.Video("director", mockVideo{}))

	studio := agent.Sequential("studio", expander, painter, director)

	r := runner.New(runner.Config{AppName: "studio", Root: studio, Store: session.InMemory()})

	banner("goagent 媒体流水线：LLM → 图片 → 视频（同一套 Agent 契约 / 同一个 Runner）")
	for ev, err := range r.Run(context.Background(), "u1", "s1", core.UserText("a fox reading a book by candlelight")) {
		if err != nil {
			log.Fatal(err)
		}
		printEvent(ev)
	}
}

// --- prompt-routing adapter -------------------------------------------------

// stateAgent wraps a child agent and feeds it a prompt pulled from session
// state (written upstream via OutputKey). It implements Agent itself, so it
// drops straight into any workflow slot.
type stateAgent struct {
	key   string
	child agent.Agent
}

func fromState(key string, child agent.Agent) *stateAgent { return &stateAgent{key, child} }

func (a *stateAgent) Name() string        { return a.child.Name() }
func (a *stateAgent) Description() string { return a.child.Description() }
func (a *stateAgent) SubAgents() []agent.Agent {
	return nil
}

func (a *stateAgent) Run(ictx agent.InvocationContext) core.Stream {
	// ictx is a value; mutating our copy's UserContent only affects the child.
	if v, ok := ictx.Session.State().Get(a.key); ok {
		if s, ok := v.(string); ok && s != "" {
			ictx.UserContent = core.UserText(s)
		}
	}
	return a.child.Run(ictx)
}

// --- mock models (offline) --------------------------------------------------

// expand is the LLM responder: it reads the latest user brief and returns an
// embellished prompt.
func expand(req *llm.Request) *llm.Response {
	brief := lastUserText(req)
	return mock.Text(brief + " — cinematic lighting, ultra-detailed, 35mm film, warm golden tones")
}

// mockImage is a synchronous llm.ImageModel: it "renders" a URL from the prompt.
type mockImage struct{}

func (mockImage) Name() string { return "mock-image" }

func (mockImage) GenerateImage(ctx context.Context, req *llm.ImageRequest) iter.Seq2[*llm.ImageResponse, error] {
	return func(yield func(*llm.ImageResponse, error) bool) {
		select {
		case <-ctx.Done():
			yield(nil, ctx.Err())
			return
		case <-time.After(250 * time.Millisecond): // simulate render latency
		}
		url := "https://img.example/" + slug(req.Prompt) + ".png"
		yield(&llm.ImageResponse{Images: []core.Image{{MIME: "image/png", URL: url}}}, nil)
	}
}

// mockVideo is an asynchronous llm.VideoModel: it emits queued→running→done.
type mockVideo struct{}

func (mockVideo) Name() string { return "mock-video" }

func (mockVideo) GenerateVideo(ctx context.Context, req *llm.VideoRequest) iter.Seq2[*llm.VideoProgress, error] {
	return func(yield func(*llm.VideoProgress, error) bool) {
		steps := []llm.VideoProgress{
			{JobID: "job-mock", Status: llm.JobQueued, Percent: 0},
			{JobID: "job-mock", Status: llm.JobRunning, Percent: 45},
			{JobID: "job-mock", Status: llm.JobRunning, Percent: 85},
			{JobID: "job-mock", Status: llm.JobSucceeded, Percent: 100,
				Video: &core.Video{MIME: "video/mp4", URL: "https://vid.example/" + slug(req.Prompt) + ".mp4", DurationMs: 5000}},
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

// --- rendering --------------------------------------------------------------

func printEvent(ev *core.Event) {
	if ev == nil {
		return
	}
	switch {
	case ev.Err != nil:
		fmt.Printf("   ✗ %-9s %v\n", ev.Author, ev.Err)
	case ev.Message != nil:
		printMessage(ev)
	case ev.Progress != nil:
		fmt.Printf("   … %-9s [%s] %s %d%%\n", ev.Author, ev.Progress.Kind, ev.Progress.Status, ev.Progress.Percent)
	}
}

func printMessage(ev *core.Event) {
	m := ev.Message
	switch m.Role {
	case core.RoleUser:
		fmt.Printf("\n👤 user      %s\n", m.Text())
	case core.RoleAssistant:
		if t := strings.TrimSpace(m.Text()); t != "" {
			fmt.Printf("   ✎ %-9s prompt ⇒ %s\n", ev.Author, t)
		}
		for _, p := range m.Parts {
			switch v := p.(type) {
			case core.Image:
				fmt.Printf("   🖼 %-9s image  ⇒ %s\n", ev.Author, v.URL)
			case core.Video:
				fmt.Printf("   🎬 %-9s video  ⇒ %s (%.1fs)\n", ev.Author, v.URL, float64(v.DurationMs)/1000)
			}
		}
	}
}

// --- small helpers ----------------------------------------------------------

func lastUserText(req *llm.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == core.RoleUser {
			return req.Messages[i].Text()
		}
	}
	return ""
}

func slug(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ' || r == '-':
			b.WriteByte('-')
		}
		if b.Len() >= 32 {
			break
		}
	}
	return strings.Trim(b.String(), "-")
}

func banner(title string) {
	line := strings.Repeat("═", 60)
	fmt.Printf("%s\n  %s\n%s\n", line, title, line)
}
