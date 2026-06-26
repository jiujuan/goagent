// Command video is a "complex" video-generation example against the real Agnes
// gateway. Video is asynchronous and long-running (submit → poll for minutes),
// so this example highlights the full job lifecycle AND the capability that sets
// video apart from text/image: RESUMABILITY. Agnes implements
// llm.ResumableVideoModel, so a crashed or restarted process can reattach to an
// in-flight job by its id instead of paying to regenerate.
//
// Two subcommands:
//
//	export AGNES_API_KEY=sk-...
//	go run ./examples/media/video                       # submit + stream a live progress bar
//	go run ./examples/media/video resume <job-id>       # reattach to an existing job and finish
//
// The first prints the job id up front; if it dies mid-flight, rerun with
// `resume <job-id>` to pick up where it left off. To use this model inside a
// workflow, wrap it with agent.Video(name, model) — see ../chain.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/agnes"
)

const videoModel = "agnes-video-v2.0"

func main() {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		log.Fatal("set AGNES_API_KEY to run this example")
	}

	// Ctrl+C cancels the context, which stops polling promptly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Poll every 5s (the Agnes-recommended cadence); a long HTTP timeout suits
	// the minute-scale endpoint.
	model := agnes.Video(videoModel, key, agnes.WithPollInterval(5*time.Second))

	if len(os.Args) > 2 && os.Args[1] == "resume" {
		resume(ctx, model, os.Args[2])
		return
	}
	generate(ctx, model)
}

// generate submits a new job with a rich set of options and streams its progress
// to a live bar, ending with the finished video URL.
func generate(ctx context.Context, model llm.VideoModel) {
	req := &llm.VideoRequest{
		Prompt: "a paper boat sailing down a rain-soaked city gutter, leaves swirling, shot at street level",
		Options: llm.VideoOptions{
			Width:          1152,
			Height:         768,
			NumFrames:      121, // ~5s at 24fps
			FrameRate:      24,
			NegativePrompt: "blurry, distorted, low quality, watermark",
		},
	}
	req.Options.Apply(llm.WithVideoSeed(7)) // reproducible

	banner("Agnes 视频生成（异步：提交 → 轮询 → 取回）")
	fmt.Printf("prompt: %s\n\n", req.Prompt)

	idShown := false
	for p, err := range model.GenerateVideo(ctx, req) {
		if err != nil {
			log.Fatalf("\ngeneration failed: %v", err)
		}
		if !idShown && p.JobID != "" {
			fmt.Printf("job id: %s\n  (if this dies, resume with:  go run ./examples/media/video resume %s)\n\n", p.JobID, p.JobID)
			idShown = true
		}
		if done := render(p); done {
			return
		}
	}
}

// resume reattaches to an existing job id — the crash-recovery path. It type-
// asserts the optional ResumableVideoModel capability.
func resume(ctx context.Context, model llm.VideoModel, jobID string) {
	rm, ok := model.(llm.ResumableVideoModel)
	if !ok {
		log.Fatalf("%s does not support resume", model.Name())
	}
	banner("Agnes 视频恢复（凭 JobID 重连，不重复付费）")
	fmt.Printf("resuming job: %s\n\n", jobID)

	for p, err := range rm.ResumeVideo(ctx, jobID) {
		if err != nil {
			log.Fatalf("\nresume failed: %v", err)
		}
		if done := render(p); done {
			return
		}
	}
}

// render draws one progress update and reports whether the job reached a
// terminal state (so the caller can stop).
func render(p *llm.VideoProgress) (done bool) {
	switch p.Status {
	case llm.JobSucceeded:
		fmt.Printf("\r%s 100%%  done\n", bar(100))
		if p.Video != nil {
			fmt.Printf("\n✓ video: %s  (%.1fs)\n", p.Video.URL, float64(p.Video.DurationMs)/1000)
		}
		return true
	case llm.JobFailed:
		fmt.Printf("\n✗ failed: %s\n", p.Err)
		return true
	default: // queued / running
		fmt.Printf("\r%s %3d%%  %s", bar(p.Percent), p.Percent, p.Status)
		return false
	}
}

// bar renders a fixed-width progress bar for a percentage.
func bar(pct int) string {
	const width = 30
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := pct * width / 100
	return "[" + strings.Repeat("#", filled) + strings.Repeat("-", width-filled) + "]"
}

func banner(title string) {
	line := strings.Repeat("═", 60)
	fmt.Printf("%s\n  %s\n%s\n", line, title, line)
}
