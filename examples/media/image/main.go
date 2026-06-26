// Command image is a "complex" image-generation example against the real Agnes
// gateway. It shows the ImageAgent at scale: one concept rendered into a whole
// multi-format poster set CONCURRENTLY, by composing several ImageAgents — each
// pinned to a different output size — under a ParallelAgent. A shared seed keeps
// the subject consistent across formats. Generated images are streamed live and
// downloaded to ./out.
//
// Run:
//
//	export AGNES_API_KEY=sk-...
//	go run ./examples/media/image "a samurai cat standing on a neon rooftop, rain"
//
// The prompt is optional; a default is used when omitted.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/agnes"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

const (
	imageModel = "agnes-image-2.1-flash"
	outDir     = "out"
	seed       = 42 // shared seed → consistent subject across formats
)

// format defines one poster variant: a distinct agent name + output size.
type format struct {
	name string
	size string
}

var formats = []format{
	{"portrait", "768x1024"}, // poster
	{"square", "1024x1024"},  // social
	{"banner", "1536x640"},   // hero / cover
}

func main() {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		log.Fatal("set AGNES_API_KEY to run this example")
	}
	concept := "a samurai cat standing on a neon rooftop at night, heavy rain, cinematic"
	if len(os.Args) > 1 {
		concept = strings.Join(os.Args[1:], " ")
	}

	// Cancel cleanly on Ctrl+C so in-flight HTTP calls stop.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// One Agnes model, shared by every agent. A generous client timeout covers
	// the synchronous image endpoint.
	model := agnes.Image(imageModel, key,
		agnes.WithHTTPClient(&http.Client{Timeout: 90 * time.Second}))

	// Build one ImageAgent per format, each with its own default size + a shared
	// seed and "hd" quality. Then fan them out with a ParallelAgent: all formats
	// render at once, each on its own branch.
	agents := make([]agent.Agent, len(formats))
	for i, f := range formats {
		agents[i] = agent.Image("poster-"+f.name, model,
			llm.WithImageSize(f.size),
			llm.WithImageQuality("hd"),
			llm.WithImageSeed(seed),
		)
	}
	studio := agent.Parallel("poster-studio", agents...)

	r := runner.New(runner.Config{AppName: "poster", Root: studio, Store: session.InMemory()})

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatal(err)
	}

	banner("Agnes 多版式海报生成（Parallel · " + fmt.Sprint(len(formats)) + " 个 ImageAgent 并发）")
	fmt.Printf("concept: %s\n\n", concept)

	saved := 0
	for ev, err := range r.Run(ctx, "u1", "s1", core.UserText(concept)) {
		if err != nil {
			log.Fatalf("generation failed: %v", err)
		}
		if ev == nil {
			continue
		}
		switch {
		case ev.Err != nil:
			fmt.Printf("   ✗ %-16s %v\n", ev.Author, ev.Err)
		case ev.Progress != nil && ev.Message == nil:
			fmt.Printf("   … %-16s %s\n", ev.Author, ev.Progress.Status)
		case ev.Message != nil:
			for _, p := range ev.Message.Parts {
				img, ok := p.(core.Image)
				if !ok {
					continue
				}
				path := filepath.Join(outDir, ev.Author+".png")
				if err := saveImage(ctx, img, path); err != nil {
					fmt.Printf("   ⚠ %-16s got image but save failed: %v (url=%s)\n", ev.Author, err, img.URL)
					continue
				}
				saved++
				fmt.Printf("   ✓ %-16s → %s\n", ev.Author, path)
			}
		}
	}

	fmt.Printf("\nsaved %d/%d posters into ./%s/\n", saved, len(formats), outDir)
}

// saveImage writes a generated image to disk, handling both delivery modes:
// inline bytes (Data) or a URL the gateway returned (the Agnes default), which
// we download.
func saveImage(ctx context.Context, img core.Image, path string) error {
	if len(img.Data) > 0 {
		return os.WriteFile(path, img.Data, 0o644)
	}
	if img.URL == "" {
		return fmt.Errorf("image has neither data nor url")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, img.URL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download status %d", resp.StatusCode)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func banner(title string) {
	line := strings.Repeat("═", 60)
	fmt.Printf("%s\n  %s\n%s\n", line, title, line)
}
