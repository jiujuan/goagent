// Command loop-guard is a runnable offline tour of middleware.LoopGuard: the
// stuck-pattern detector for long-horizon runs. A deliberately stubborn model
// repeats one identical tool call forever; the guard warns it, pauses for a
// human (HITL), and finally stops the run when the intervention budget runs
// out — all without burning the loop's MaxTurns.
//
//	go run ./examples/loop-guard
//
// 无 API key 依赖：全程使用 llm/mock。
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	fetches := 0
	fetch := tool.New("fetch", "fetch a document", func(_ *tool.Context, _ struct{}) (string, error) {
		fetches++
		return "the same page again", nil
	})

	// The stubborn model: every turn re-issues the identical call, ignoring
	// every warning.
	model := mock.New("stubborn", func(*llm.Request) *llm.Response {
		return mock.CallTool("c1", "fetch", "{}")
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(fetch),
		agent.WithMiddleware(middleware.LoopGuard(middleware.LoopGuardOptions{
			// defaults: warn after 2 identical batches, interrupt on the
			// recurrence, budget of 3 interventions then hard stop
		})),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	run := a.Stream(ctx, "read the manual for me")
	for wave := 1; ; wave++ {
		fmt.Printf("\n=== wave %d ===\n", wave)
		var pending []core.ApprovalRequest
		var terminal string
		for ev := range collect(run) {
			switch e := ev.(type) {
			case core.StuckDetected:
				fmt.Printf("  [guard] step %d rule=%s: %s\n", e.Step, e.Rule, e.Reason)
			case core.ToolDone:
				fmt.Printf("  [tool ] %s ran (total %d)\n", e.Result.Name, fetches)
			case core.Interrupted:
				pending = e.Pending
				terminal = "interrupted (waiting for a human)"
			case core.RunDone:
				terminal = "done"
			case core.RunFailed:
				terminal = fmt.Sprintf("failed: %v", e.Err)
			}
		}
		fmt.Printf("  run settled: %s\n", terminal)
		if len(pending) > 0 {
			fmt.Printf("  [human] approving %s so the run continues\n", pending[0].Tool)
			run.Decide(agent.Allow(pending[0].CallID))
			next, err := run.Resume(ctx)
			if err != nil {
				log.Fatal(err)
			}
			run = next
			continue
		}
		return
	}
}

// collect drains one run's events into a channel, ending at the terminal
// event (done / failed / interrupted).
func collect(run *agent.Run) <-chan core.Event {
	ch := make(chan core.Event)
	go func() {
		defer close(ch)
		for ev, err := range run.Iter() {
			if err != nil {
				return
			}
			ch <- ev
		}
	}()
	return ch
}
