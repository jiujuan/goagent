package agent

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// This file holds the deterministic workflow agents. They are *Agent values too
// — uniform with LLM agents, so they have the same Run/Stream/Resume surface —
// but their runnable orchestrates child agents instead of calling a model. A
// workflow drives children by calling child.runnable.run(rc) and composes by
// the returned runOutcome (e.g. inspecting Escalate), while children publish
// their internal events to the shared bus/topic.

// Sequential runs sub-agents in order, sharing one State so each stage's output
// flows to the next. It stops early if a sub fails, pauses (Interrupt), or
// returns Stop/Escalate.
func Sequential(name string, subs ...*Agent) *Agent {
	return wrapWorkflow(name, &sequentialRunner{subs: subs})
}

// Parallel runs sub-agents concurrently, each on an isolated (cloned) State
// branch, then merges their final texts. Events from all branches interleave on
// the shared topic.
func Parallel(name string, subs ...*Agent) *Agent {
	return wrapWorkflow(name, &parallelRunner{subs: subs})
}

// Loop runs sub-agents in sequence repeatedly until a sub escalates (a tool/
// middleware returns Escalate — see ExitLoopTool) or maxIter is reached (0 =
// until escalation).
func Loop(name string, maxIter int, subs ...*Agent) *Agent {
	return wrapWorkflow(name, &loopRunner{subs: subs, maxIter: maxIter})
}

// wrapWorkflow builds an *Agent whose runnable is a workflow orchestrator. It
// gets its own bus/store so it is independently Run-able; children are driven
// with the workflow's RunContext, so their events land on the workflow's topic.
func wrapWorkflow(name string, r Runnable) *Agent {
	c := config{name: name}
	return &Agent{cfg: c, runnable: r, bus: bus.New(), store: checkpoint.NewMemory()}
}

// --- Sequential -------------------------------------------------------------

type sequentialRunner struct{ subs []*Agent }

func (s *sequentialRunner) run(rc *RunContext) runOutcome {
	var last runOutcome
	for _, sub := range s.subs {
		last = sub.runnable.run(rc)
		if last.terminal() { // error or HITL pause
			return last
		}
		if k := last.Control.Kind; k == core.Stop || k == core.Escalate {
			return last
		}
	}
	return last
}

var _ Runnable = (*sequentialRunner)(nil)

// --- Loop -------------------------------------------------------------------

type loopRunner struct {
	subs    []*Agent
	maxIter int
}

func (l *loopRunner) run(rc *RunContext) runOutcome {
	var last runOutcome
	for iter := 0; l.maxIter == 0 || iter < l.maxIter; iter++ {
		for _, sub := range l.subs {
			last = sub.runnable.run(rc)
			if last.terminal() {
				return last
			}
			if last.Control.Kind == core.Escalate {
				// Escalation breaks the loop cleanly (not an error).
				return runOutcome{Result: last.Result}
			}
			if last.Control.Kind == core.Stop {
				return last
			}
		}
	}
	return last
}

var _ Runnable = (*loopRunner)(nil)

// --- Parallel ---------------------------------------------------------------

type parallelRunner struct{ subs []*Agent }

func (p *parallelRunner) run(rc *RunContext) runOutcome {
	results := make([]runOutcome, len(p.subs))
	var wg sync.WaitGroup
	for i := range p.subs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			child := rc.forBranch(p.subs[i].cfg.name)
			results[i] = p.subs[i].runnable.run(child)
		}(i)
	}
	wg.Wait()
	return mergeOutcomes(results)
}

var _ Runnable = (*parallelRunner)(nil)

// mergeOutcomes folds parallel branch outcomes: the first error wins; otherwise
// the branches' final texts are concatenated into one assistant message.
func mergeOutcomes(rs []runOutcome) runOutcome {
	texts := make([]string, 0, len(rs))
	for _, r := range rs {
		if r.Err != nil {
			return runOutcome{Err: r.Err}
		}
		if t := r.Result.Message.Text(); t != "" {
			texts = append(texts, t)
		}
	}
	return runOutcome{Result: core.Result{Message: core.AssistantText(strings.Join(texts, "\n\n"))}}
}

// renderTemplate substitutes {{key}} placeholders in an instruction with values
// from State.KV — the read side of WithOutputKey, letting a later workflow stage
// reference an earlier stage's output.
func renderTemplate(tmpl string, kv map[string]any) string {
	if tmpl == "" || len(kv) == 0 || !strings.Contains(tmpl, "{{") {
		return tmpl
	}
	out := tmpl
	for k, v := range kv {
		out = strings.ReplaceAll(out, "{{"+k+"}}", fmt.Sprint(v))
	}
	return out
}

// --- ExitLoopTool -----------------------------------------------------------

// ExitLoopTool returns a tool an agent can call to break an enclosing Loop
// workflow. Calling it yields a result whose Control is Escalate, which the loop
// folds and the loopRunner reads to stop iterating.
func ExitLoopTool() tool.Tool { return exitTool{} }

type exitTool struct{}

func (exitTool) Name() string { return "exit_loop" }
func (exitTool) Description() string {
	return "Call when the work is good enough to stop the refinement loop."
}
func (exitTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"reason":{"type":"string"}}}`)
}

func (exitTool) Call(_ *tool.Context, args json.RawMessage) (*tool.Result, error) {
	var in struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(args, &in)
	msg := "exiting loop"
	if in.Reason != "" {
		msg += ": " + in.Reason
	}
	return &tool.Result{
		Content: []core.Part{core.Text{Text: msg}},
		Control: &core.Directive{Kind: core.Escalate, Reason: in.Reason},
	}, nil
}
