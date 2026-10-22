package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
// branch, then merges their results. Events from all branches interleave on the
// shared topic. Branch KV writes are folded back deterministically in branch
// declaration order; conflicting writes to the same key are rejected (use
// ParallelWithOptions to prefer a branch instead). Final texts are concatenated.
func Parallel(name string, subs ...*Agent) *Agent {
	return ParallelWithOptions(name, ParallelOptions{}, subs...)
}

// ParallelWithOptions builds a Parallel workflow with an explicit merge policy.
func ParallelWithOptions(name string, opts ParallelOptions, subs ...*Agent) *Agent {
	return wrapWorkflow(name, &parallelRunner{subs: subs, opts: opts})
}

// StateConflictPolicy controls deterministic resolution when multiple parallel
// branches write different values to the same State.KV key.
type StateConflictPolicy int

const (
	// RejectStateConflicts fails the merge if two branches set the same key to
	// different values (the safe default — surfaces accidental contention).
	RejectStateConflicts StateConflictPolicy = iota
	// PreferEarlierBranch keeps the value from the branch listed first.
	PreferEarlierBranch
	// PreferLaterBranch keeps the value from the branch listed last.
	PreferLaterBranch
)

// FailurePolicy controls what a Parallel workflow does when a branch fails.
type FailurePolicy int

const (
	// CancelSiblings cancels the remaining branches as soon as a branch fails.
	// The run still waits for every started branch to exit before reporting.
	CancelSiblings FailurePolicy = iota
	// WaitForAll lets every branch run to completion before reporting the first
	// failure in declaration order.
	WaitForAll
)

// ParallelOptions configures branch execution and merge behavior without
// changing the simple Parallel constructor. The zero value cancels siblings on
// the first failure and rejects conflicting state writes.
type ParallelOptions struct {
	Failure        FailurePolicy
	MaxConcurrency int
	StateConflict  StateConflictPolicy
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
	return wrapWorkflowWithStore(name, r, nil)
}

func wrapWorkflowWithStore(name string, r Runnable, store checkpoint.Checkpointer) *Agent {
	if store == nil {
		store = checkpoint.NewMemory()
	}
	c := config{
		name:            name,
		taskKind:        TaskKindWorkflow,
		taskBudget:      defaultTaskBudget(),
		toolConcurrency: defaultToolConcurrency,
	}
	return &Agent{cfg: c, runnable: r, bus: bus.New(), store: store}
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

type parallelRunner struct {
	subs []*Agent
	opts ParallelOptions
}

// branchResult carries one branch's outcome back to the runner together with
// its declaration index, so results are ordered deterministically regardless of
// completion order.
type branchResult struct {
	index int
	out   runOutcome
}

func isRealFailure(out runOutcome) bool { return out.Err != nil }

func (p *parallelRunner) run(rc *RunContext) runOutcome {
	// Baseline KV captured before any branch forks, so each branch's writes can
	// be diffed against a common ancestor and folded deterministically.
	baseline := cloneKV(rc.State.KV)

	// Branch failures cancel siblings through this context only; the parent run
	// keeps running so it can collect results and report the root error.
	branchCtx, cancelBranches := context.WithCancel(rc.Context)
	defer cancelBranches()

	limit := p.opts.MaxConcurrency
	if limit <= 0 || limit > len(p.subs) {
		limit = len(p.subs)
	}
	results := make(chan branchResult, len(p.subs))
	children := make([]*RunContext, len(p.subs))
	settled := make([]bool, len(p.subs))
	announced := make([]bool, len(p.subs))
	next, inflight := 0, 0
	var collected []branchResult
	var failure, launchErr error

	// announceCanceled reports each started branch that has not yet returned,
	// at most once, so both cancellation triggers publish a consistent set.
	announceCanceled := func(reason error) {
		for i, child := range children {
			if child == nil || settled[i] || announced[i] {
				continue
			}
			announced[i] = true
			rc.publish(core.BranchCanceled{Name: child.branch, Reason: reason})
		}
	}

	for {
		for next < len(p.subs) && inflight < limit && launchErr == nil &&
			branchCtx.Err() == nil && (failure == nil || p.opts.Failure == WaitForAll) {
			i := next
			child, err := rc.forBranchWithContext(branchCtx, p.subs[i].cfg.name, p.subs[i].taskKind())
			if err != nil {
				launchErr = err
				cancelBranches()
				announceCanceled(err)
				break
			}
			children[i] = child
			next++
			inflight++
			info := child.taskInfo()
			rc.publish(core.TaskStarted{
				TaskID:   info.ID,
				ParentID: info.ParentID,
				ThreadID: info.ThreadID,
				Kind:     string(info.Kind),
				Depth:    info.Depth,
			})
			go func(i int) {
				out := runChild(children[i], p.subs[i].runnable)
				results <- branchResult{index: i, out: out}
			}(i)
		}
		if inflight == 0 {
			break
		}

		r := <-results
		inflight--
		settled[r.index] = true
		collected = append(collected, r)
		// TaskDone is published when the result is collected, not inside the
		// branch goroutine, so its order relative to BranchCanceled is stable.
		info := children[r.index].taskInfo()
		rc.publish(core.TaskDone{
			TaskID: info.ID,
			Kind:   string(info.Kind),
			Status: taskStatus(r.out),
			Err:    r.out.Err,
		})
		if failure == nil && isRealFailure(r.out) {
			failure = r.out.Err
			if p.opts.Failure == CancelSiblings {
				cancelBranches()
				announceCanceled(failure)
			}
		}
	}

	if failure != nil || launchErr != nil || branchCtx.Err() != nil {
		// Errors take priority over a pause: a failed run must not report a
		// sibling's Interrupted outcome as the final state.
		return runOutcome{Err: chooseRootError(rc.Context, collected, launchErr)}
	}
	outs := make([]runOutcome, len(p.subs))
	for _, r := range collected {
		outs[r.index] = r.out
	}
	return p.merge(rc, baseline, children, outs)
}

var _ Runnable = (*parallelRunner)(nil)

// chooseRootError picks the error a failed Parallel run reports. The parent
// context wins; then the first branch error in declaration order that is not a
// cancellation; then the launch error (a budget refusal must not be hidden by
// canceled siblings); then the first error at all, so a failure is never
// reported as success. It deliberately does not use whether a branch's context
// was canceled when it returned: a real failure that arrives after cancellation
// is still the root cause.
func chooseRootError(parentCtx context.Context, collected []branchResult, fallback error) error {
	if err := parentCtx.Err(); err != nil {
		return err
	}
	var root, anyErr error
	rootIdx, anyIdx := 0, 0
	for _, r := range collected {
		if r.out.Err == nil {
			continue
		}
		if anyErr == nil || r.index < anyIdx {
			anyErr, anyIdx = r.out.Err, r.index
		}
		if errors.Is(r.out.Err, context.Canceled) {
			continue
		}
		if root == nil || r.index < rootIdx {
			root, rootIdx = r.out.Err, r.index
		}
	}
	if root != nil {
		return root
	}
	if fallback != nil {
		return fallback
	}
	return anyErr
}

// merge folds parallel branch outcomes. The first error (or non-completion
// control, e.g. Interrupt) short-circuits and propagates. Otherwise each
// branch's KV diff against the baseline is folded by the conflict policy and
// applied to the shared State, and the branches' final texts are concatenated.
func (p *parallelRunner) merge(rc *RunContext, baseline map[string]any, branches []*RunContext, rs []runOutcome) runOutcome {
	texts := make([]string, 0, len(rs))
	patches := make([]statePatch, len(rs))
	for i, r := range rs {
		if r.Err != nil {
			return runOutcome{Err: r.Err}
		}
		// A branch that paused (HITL) or asked to Stop/Escalate can't be merged
		// deterministically; surface its control to the enclosing workflow.
		if r.terminal() || r.Control.Kind == core.Stop || r.Control.Kind == core.Escalate {
			return r
		}
		patches[i] = diffKV(baseline, branches[i].State.KV)
		if t := r.Result.Message.Text(); t != "" {
			texts = append(texts, t)
		}
	}
	merged, err := mergeKVPatches(patches, p.opts.StateConflict)
	if err != nil {
		return runOutcome{Err: err}
	}
	applyKVPatch(rc.State, merged)
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
