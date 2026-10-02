package agent

import (
	"context"
	"fmt"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

// This file holds the human-in-the-loop pause/continue closure — the part of
// the runtime that turns a BeforeTool Interrupt into a durable pause and back
// into a continued run. Pausing lives in the loop (it writes a PendingHITL
// checkpoint and emits Interrupted); resuming lives here, where the decisions
// are recorded, and the calls they unlock run in the loop via runResumed.

// Approval is a human decision about one pending tool call.
type Approval struct {
	CallID  string
	Approve bool
	Reason  string // rejection reason (fed back to the model so it can re-route)
}

// Allow approves a pending tool call by id.
func Allow(callID string) Approval { return Approval{CallID: callID, Approve: true} }

// Reject denies a pending tool call by id, with a reason reported to the model.
func Reject(callID, reason string) Approval {
	return Approval{CallID: callID, Reason: reason}
}

// Decide records a decision for a pending tool call on this (paused) run. Call
// Resume after recording all decisions.
func (r *Run) Decide(ap Approval) {
	r.mu.Lock()
	r.decisions = append(r.decisions, ap)
	r.mu.Unlock()
}

// Resume continues this paused run, applying the decisions recorded with Decide.
// It returns a fresh *Run for the continued execution.
func (r *Run) Resume(ctx context.Context) (*Run, error) {
	r.mu.Lock()
	decs := append([]Approval(nil), r.decisions...)
	r.mu.Unlock()
	return r.agent.Resume(ctx, r.ThreadID, decs...)
}

// Resume continues a thread from its latest checkpoint. If that checkpoint is a
// HITL pause (has Pending tool calls), the given approvals are applied:
// approved calls execute, rejected (or undecided) calls become error
// ToolResults reported to the model.
//
// It is also how a failed run is recovered: the loop checkpoints the last
// replayable seam when a model call or a gate errors, so resuming continues the
// conversation where it stopped instead of rewinding to the last completed step.
//
// The approved batch does not execute here: it is handed to the resumed run's
// loop (RunContext.resumed, runResumed below), so an approved call goes through
// exactly the path a call executed inside a step goes through — ToolStarted /
// ToolDone events, AfterTool hooks, state mutations, and its Result Control
// folded into the run's control flow. Running it here instead would give one
// conversation a second, weaker executor.
func (a *Agent) Resume(ctx context.Context, threadID string, approvals ...Approval) (*Run, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return nil, err
	}
	cp, err := a.store.Latest(ctx, threadID)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, fmt.Errorf("agent: no checkpoint to resume for thread %q", threadID)
	}
	state := cloneState(cp.State)
	applyFileSnapshot(&state, cp.FileSnapshot)
	if state.Files == nil {
		state.Files = vfs.NewInState()
	}
	// Stash decisions in a generic State slot so non-LLM runnables (the DAG plan
	// executor) can consume them too; the LLM path below reads the approvals
	// directly. Merge into any existing decisions so per-node approvals accumulate
	// across multiple pause/resume waves.
	if len(approvals) > 0 {
		if state.KV == nil {
			state.KV = map[string]any{}
		}
		merged := map[string]any{}
		if old, ok := state.KV[approvalsKey].(map[string]any); ok {
			for k, v := range old {
				merged[k] = v
			}
		}
		for _, ap := range approvals {
			if ap.Approve {
				merged[ap.CallID] = "allow"
			} else {
				merged[ap.CallID] = "reject"
			}
		}
		state.KV[approvalsKey] = merged
	}
	run := a.newRunHandle(ctx, threadID, &state)

	if cp.Pending == nil || len(cp.Pending.Pending) == 0 {
		return run, nil
	}
	if a.loop == nil {
		// A workflow agent has no tool table of its own, so it cannot run the calls
		// an LLM agent paused on. Report each pending call as unexecuted; the KV
		// slot above is what its DAG executor reads instead.
		parts := make([]core.Part, 0, len(cp.Pending.Pending))
		for _, c := range cp.Pending.Pending {
			parts = append(parts, errResult(c, "not executed: the thread was resumed by a workflow agent, which has no tool table of its own"))
		}
		run.rc.State.Messages = append(run.rc.State.Messages, core.Message{Role: core.RoleTool, Parts: parts})
		return run, nil
	}
	run.rc.resumed = &resumeBatch{
		step:   cp.Pending.Step,
		calls:  cp.Pending.Pending,
		decide: decisionsBy(approvals),
		final:  lastAssistant(state.Messages),
	}
	return run, nil
}

// resumeBatch is the pending tool-call batch a HITL pause left behind, carried
// into the resumed run so the loop runs it before its next model call.
type resumeBatch struct {
	step   int             // loop step the run paused at
	calls  []core.ToolCall // pending calls, in the model's original order
	decide map[string]Approval
	final  core.Message // assistant message that issued the calls, if a call ends the run
}

// runResumed executes a resumeBatch under the step context the loop built for it
// and returns its tool results in the model's original call order, the batch's
// folded directive, and the calls this batch rejected. Approved calls go through
// execTools as one batch — so they inherit its concurrency decision, its events,
// its AfterTool hooks and its immediate state application; denied or undecided
// calls never reach a handler.
func (l *AgentLoop) runResumed(rb *resumeBatch, lc *LoopContext) ([]core.Part, core.Directive, []ToolRejection) {
	rc := lc.RunContext

	approved := make([]core.ToolCall, 0, len(rb.calls))
	for _, c := range rb.calls {
		if ap, ok := rb.decide[c.ID]; ok && ap.Approve {
			approved = append(approved, c)
		}
	}
	results, dirs, rejects := l.execTools(rc, lc, approved)

	parts := make([]core.Part, 0, len(rb.calls))
	next := 0
	for _, c := range rb.calls {
		ap, recorded := rb.decide[c.ID]
		if recorded && ap.Approve {
			parts = append(parts, results[next])
			next++
			continue
		}
		parts = append(parts, deniedResult(rc, c, rejectionReason(ap, recorded)))
	}
	return parts, core.Resolve(dirs...), rejects
}

// rejectionReason words a denial for the model, which may re-route on it: an
// explicit rejection carries its reason, an absent decision says so.
func rejectionReason(ap Approval, recorded bool) string {
	switch {
	case recorded && ap.Reason != "":
		return "rejected: " + ap.Reason
	case recorded:
		return "rejected by human"
	default:
		return "rejected: no decision provided"
	}
}

func decisionsBy(approvals []Approval) map[string]Approval {
	out := make(map[string]Approval, len(approvals))
	for _, ap := range approvals {
		out[ap.CallID] = ap
	}
	return out
}

// lastAssistant returns the most recent assistant message, which in a paused
// history is the one that issued the pending calls.
func lastAssistant(msgs []core.Message) core.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == core.RoleAssistant {
			return msgs[i]
		}
	}
	return core.Message{}
}

// deniedResult turns a tool call a human gate refused (or left undecided) into
// an error result reported to the model. It publishes the ToolStarted/ToolDone
// pair so the decision is visible on the event stream, but the handler never
// runs — so no AfterTool hook sees it, the same contract truncatedResults uses.
// It is deliberately not a ToolRejection either: a human saying no is a decision,
// not a sign the call was malformed, and counting it would let a gate trip an
// argument guard.
func deniedResult(rc *RunContext, c core.ToolCall, reason string) core.ToolResult {
	rc.publish(core.ToolStarted{Call: c})
	tr := errResult(c, reason)
	rc.publish(core.ToolDone{Result: tr})
	return tr
}
