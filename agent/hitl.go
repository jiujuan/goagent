package agent

import (
	"context"
	"fmt"

	"github.com/jiujuan/goagent/checkpoint"
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
	if err := validatePause(cp); err != nil {
		return nil, err
	}
	var (
		durable          checkpoint.Durable
		claim            *checkpoint.Claim
		durableDecisions []core.ApprovalDecision
	)
	if store, ok := a.store.(checkpoint.Durable); ok {
		durable = store
		claimed, err := durable.Claim(ctx, checkpoint.ClaimRequest{
			ThreadID: threadID,
			Revision: cp.Revision,
			PauseID:  checkpoint.EffectivePauseID(cp),
			Owner:    core.NewID("resume"),
			Lease:    checkpoint.DefaultLease,
		})
		if err != nil {
			return nil, err
		}
		claim = &claimed
		durableDecisions, err = durable.Decisions(ctx, threadID, claimed.PauseID)
		if err != nil {
			_ = durable.ReleaseClaim(context.Background(), claimed)
			return nil, err
		}
	}
	durableApprovals, err := approvalsFromDurable(cp, durableDecisions)
	if err != nil {
		if claim != nil {
			_ = durable.ReleaseClaim(context.Background(), *claim)
		}
		return nil, err
	}
	allApprovals := append(durableApprovals, approvals...)
	state := cloneState(cp.State)
	applyFileSnapshot(&state, cp.FileSnapshot)
	if state.Files == nil {
		state.Files = vfs.NewInState()
	}
	// Stash decisions in a generic State slot so non-LLM runnables (the DAG plan
	// executor) can consume them too; the LLM path below reads the approvals
	// directly. Merge into any existing decisions so per-node approvals accumulate
	// across multiple pause/resume waves.
	if len(allApprovals) > 0 {
		if state.KV == nil {
			state.KV = map[string]any{}
		}
		merged := map[string]any{}
		if old, ok := state.KV[approvalsKey].(map[string]any); ok {
			for k, v := range old {
				merged[k] = v
			}
		}
		for _, ap := range allApprovals {
			if ap.Approve {
				merged[ap.CallID] = "allow"
			} else {
				merged[ap.CallID] = "reject"
			}
		}
		state.KV[approvalsKey] = merged
	}
	run := a.newRunHandle(ctx, threadID, &state)
	run.rc.setDurableClaim(claim)
	for _, decision := range durableDecisions {
		run.rc.queueApprovalEvent(core.ApprovalDecided{Decision: decision})
	}

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
	run.rc.resumed = newResumeBatch(cp.Pending.Step, cp.Pending.Pending, allApprovals, lastAssistant(state.Messages))
	return run, nil
}

func validatePause(cp *checkpoint.Checkpoint) error {
	if cp.Pause == nil {
		return nil // checkpoint format before pause metadata
	}
	switch cp.Pause.Recovery {
	case checkpoint.RecoveryReplayModel:
		if cp.Pending != nil && len(cp.Pending.Pending) > 0 {
			return fmt.Errorf("agent: replay-model pause cannot carry pending tools")
		}
	case checkpoint.RecoveryResumeTools:
		if cp.Pending == nil || len(cp.Pending.Pending) == 0 {
			return fmt.Errorf("agent: resume-tools pause has no pending tools")
		}
	default:
		return fmt.Errorf("agent: unsupported checkpoint pause recovery %q", cp.Pause.Recovery)
	}
	return nil
}

// resumeBatch is the pending tool-call batch a HITL pause left behind, carried
// into the resumed run so the loop runs it before its next model call.
type resumeBatch struct {
	step   int             // loop step the run paused at
	calls  []core.ToolCall // pending calls, in the model's original order
	decide map[string]Approval
	ids    map[string]int
	final  core.Message // assistant message that issued the calls, if a call ends the run
}

func newResumeBatch(step int, calls []core.ToolCall, approvals []Approval, final core.Message) *resumeBatch {
	ids := make(map[string]int, len(calls))
	for _, c := range calls {
		ids[c.ID]++
	}
	return &resumeBatch{
		step:   step,
		calls:  append([]core.ToolCall(nil), calls...),
		decide: decisionsBy(approvals),
		ids:    ids,
		final:  final,
	}
}

// resumedBatchOutcome is the result of revalidating and possibly executing a
// checkpointed batch. A regular gate can interrupt it again before any handler
// starts; in that case all calls remain pending because none was executed.
type resumedBatchOutcome struct {
	parts   []core.Part
	control core.Directive
	rejects []ToolRejection
	err     error
	pause   bool
	reason  string
}

// runResumed executes a resumeBatch under the step context the loop built for it.
// Every approved call re-enters BeforeTool. Only middleware that recognizes
// LoopContext.IsApproved may waive its own approval prompt; custom gates still
// get a chance to interrupt or reject the resumed batch.
func (l *AgentLoop) runResumed(rb *resumeBatch, lc *LoopContext) resumedBatchOutcome {
	rc := lc.RunContext

	approvedIndexes := make([]int, 0, len(rb.calls))
	lc.approved = make(map[toolApprovalKey]struct{}, len(rb.calls))
	for i := range rb.calls {
		c := rb.calls[i]
		if ap, ok := rb.approvalFor(c); ok && ap.Approve {
			approvedIndexes = append(approvedIndexes, i)
			lc.approved[toolApprovalKeyFor(c)] = struct{}{}
		}
	}
	for _, i := range approvedIndexes {
		d, err := l.mw.BeforeTool(lc, &rb.calls[i])
		if err != nil {
			return resumedBatchOutcome{err: err}
		}
		switch d.Kind {
		case core.Interrupt:
			return resumedBatchOutcome{pause: true, reason: d.Reason}
		case core.Stop, core.Escalate, core.Transfer:
			return resumedBatchOutcome{control: d}
		}
	}
	approved := make([]core.ToolCall, 0, len(approvedIndexes))
	for _, i := range approvedIndexes {
		approved = append(approved, rb.calls[i])
	}
	results, dirs, rejects, execErr := l.execTools(rc, lc, approved)

	parts := make([]core.Part, 0, len(rb.calls))
	next := 0
	for _, c := range rb.calls {
		ap, recorded := rb.approvalFor(c)
		if recorded && ap.Approve {
			parts = append(parts, results[next])
			next++
			continue
		}
		tr := deniedResult(rc, c, rejectionReason(ap, recorded, rb.ids[c.ID] != 1))
		rc.recordSkippedTool(rb.step, c, tr)
		parts = append(parts, tr)
	}
	return resumedBatchOutcome{parts: parts, control: core.Resolve(dirs...), rejects: rejects, err: execErr}
}

func (rb *resumeBatch) approvalFor(c core.ToolCall) (Approval, bool) {
	if rb.ids[c.ID] != 1 {
		return Approval{}, false
	}
	ap, ok := rb.decide[c.ID]
	return ap, ok
}

// rejectionReason words a denial for the model, which may re-route on it: an
// explicit rejection carries its reason, an absent decision says so.
func rejectionReason(ap Approval, recorded, ambiguous bool) string {
	switch {
	case ambiguous:
		return "rejected: ambiguous call ID"
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

// recordSkippedTool closes a pending durable invocation when a human rejects it
// or declines to decide. The result is committed with the resumed history, so a
// later recovery neither reopens the approval nor mistakes the call for an
// unobserved handler execution.
func (rc *RunContext) recordSkippedTool(step int, c core.ToolCall, tr core.ToolResult) {
	if _, ok := rc.durableStore(); !ok {
		return
	}
	invocationID, idempotencyKey := toolInvocation(rc.ThreadID, step, c)
	result := core.Message{Role: core.RoleTool, Parts: []core.Part{tr}}
	rc.addCompletedTool(checkpoint.ToolExecution{
		InvocationID:   invocationID,
		CallID:         c.ID,
		Tool:           c.Name,
		IdempotencyKey: idempotencyKey,
		State:          checkpoint.ToolCompleted,
		Result:         &result,
	})
}
