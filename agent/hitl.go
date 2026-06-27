package agent

import (
	"context"
	"fmt"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

// This file holds the human-in-the-loop pause/continue closure — the part of
// the runtime that turns a BeforeTool Interrupt into a durable pause and back
// into a continued run. Pausing itself lives in the loop (it writes a
// PendingHITL checkpoint and emits Interrupted); resuming lives here.

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
// ToolResults reported to the model. The continued loop then runs from there.
func (a *Agent) Resume(ctx context.Context, threadID string, approvals ...Approval) (*Run, error) {
	cp, err := a.store.Latest(ctx, threadID)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, fmt.Errorf("agent: no checkpoint to resume for thread %q", threadID)
	}
	state := cloneState(cp.State)
	if state.Files == nil {
		state.Files = vfs.NewInState()
	}
	run := a.newRunHandle(ctx, threadID, &state)

	if cp.Pending != nil && len(cp.Pending.Pending) > 0 {
		parts := a.applyApprovals(run.rc, cp.Pending.Pending, approvals)
		state.Messages = append(state.Messages, core.Message{Role: core.RoleTool, Parts: parts})
	}
	return run, nil
}

// applyApprovals turns pending tool calls into tool results given human
// decisions: approved → execute the tool; rejected/undecided → an error result
// carrying the reason. Executed synchronously before the continued loop drives.
func (a *Agent) applyApprovals(rc *RunContext, pending []core.ToolCall, approvals []Approval) []core.Part {
	byID := make(map[string]Approval, len(approvals))
	for _, ap := range approvals {
		byID[ap.CallID] = ap
	}
	parts := make([]core.Part, 0, len(pending))
	// Approved calls execute via the underlying LLM loop (workflow agents have
	// no single loop, so approval-execution applies to LLM agents).
	loop, _ := a.runnable.(*AgentLoop)
	for _, c := range pending {
		ap, ok := byID[c.ID]
		switch {
		case ok && ap.Approve && loop != nil:
			tr, _, ops := loop.callOne(rc, c)
			rc.State.Apply(ops...)
			parts = append(parts, tr)
		default:
			reason := "rejected by human"
			if ok && ap.Reason != "" {
				reason = "rejected: " + ap.Reason
			} else if !ok {
				reason = "rejected: no decision provided"
			}
			parts = append(parts, core.ToolResult{
				CallID: c.ID, Name: c.Name, IsError: true,
				Content: []core.Part{core.Text{Text: reason}},
			})
		}
	}
	return parts
}
