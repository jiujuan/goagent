package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// ErrInFlightUnrecoverable means a thread's in-flight batch cannot be settled
// safely by this agent: the recorded step no longer matches the current tool
// table, the batch cannot be matched to its assistant message, or the storage
// cannot answer what already ran. The wrapped cause says which.
var ErrInFlightUnrecoverable = errors.New("agent: in-flight tool batch cannot be recovered safely")

// inFlightBatch is the durable batch an in-flight checkpoint left behind,
// carried into the resumed run so the loop settles it before its next model
// call. It is registered by Agent.Resume and consumed once by the loop.
type inFlightBatch struct {
	step     int
	calls    []core.ToolCall
	final    core.Message
	snapshot *checkpoint.StepSnapshot
}

// matchInFlightMessage finds the assistant message that issued the recorded
// batch. Only call IDs, tool names and order are compared: the gate may have
// rewritten arguments after the model answered, and the batch records the
// arguments the handlers were given.
func matchInFlightMessage(messages []core.Message, calls []core.ToolCall) (core.Message, error) {
	if len(messages) == 0 {
		return core.Message{}, fmt.Errorf("%w: no assistant message to recover", ErrInFlightUnrecoverable)
	}
	last := messages[len(messages)-1]
	if last.Role != core.RoleAssistant {
		return core.Message{}, fmt.Errorf("%w: last message is %q, not assistant", ErrInFlightUnrecoverable, last.Role)
	}
	declared := last.ToolCalls()
	if len(declared) != len(calls) {
		return core.Message{}, fmt.Errorf("%w: assistant declared %d calls, batch has %d", ErrInFlightUnrecoverable, len(declared), len(calls))
	}
	for i := range calls {
		if declared[i].ID != calls[i].ID || declared[i].Name != calls[i].Name {
			return core.Message{}, fmt.Errorf("%w: call %d does not match the recorded batch", ErrInFlightUnrecoverable, i)
		}
	}
	return last, nil
}

// preflightInFlight validates an in-flight head before the resume claim is
// taken. Every check is read-only: a rejected batch leaves the thread free for
// another attempt. History matching happens after the claim, against the state
// the resumed handle cloned.
func (a *Agent) preflightInFlight(ctx context.Context, cp *checkpoint.Checkpoint) (*inFlightBatch, error) {
	ib := cp.InFlight
	if a.loop == nil {
		return nil, fmt.Errorf("%w: thread %q has an in-flight batch but this agent has no tool table", ErrInFlightUnrecoverable, cp.ThreadID)
	}
	if cp.Snapshot == nil || cp.Snapshot.Step != cp.Step {
		return nil, fmt.Errorf("%w: thread %q has no step snapshot for the in-flight batch", ErrInFlightUnrecoverable, cp.ThreadID)
	}
	if ib.Step != cp.Step {
		return nil, fmt.Errorf("%w: thread %q in-flight step %d does not match checkpoint step %d", ErrInFlightUnrecoverable, cp.ThreadID, ib.Step, cp.Step)
	}
	if len(ib.Calls) == 0 {
		return nil, fmt.Errorf("%w: thread %q in-flight batch has no calls", ErrInFlightUnrecoverable, cp.ThreadID)
	}
	durable, ok := a.store.(checkpoint.Durable)
	if !ok {
		return nil, fmt.Errorf("%w: %w: thread %q in-flight recovery requires a durable store", ErrInFlightUnrecoverable, checkpoint.ErrDurableUnavailable, cp.ThreadID)
	}
	advertised := make(map[string]struct{}, len(cp.Snapshot.Advertised))
	for _, name := range cp.Snapshot.Advertised {
		advertised[name] = struct{}{}
	}
	for _, call := range ib.Calls {
		if _, ok := advertised[call.Name]; !ok {
			return nil, fmt.Errorf("%w: %w: thread %q call %q requested tool %q", ErrInFlightUnrecoverable, ErrToolNotAdvertised, cp.ThreadID, call.ID, call.Name)
		}
		if _, ok := a.loop.byName[call.Name]; !ok {
			return nil, fmt.Errorf("%w: %w: thread %q call %q requested tool %q; runtime-injected tools do not survive resume", ErrInFlightUnrecoverable, ErrToolUnavailable, cp.ThreadID, call.ID, call.Name)
		}
	}
	mayExecute, err := a.inFlightMayExecute(ctx, durable, cp)
	if err != nil {
		return nil, err
	}
	if mayExecute {
		if cp.Snapshot.ToolDigest == "" {
			return nil, fmt.Errorf("%w: thread %q in-flight batch has no tool digest", ErrInFlightUnrecoverable, cp.ThreadID)
		}
		view := a.loop.resumeStepViewFor(cp.Step, ib.Calls, cp.Snapshot)
		if current := toolDigest(view.Tools().Schemas()); current != cp.Snapshot.ToolDigest {
			return nil, fmt.Errorf("%w: %w: thread %q in-flight batch was recorded under a different tool table", ErrInFlightUnrecoverable, ErrStepViewDrift, cp.ThreadID)
		}
	}
	return &inFlightBatch{
		step:     ib.Step,
		calls:    cloneToolCalls(ib.Calls),
		snapshot: cloneStepSnapshot(cp.Snapshot),
	}, nil
}

// inFlightMayExecute reports whether recovery could start a handler. A missing
// ledger record or a pending one may; completed, claimed and unknown never do,
// because those calls are answered from the ledger. The read is deliberately
// strict: if the ledger cannot be read, the batch is not recoverable.
func (a *Agent) inFlightMayExecute(ctx context.Context, durable checkpoint.Durable, cp *checkpoint.Checkpoint) (bool, error) {
	for _, call := range cp.InFlight.Calls {
		invocationID, _ := toolInvocation(cp.ThreadID, cp.Step, call)
		ex, found, err := durable.ToolExecution(ctx, cp.ThreadID, invocationID)
		if err != nil {
			return false, fmt.Errorf("%w: read tool execution %s: %v", ErrInFlightUnrecoverable, invocationID, err)
		}
		if !found || ex.State == checkpoint.ToolPending {
			return true, nil
		}
	}
	return false, nil
}

// inFlightCallsAdvertised reports whether every call names a tool this step
// offered the model. A call outside that table never runs, so a batch holding
// one has no side effect to recover; writing it would also record work the
// storage layer rejects (an in-flight call must belong to the step's
// advertised table). Such a batch skips the write and takes the ordinary
// per-call rejection path.
func inFlightCallsAdvertised(lc *LoopContext, calls []core.ToolCall) bool {
	tv := lc.view.Tools()
	for _, c := range calls {
		if !tv.Advertised(c.Name) {
			return false
		}
	}
	return true
}

// cloneToolCalls deep-copies a batch so a checkpoint, an event and the handlers
// cannot share argument bytes. The gate may rewrite Args in place, each record
// keeps the bytes as they were at the moment it was taken.
func cloneToolCalls(calls []core.ToolCall) []core.ToolCall {
	if calls == nil {
		return nil
	}
	out := make([]core.ToolCall, len(calls))
	for i, c := range calls {
		if c.Args != nil {
			c.Args = append(json.RawMessage(nil), c.Args...)
		}
		out[i] = c
	}
	return out
}
