package agent

import "github.com/jiujuan/goagent/core"

// Runnable is the sealed internal contract every executable unit satisfies — the
// LLM agent loop and the workflow agents (Sequential/Parallel/Loop). The run
// method is unexported so only this package defines runnables.
//
// A runnable publishes its INTERNAL observation events (TurnStarted, Message*,
// Tool*, TurnDone) to rc.Bus and returns a runOutcome. It does NOT publish the
// lifecycle events (RunStarted and the single terminal RunDone/RunFailed/
// Interrupted) — the Run handle wraps that, so composing runnables (a workflow
// driving children) never double-emits lifecycle on the shared topic.
type Runnable interface {
	run(rc *RunContext) runOutcome
}

// runOutcome is what a runnable returns to its caller (the Run handle for the
// top level, or a workflow runner for a child). It lets workflows compose by
// value — inspect a child's terminal control (e.g. Escalate) and result —
// instead of scraping the event bus.
type runOutcome struct {
	// Result is the final assistant message of the unit (empty on error/pause).
	Result core.Result
	// Control is the terminal control directive: Continue = ran to completion;
	// Escalate/Stop = a sub asked to stop; Interrupt = paused for HITL.
	Control core.Directive
	// Pending lists the tool calls awaiting approval when Control is Interrupt.
	Pending []core.ApprovalRequest
	// Err is set when the unit failed.
	Err error
}

// terminal reports whether this outcome ends the enclosing run.
func (o runOutcome) terminal() bool {
	return o.Err != nil || o.Control.Kind == core.Interrupt
}
