package core

// Directive is v2's explicit control-flow signal. Instead of v1's Event.Actions
// bag (intent written into an event and merged by a Runner at commit time), a
// loop phase — or a tool, via Result.Control — returns a Directive that the
// AgentLoop acts on immediately. Reading the loop shows the control flow.
type Directive struct {
	Kind   DirectiveKind
	Target string // delegation target, for Transfer
	Reason string // explanation (HITL prompts, logs)
}

// DirectiveKind enumerates the control signals. Values are ordered by
// precedence (higher wins) so Resolve folds a batch with one max comparison;
// Continue is the zero value, the safe default.
//
// Precedence (low → high): Continue < Transfer < Escalate < Stop < Interrupt.
type DirectiveKind int

const (
	// Continue proceeds to the next loop phase/step. Zero value.
	Continue DirectiveKind = iota
	// Transfer hands control to the agent named in Directive.Target.
	Transfer
	// Escalate asks an enclosing Loop workflow agent to stop iterating.
	Escalate
	// Stop ends the current run after this point.
	Stop
	// Interrupt pauses the run for human-in-the-loop; the loop checkpoints a
	// Pending snapshot and returns until resumed.
	Interrupt
)

func (k DirectiveKind) String() string {
	switch k {
	case Continue:
		return "continue"
	case Transfer:
		return "transfer"
	case Escalate:
		return "escalate"
	case Stop:
		return "stop"
	case Interrupt:
		return "interrupt"
	default:
		return "unknown"
	}
}

// Resolve folds several directives into one by precedence: the highest-Kind
// wins, and on a tie the first in argument order is kept (so middleware order
// is the tie-breaker). An empty call yields Continue.
func Resolve(ds ...Directive) Directive {
	best := Directive{Kind: Continue}
	for _, d := range ds {
		if d.Kind > best.Kind {
			best = d
		}
	}
	return best
}
