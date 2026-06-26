package core

// Directive is v2's explicit control-flow signal. It replaces v1's
// Event.Actions side-effect bag: instead of writing intent into an event that
// the Runner later merges at commit time, a loop phase (or a tool, via
// Result.Control) returns a Directive and the AgentLoop acts on it immediately.
// Reading the loop therefore shows the control flow directly.
//
// See ADR 0023. This file is additive to v1; nothing here collides with the v1
// core.Event struct, which keeps working until the migration completes.
type Directive struct {
	Kind   DirectiveKind
	Target string // delegation target, for Transfer
	Reason string // human-readable explanation (HITL prompts, logs)
}

// DirectiveKind enumerates the control signals. The integer values are ordered
// by precedence (higher value wins) so Resolve can fold a batch with a single
// max comparison; Continue is the zero value, i.e. the safe default.
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
// directive wins, and on a tie the first one in argument order is kept (so
// middleware order is the tie-breaker, as specified in ADR 0023). An empty
// call yields the zero Directive (Continue) — the safe default.
//
// This replaces v1's mergeActions, whose last-write-wins / OR semantics for
// TransferToAgent were ambiguous.
func Resolve(ds ...Directive) Directive {
	best := Directive{Kind: Continue}
	for _, d := range ds {
		if d.Kind > best.Kind {
			best = d
		}
	}
	return best
}
