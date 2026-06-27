package agent

// Runnable is the sealed internal contract every executable unit satisfies — the
// LLM agent loop and (in a later stage) the workflow agents. It is unexported so
// only this package defines runnables; the public API stays declarative (Spec
// and workflow constructors), while dispatch is polymorphic underneath.
type Runnable interface {
	// run drives the unit against rc, publishing observational events to the bus
	// and snapshotting to the checkpointer. It returns when the unit terminates,
	// pauses (interrupt), or fails — in all cases a terminal event was published.
	run(rc *RunContext)
}

// Compile lowers a Spec into a Runnable. Today every Spec compiles to an
// AgentLoop; workflow specs will compile to their own runners in a later stage.
func Compile(spec Spec) Runnable {
	return newLoop(spec)
}

// Drive runs a compiled Runnable against rc to completion (or pause/failure).
// It is the exported entry the runtime engine uses; the Runnable.run method is
// unexported so only this package defines runnables.
func Drive(rc *RunContext, r Runnable) { r.run(rc) }
