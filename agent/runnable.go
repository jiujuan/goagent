package agent

// Runnable is the sealed internal contract every executable unit satisfies — the
// LLM agent loop now, and workflow agents (Sequential/Parallel/Loop) in a later
// stage. The run method is unexported so only this package defines runnables;
// the Run handle drives one via run(rc).
type Runnable interface {
	run(rc *RunContext)
}
