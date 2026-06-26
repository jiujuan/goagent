package agent

import (
	"sync"

	"github.com/jiujuan/goagent/core"
)

// --- Sequential -------------------------------------------------------------

// SequentialAgent runs its sub-agents in order, forwarding every event. It is
// the deterministic counterpart to LLM-driven delegation.
type SequentialAgent struct {
	name string
	subs []Agent
}

// Sequential builds a SequentialAgent.
func Sequential(name string, subs ...Agent) *SequentialAgent {
	return &SequentialAgent{name: name, subs: subs}
}

func (a *SequentialAgent) Name() string        { return a.name }
func (a *SequentialAgent) Description() string { return "runs sub-agents in sequence" }
func (a *SequentialAgent) SubAgents() []Agent  { return a.subs }

func (a *SequentialAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		for _, sub := range a.subs {
			if !core.Pipe(sub.Run(ictx.withAgent(sub, "")), yield) {
				return
			}
		}
	}
}

// --- Loop -------------------------------------------------------------------

// LoopAgent runs its sub-agents in sequence repeatedly until a sub-agent emits
// an event with Actions.Escalate set, or MaxIterations is reached (0 = until
// escalation).
type LoopAgent struct {
	name    string
	maxIter int
	subs    []Agent
}

// Loop builds a LoopAgent.
func Loop(name string, maxIterations int, subs ...Agent) *LoopAgent {
	return &LoopAgent{name: name, maxIter: maxIterations, subs: subs}
}

func (a *LoopAgent) Name() string        { return a.name }
func (a *LoopAgent) Description() string { return "runs sub-agents in a loop until escalation" }
func (a *LoopAgent) SubAgents() []Agent  { return a.subs }

func (a *LoopAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		for iter := 0; a.maxIter == 0 || iter < a.maxIter; iter++ {
			escalated := false
			for _, sub := range a.subs {
				for ev, err := range sub.Run(ictx.withAgent(sub, "")) {
					if !yield(ev, err) {
						return
					}
					if err == nil && ev != nil && ev.Actions.Escalate {
						escalated = true
					}
				}
				if escalated {
					return
				}
			}
		}
	}
}

// --- Parallel ---------------------------------------------------------------

// ParallelAgent runs its sub-agents concurrently, each on its own branch, and
// merges their events. A bounded channel provides back-pressure: a sub-agent
// blocks until the merger consumes its previous event.
type ParallelAgent struct {
	name string
	subs []Agent
}

// Parallel builds a ParallelAgent.
func Parallel(name string, subs ...Agent) *ParallelAgent {
	return &ParallelAgent{name: name, subs: subs}
}

func (a *ParallelAgent) Name() string        { return a.name }
func (a *ParallelAgent) Description() string { return "runs sub-agents in parallel" }
func (a *ParallelAgent) SubAgents() []Agent  { return a.subs }

type parallelItem struct {
	ev  *core.Event
	err error
}

func (a *ParallelAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		merged := make(chan parallelItem)
		var wg sync.WaitGroup
		done := make(chan struct{})

		for _, sub := range a.subs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				branch := ictx.Branch
				if branch == "" {
					branch = a.name
				}
				branch += "." + sub.Name()
				for ev, err := range sub.Run(ictx.withAgent(sub, branch)) {
					select {
					case merged <- parallelItem{ev, err}:
					case <-done:
						return
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(merged)
		}()

		defer close(done)
		for it := range merged {
			if !yield(it.ev, it.err) {
				return
			}
		}
	}
}

var (
	_ Agent = (*SequentialAgent)(nil)
	_ Agent = (*LoopAgent)(nil)
	_ Agent = (*ParallelAgent)(nil)
)
