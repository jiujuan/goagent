// Package runtime is v2's engine and run handles. It owns the shared event Bus
// and Checkpointer, compiles an agent.Spec into a runnable *Agent, and produces
// *Run handles that drive the loop, stream events, and settle results. It
// depends on agent (and bus/checkpoint/vfs); agent never imports runtime, which
// keeps the dependency graph acyclic.
package runtime

import (
	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
)

// Runtime ties an event bus and a checkpointer together and compiles specs.
type Runtime struct {
	bus   *bus.Bus
	store checkpoint.Checkpointer
}

// Config configures a Runtime. Both fields default sensibly.
type Config struct {
	Bus          *bus.Bus
	Checkpointer checkpoint.Checkpointer
}

// New constructs a Runtime, defaulting the bus to a fresh one and the
// checkpointer to in-memory.
func New(cfg Config) *Runtime {
	b := cfg.Bus
	if b == nil {
		b = bus.New()
	}
	s := cfg.Checkpointer
	if s == nil {
		s = checkpoint.NewMemory()
	}
	return &Runtime{bus: b, store: s}
}

// Bus exposes the shared event bus (for cross-run subscriptions/tracing).
func (rt *Runtime) Bus() *bus.Bus { return rt.bus }

// Compile lowers a Spec into a runnable Agent handle.
func (rt *Runtime) Compile(spec agent.Spec) *Agent {
	return &Agent{rt: rt, spec: spec, compiled: agent.Compile(spec)}
}
