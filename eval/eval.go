// Package eval is goagent's evaluation layer: it scores LLM answers, agent
// trajectories and tool results, and feeds the verdict back into execution to
// raise answer quality. See docs/adr/0026-eval-system.md for the design.
//
// One abstraction underlies everything — a Scorer maps a Sample to a Score in
// [0,1]. Rule scorers (rule.go) are deterministic and free; SemanticSimilarity
// (embed.go) is cheap; LLM-as-Judge scorers (judge.go) are the expensive,
// semantic tier; composites (composite.go) combine them. Two closed loops wrap
// the scorers: online self-correction (loop.go, Gate/ToolGuard middleware over
// agent.Loop) and offline regression (harness.go, a Harness over a Dataset).
//
// eval depends only on lower layers (core, llm, tool, agent, embeddings); nothing
// imports it back, so the dependency graph stays acyclic.
package eval

import (
	"context"
	"errors"
	"time"

	"github.com/jiujuan/goagent/core"
)

// errTODO marks skeleton stubs not yet implemented. Each build-out step (2–6)
// replaces the stub bodies in one file with real logic; this sentinel keeps the
// package compiling in the meantime.
var errTODO = errors.New("eval: not implemented yet")

// Sample is one thing to be judged. The three evaluation targets fill different
// fields: answer eval uses Output/Reference, agent eval uses Traj, tool eval
// uses Tool. Input is the originating user request, carried for judges that
// need it as context.
type Sample struct {
	Input     string         // user request / question
	Output    string         // the answer under evaluation
	Reference string         // optional gold/reference answer
	Traj      *Trajectory    // optional: full agent trajectory (agent eval)
	Tool      *ToolEpisode   // optional: a single tool call+result (tool eval)
	Meta      map[string]any // free-form, available to custom scorers
}

// Score is a normalized verdict in [0,1]. Passed reflects a scorer's own
// threshold; Reason explains it (and is the feedback an online loop steers back
// to the model); Sub carries the per-scorer breakdown of a composite.
type Score struct {
	Name   string  `json:"name"`
	Value  float64 `json:"value"`
	Passed bool    `json:"passed"`
	Reason string  `json:"reason,omitempty"`
	Sub    []Score `json:"sub,omitempty"`
}

// Scorer is the single evaluation contract: rule scorers, the embedding scorer
// and LLM judges all satisfy it, so they compose uniformly.
type Scorer interface {
	Name() string
	Score(ctx context.Context, s Sample) (Score, error)
}

// PairScorer compares two samples head-to-head (A vs B), for A/B and version
// regression. It is separate from Scorer because its arity differs.
type PairScorer interface {
	Name() string
	Compare(ctx context.Context, a, b Sample) (Score, error)
}

// ToolEpisode is one tool invocation under evaluation, reusing core's vocabulary.
type ToolEpisode struct {
	Call    core.ToolCall
	Result  core.ToolResult
	Latency time.Duration
}

// Trajectory is a full agent run reconstructed from the event bus (see Record).
// It reuses core types so eval never invents a parallel message model.
type Trajectory struct {
	Input    string
	Messages []core.Message
	Tools    []ToolEpisode
	Final    core.Result
	Usage    core.Usage
	Steps    int
}

// --- internal helpers -------------------------------------------------------

// scorerFunc adapts a closure to the Scorer interface, the building block every
// constructor in this package returns.
type scorerFunc struct {
	name string
	fn   func(ctx context.Context, s Sample) (Score, error)
}

func (f scorerFunc) Name() string                                       { return f.name }
func (f scorerFunc) Score(ctx context.Context, s Sample) (Score, error) { return f.fn(ctx, s) }

// newScorer builds a Scorer from a name and a scoring closure.
func newScorer(name string, fn func(context.Context, Sample) (Score, error)) Scorer {
	return scorerFunc{name: name, fn: fn}
}

// clamp01 confines a value to [0,1].
func clamp01(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}

// boolScore is a convenience for deterministic pass/fail scorers: pass → 1.0.
func boolScore(name string, pass bool, reason string) Score {
	v := 0.0
	if pass {
		v = 1.0
	}
	return Score{Name: name, Value: v, Passed: pass, Reason: reason}
}
