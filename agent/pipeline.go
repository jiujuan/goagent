package agent

import (
	"slices"

	"github.com/jiujuan/goagent/core"
)

// --- Pipeline ---------------------------------------------------------------

// PipelineAgent runs an ordered list of stages, forwarding every event. Its run
// semantics match SequentialAgent; what differs is construction: a PipelineAgent
// is assembled with the fluent Pipeline builder, which reads top-to-bottom as a
// series of named stages — including composite parallel/loop stages added inline
// — making a multi-step flow easier to follow than nested constructors.
type PipelineAgent struct {
	name   string
	desc   string
	stages []Agent
}

func (a *PipelineAgent) Name() string { return a.name }

func (a *PipelineAgent) Description() string {
	if a.desc != "" {
		return a.desc
	}
	return "runs stages in a pipeline"
}

func (a *PipelineAgent) SubAgents() []Agent { return a.stages }

func (a *PipelineAgent) Run(ictx InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		for _, stage := range a.stages {
			if !core.Pipe(stage.Run(ictx.withAgent(stage, "").refreshSnapshot()), yield) {
				return
			}
		}
	}
}

// PipelineBuilder assembles a PipelineAgent one stage at a time. Construct it
// with Pipeline, chain Then / ThenParallel / ThenLoop, then call Build.
type PipelineBuilder struct {
	name   string
	desc   string
	stages []Agent
}

// Pipeline starts building a named PipelineAgent:
//
//	p := agent.Pipeline("etl").
//	    Then(ingest).
//	    ThenParallel("enrich", geo, sentiment).
//	    Then(writer).
//	    ThenLoop("review", 3, critic, reviser).
//	    Build()
func Pipeline(name string) *PipelineBuilder {
	return &PipelineBuilder{name: name}
}

// Describe sets an optional human-readable description for the pipeline.
func (b *PipelineBuilder) Describe(desc string) *PipelineBuilder {
	b.desc = desc
	return b
}

// Then appends a single agent as the next stage.
func (b *PipelineBuilder) Then(stage Agent) *PipelineBuilder {
	b.stages = append(b.stages, stage)
	return b
}

// ThenParallel appends a fan-out stage whose sub-agents run concurrently, each
// on its own branch (see ParallelAgent).
func (b *PipelineBuilder) ThenParallel(name string, subs ...Agent) *PipelineBuilder {
	return b.Then(Parallel(name, subs...))
}

// ThenParallelWithOptions appends a fan-out stage with explicit state-conflict
// behavior for the deterministic merge.
func (b *PipelineBuilder) ThenParallelWithOptions(name string, opts ParallelOptions, subs ...Agent) *PipelineBuilder {
	return b.Then(ParallelWithOptions(name, opts, subs...))
}

// ThenLoop appends a stage that repeats its sub-agents until one escalates or
// maxIterations is reached (0 = until escalation; see LoopAgent).
func (b *PipelineBuilder) ThenLoop(name string, maxIterations int, subs ...Agent) *PipelineBuilder {
	return b.Then(Loop(name, maxIterations, subs...))
}

// Build finalizes the pipeline. The result is itself an Agent, so it can be
// nested as a stage in another pipeline or workflow.
func (b *PipelineBuilder) Build() *PipelineAgent {
	return &PipelineAgent{name: b.name, desc: b.desc, stages: slices.Clone(b.stages)}
}

var _ Agent = (*PipelineAgent)(nil)
