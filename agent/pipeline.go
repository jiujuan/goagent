package agent

// Pipeline is a fluent builder for a multi-stage workflow. Each stage is an
// *Agent — a plain LLM agent, or a Parallel/Loop compound stage. Build() folds
// the stages into a Sequential workflow, so a pipeline is just sugar over
// Sequential with compound stages inlined.
//
//	pipe := agent.NewPipeline("research-report").
//	    Then(planner).
//	    ThenParallel("gather", web, papers).
//	    Then(writer).
//	    ThenLoop("review", 3, critic, reviser).
//	    Build()
//	answer, _ := pipe.Run(ctx, "topic: vector databases")
type Pipeline struct {
	name   string
	stages []*Agent
}

// NewPipeline starts a pipeline builder.
func NewPipeline(name string) *Pipeline { return &Pipeline{name: name} }

// Then appends a single agent stage.
func (p *Pipeline) Then(a *Agent) *Pipeline {
	p.stages = append(p.stages, a)
	return p
}

// ThenParallel appends a concurrent fan-out stage.
func (p *Pipeline) ThenParallel(name string, subs ...*Agent) *Pipeline {
	p.stages = append(p.stages, Parallel(name, subs...))
	return p
}

// ThenLoop appends a refinement-loop stage (runs until Escalate or maxIter).
func (p *Pipeline) ThenLoop(name string, maxIter int, subs ...*Agent) *Pipeline {
	p.stages = append(p.stages, Loop(name, maxIter, subs...))
	return p
}

// Build assembles the pipeline into a runnable Sequential agent.
func (p *Pipeline) Build() *Agent { return Sequential(p.name, p.stages...) }
