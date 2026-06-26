package plan

import (
	"fmt"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// Config configures a PlanAgent.
type Config struct {
	Name        string
	Description string

	// Plan is a statically declared plan. Either Plan or Planner must be set;
	// when both are, a Planner-produced plan (from session state) wins, falling
	// back to Plan as a template for executor rebinding on resume.
	Plan *Plan

	// Planner, when set, runs first to produce a plan dynamically (see planner.go).
	Planner agent.Agent

	// Tools and Agents are the registries a dynamically-produced plan resolves
	// step executors against, by name.
	Tools  []tool.Tool
	Agents []agent.Agent

	// MaxConc caps how many steps run concurrently (default GOMAXPROCS).
	MaxConc int

	// Backend selects the concurrency engine: BackendGoroutines (default,
	// goroutines + semaphore) or BackendQueue (a queue.Worker pool). Both honor
	// OnError/Retry/Timeout identically.
	Backend Backend

	// Approver gates the plan and its steps; nil approves everything.
	Approver Approver

	// MaxReplans bounds how many times a failed plan is regenerated. It only
	// applies when Planner is set: on an aborting failure the planner is
	// re-invoked (it can read the failure from state under PlanFailureKey) to
	// revise the remaining work; completed steps are preserved across the replan.
	// 0 (default) disables replanning.
	MaxReplans int
}

// PlanAgent runs an execution plan as a dependency-aware DAG and streams a
// core.Event for every step transition. It implements agent.Agent, so it plugs
// into the Runner, composes inside a Pipeline, and can nest (a step's
// AgentExecutor may wrap another PlanAgent).
type PlanAgent struct {
	cfg Config
}

// New constructs a PlanAgent.
func New(cfg Config) *PlanAgent { return &PlanAgent{cfg: cfg} }

func (a *PlanAgent) Name() string        { return a.cfg.Name }
func (a *PlanAgent) Description() string { return a.cfg.Description }

// SubAgents exposes the planner and any agent-backed step executors so the
// transfer/tree machinery can see them.
func (a *PlanAgent) SubAgents() []agent.Agent {
	var subs []agent.Agent
	if a.cfg.Planner != nil {
		subs = append(subs, a.cfg.Planner)
	}
	subs = append(subs, a.cfg.Agents...)
	return subs
}

// Run resolves the plan (static, resumed, or planner-produced), validates it,
// runs the approval gate, then executes the DAG, streaming step events and a
// final summary. When Planner and MaxReplans are set, an aborting failure
// triggers regeneration of the remaining work, up to MaxReplans times.
func (a *PlanAgent) Run(ictx agent.InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		for attempt := 0; ; attempt++ {
			p, ok, err := a.resolvePlan(ictx, yield)
			if err != nil {
				yield(a.errEvent(ictx, err), err)
				return
			}
			if !ok {
				return // planner stream forwarded its own error/stop, or consumer stopped
			}
			if err := Validate(p); err != nil {
				yield(a.errEvent(ictx, err), err)
				return
			}

			// Plan-level approval gate.
			if dec, err := approvePlan(ictx, a.cfg.Approver, p); err != nil || !dec.Approve {
				msg := core.AssistantText("计划未获批准：" + denyReason(dec, err))
				yield(&core.Event{
					ID: core.NewID("evt"), InvocationID: ictx.InvocationID, Author: a.cfg.Name,
					Message: &msg, Actions: core.Actions{Stop: true},
				}, nil)
				return
			}

			// Execute the DAG, merging step goroutines' events through one channel
			// (yield is not concurrency-safe) — the agent.ParallelAgent pattern.
			sched := newScheduler(p, a.cfg.MaxConc, a.cfg.Backend, a.cfg.Approver, a.cfg.Name, ictx)
			emit := make(chan *core.Event)
			go sched.run(emit)
			for ev := range emit {
				if !yield(ev, nil) {
					return
				}
			}

			// Replan on aborting failure, if configured and budget remains.
			if a.canReplan(p, attempt) {
				recordFailure(ictx.Session.State(), p)
				ictx.Session.State().Delete(draftKey) // force the planner to re-run
				note := core.AssistantText(fmt.Sprintf("⚠️ 计划存在失败步骤，第 %d 次重规划…", attempt+1))
				if !yield(&core.Event{ID: core.NewID("evt"), InvocationID: ictx.InvocationID, Author: a.cfg.Name, Message: &note}, nil) {
					return
				}
				continue
			}

			// Final summary event.
			sum := summarize(p)
			msg := core.AssistantText(sum)
			yield(&core.Event{
				ID: core.NewID("evt"), InvocationID: ictx.InvocationID, Author: a.cfg.Name,
				Message: &msg,
				Actions: core.Actions{StateDelta: map[string]any{PlanStateKey(p.ID) + ":summary": sum}},
			}, nil)
			return
		}
	}
}

// canReplan reports whether a failed plan should be regenerated again.
func (a *PlanAgent) canReplan(p *Plan, attempt int) bool {
	return a.cfg.Planner != nil && attempt < a.cfg.MaxReplans && hasFailure(p)
}

// resolvePlan picks the plan to run. Precedence: a Planner-produced plan (run
// the planner unless its draft is already in state) wins; otherwise the static
// template. Either way, a prior snapshot in session state is merged on top so a
// resumed or replanned run skips completed steps. The bool is false when the
// planner stream was stopped by the consumer (the caller should just return).
func (a *PlanAgent) resolvePlan(ictx agent.InvocationContext, yield func(*core.Event, error) bool) (*Plan, bool, error) {
	state := ictx.Session.State()

	var template *Plan
	switch {
	case a.cfg.Planner != nil:
		if _, ok := state.Get(draftKey); !ok {
			if !core.Pipe(a.cfg.Planner.Run(ictx.ForSubAgent(a.cfg.Planner, "")), yield) {
				return nil, false, nil // consumer stopped mid-planning
			}
		}
		raw, ok := state.Get(draftKey)
		if !ok {
			return nil, false, errNoDraft
		}
		s, _ := raw.(string)
		p, err := Parse([]byte(s), a.cfg.Tools, a.cfg.Agents)
		if err != nil {
			return nil, false, err
		}
		template = p
	case a.cfg.Plan != nil:
		template = a.cfg.Plan
	default:
		return nil, false, errNoPlan
	}

	merged, _ := Merge(template, state)
	return merged, true, nil
}

func (a *PlanAgent) errEvent(ictx agent.InvocationContext, err error) *core.Event {
	return &core.Event{ID: core.NewID("evt"), InvocationID: ictx.InvocationID, Author: a.cfg.Name, Err: err}
}

var _ agent.Agent = (*PlanAgent)(nil)
