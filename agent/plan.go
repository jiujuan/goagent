package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// This file is the DAG plan executor (Step 1). A Plan is a set of Nodes with
// dependency edges; NewPlan wraps it as an *Agent (Runnable), so it has the same
// Run/Stream/Resume surface as any agent. The executor schedules nodes in
// topological order, running ready nodes concurrently, threading each node's
// output to its dependents via {{id}} templating, checkpointing the plan state
// for resume, and (optionally) pausing for one final approval when the whole
// plan completes.
//
// A node runs an isolated sub-run (fresh State seeded with its rendered task,
// shared Files) and contributes only its final text — node internals never
// pollute other nodes (no shared conversation; that was the chosen Step-1 model).
//
// NOT in Step 1: per-node approval, dynamic replanning (rewrite-and-execute).

// Node is one unit of work in a plan.
type Node struct {
	// ID uniquely identifies the node (avoid the reserved prefix "__").
	ID string
	// Task is the instruction for this node. It may reference upstream outputs
	// and the plan input via {{id}} / {{input}} placeholders. (DependsOn controls
	// ordering; {{id}} controls data flow — they are independent.)
	Task string
	// Worker runs this node; if nil, the plan's WithWorker default is used.
	Worker *Agent
	// DependsOn lists node IDs that must complete (done) before this node runs.
	DependsOn []string
	// Approve requires a human decision before this node runs (per-node HITL):
	// the plan pauses (Interrupted) with this node in Pending; approve/reject via
	// Run.Decide + Run.Resume. Independent branches keep running while it waits.
	Approve bool
	// MaxRetries re-runs the node on failure up to this many times.
	MaxRetries int
}

// Plan is a DAG of nodes.
type Plan struct{ Nodes []Node }

// ErrorPolicy selects what happens when a node fails after its retries.
type ErrorPolicy int

const (
	// FailFast aborts the plan on the first unrecoverable node failure.
	FailFast ErrorPolicy = iota
	// ContinueOnError marks the node failed, skips its dependents, and keeps
	// running independent branches.
	ContinueOnError
)

type planConfig struct {
	worker        *Agent
	concurrency   int
	policy        ErrorPolicy
	finalApproval bool
}

// PlanOption configures a plan.
type PlanOption func(*planConfig)

// WithWorker sets the default agent that runs nodes lacking their own Worker.
func WithWorker(a *Agent) PlanOption { return func(c *planConfig) { c.worker = a } }

// WithConcurrency caps how many nodes run at once (default 8).
func WithConcurrency(n int) PlanOption {
	return func(c *planConfig) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithErrorPolicy selects FailFast (default) or ContinueOnError.
func WithErrorPolicy(p ErrorPolicy) PlanOption { return func(c *planConfig) { c.policy = p } }

// WithFinalApproval pauses for one human approval after all nodes complete,
// before the plan's result is finalized (HITL: Run.Decide + Run.Resume).
func WithFinalApproval() PlanOption { return func(c *planConfig) { c.finalApproval = true } }

const (
	planStateKey   = "__plan__"
	approvalsKey   = "__approvals__"
	planInputKey   = "input"
	planApprovalID = "__plan__"
)

// NewPlan compiles a Plan into a runnable *Agent. DAG validity (no unknown deps,
// no cycles) is checked here; a violation surfaces as a run error.
func NewPlan(name string, plan Plan, opts ...PlanOption) *Agent {
	cfg := planConfig{concurrency: 8, policy: FailFast}
	for _, o := range opts {
		o(&cfg)
	}
	pr := &planRunner{name: name, cfg: cfg}
	pr.index(plan.Nodes)
	pr.buildErr = pr.validate()
	return wrapWorkflow(name, pr)
}

type planRunner struct {
	name     string
	nodes    map[string]Node
	order    []string
	cfg      planConfig
	buildErr error
}

func (p *planRunner) index(nodes []Node) {
	p.nodes = make(map[string]Node, len(nodes))
	p.order = make([]string, 0, len(nodes))
	for _, n := range nodes {
		p.nodes[n.ID] = n
		p.order = append(p.order, n.ID)
	}
}

func (p *planRunner) validate() error {
	for _, id := range p.order {
		for _, d := range p.nodes[id].DependsOn {
			if _, ok := p.nodes[d]; !ok {
				return fmt.Errorf("plan %q: node %q depends on unknown node %q", p.name, id, d)
			}
		}
	}
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(p.order))
	var visit func(string) error
	visit = func(id string) error {
		color[id] = gray
		for _, d := range p.nodes[id].DependsOn {
			switch color[d] {
			case gray:
				return fmt.Errorf("plan %q: dependency cycle involving %q", p.name, d)
			case white:
				if err := visit(d); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}
	for _, id := range p.order {
		if color[id] == white {
			if err := visit(id); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ Runnable = (*planRunner)(nil)

func (p *planRunner) run(rc *RunContext) runOutcome {
	if p.buildErr != nil {
		return runOutcome{Err: p.buildErr}
	}
	if rc.State.KV == nil {
		rc.State.KV = map[string]any{}
	}
	if _, ok := rc.State.KV[planInputKey]; !ok {
		rc.State.KV[planInputKey] = planInput(rc.State.Messages)
	}

	st := loadPlanState(rc, p.order)

	type result struct {
		id, out string
		err     error
	}
	results := make(chan result)
	inflight := 0
	step := 0
	var failFastErr error

	for {
		// Launch every ready node up to the concurrency cap. A node marked
		// Approve waits for a human decision before it runs: allow → launch,
		// reject → drop it (cascades to its dependents), undecided → collect it
		// and pause once no other work is in flight.
		var awaiting []core.ApprovalRequest
		for _, id := range p.order {
			if failFastErr != nil || inflight >= p.cfg.concurrency {
				break
			}
			node := p.nodes[id]
			if st.Status[id] != "pending" || !depsDone(st, node) {
				continue
			}
			if node.Approve {
				switch approvalFor(rc.State, id) {
				case "allow":
					// approved → fall through to launch
				case "reject":
					st.Status[id] = "rejected"
					rc.publish(core.PlanNodeDone{NodeID: id, Status: "rejected"})
					continue
				default:
					awaiting = append(awaiting, core.ApprovalRequest{
						CallID: id, Tool: "approve_node", Args: []byte(node.Task),
					})
					continue
				}
			}
			input := renderTemplate(node.Task, rc.State.KV)
			st.Status[id] = "running"
			inflight++
			rc.publish(core.PlanNodeStarted{NodeID: id})
			go func(node Node, input string) {
				out, err := p.execNode(rc, node, input)
				results <- result{node.ID, out, err}
			}(node, input)
		}

		if inflight == 0 {
			if failFastErr != nil {
				return runOutcome{Err: failFastErr}
			}
			if len(awaiting) > 0 {
				// Pause for per-node approval (independent branches have drained).
				p.save(rc, st, step)
				return runOutcome{Control: core.Directive{Kind: core.Interrupt}, Pending: awaiting}
			}
			if p.skipBlocked(st, rc) {
				continue // cascade-skip nodes blocked by failed/skipped/rejected deps
			}
			break // every node is terminal
		}

		// Apply one completion.
		d := <-results
		inflight--
		node := p.nodes[d.id]
		switch {
		case d.err != nil && st.Attempts[d.id] < node.MaxRetries:
			st.Attempts[d.id]++
			st.Status[d.id] = "pending"
			rc.publish(core.PlanNodeDone{NodeID: d.id, Status: "retry", Err: d.err})
		case d.err != nil:
			st.Status[d.id] = "failed"
			rc.publish(core.PlanNodeDone{NodeID: d.id, Status: "failed", Err: d.err})
			if p.cfg.policy == FailFast {
				failFastErr = fmt.Errorf("plan %q: node %q failed: %w", p.name, d.id, d.err)
			}
		default:
			st.Status[d.id] = "done"
			st.Output[d.id] = d.out
			rc.State.KV[d.id] = d.out
			rc.publish(core.PlanNodeDone{NodeID: d.id, Status: "done"})
		}
		step++
		p.save(rc, st, step)
	}

	// Final approval gate (whole-plan, not per-node).
	if p.cfg.finalApproval {
		switch approvalFor(rc.State, planApprovalID) {
		case "allow":
			// proceed to finalize
		case "reject":
			return runOutcome{Result: core.Result{Message: core.AssistantText("计划结果被拒绝。")}}
		default:
			p.save(rc, st, step)
			return runOutcome{
				Control: core.Directive{Kind: core.Interrupt},
				Pending: []core.ApprovalRequest{{CallID: planApprovalID, Tool: "approve_plan"}},
			}
		}
	}

	return runOutcome{Result: core.Result{Message: core.AssistantText(p.finalOutput(st))}}
}

// execNode runs a node's worker as an isolated sub-run and returns its final text.
func (p *planRunner) execNode(rc *RunContext, node Node, input string) (string, error) {
	w := node.Worker
	if w == nil {
		w = p.cfg.worker
	}
	if w == nil {
		return "", fmt.Errorf("node %q has no worker (set Node.Worker or WithWorker)", node.ID)
	}
	out := w.runnable.run(rc.subRun(core.UserText(input)))
	if out.Err != nil {
		return "", out.Err
	}
	return out.Result.Message.Text(), nil
}

// skipBlocked marks pending nodes whose deps failed/skipped as skipped, so the
// scheduler can make progress; returns whether anything changed.
func (p *planRunner) skipBlocked(st *planState, rc *RunContext) bool {
	changed := false
	for _, id := range p.order {
		if st.Status[id] != "pending" {
			continue
		}
		for _, d := range p.nodes[id].DependsOn {
			if s := st.Status[d]; s == "failed" || s == "skipped" || s == "rejected" {
				st.Status[id] = "skipped"
				rc.publish(core.PlanNodeDone{NodeID: id, Status: "skipped"})
				changed = true
				break
			}
		}
	}
	return changed
}

// finalOutput joins the outputs of the plan's leaf (sink) nodes.
func (p *planRunner) finalOutput(st *planState) string {
	dependedOn := map[string]bool{}
	for _, id := range p.order {
		for _, d := range p.nodes[id].DependsOn {
			dependedOn[d] = true
		}
	}
	var parts []string
	for _, id := range p.order {
		if !dependedOn[id] && st.Status[id] == "done" {
			parts = append(parts, st.Output[id])
		}
	}
	if len(parts) == 0 {
		for _, id := range p.order {
			if st.Status[id] == "done" {
				parts = append(parts, st.Output[id])
			}
		}
	}
	return strings.Join(parts, "\n\n")
}

func (p *planRunner) save(rc *RunContext, st *planState, step int) {
	b, _ := json.Marshal(st)
	rc.State.KV[planStateKey] = string(b)
	if rc.Store == nil {
		return
	}
	_ = rc.Store.Save(rc, &checkpoint.Checkpoint{
		ID:       core.NewID("cp"),
		ThreadID: rc.ThreadID,
		Step:     step,
		State:    *rc.State,
	})
}

func depsDone(st *planState, n Node) bool {
	for _, d := range n.DependsOn {
		if st.Status[d] != "done" {
			return false
		}
	}
	return true
}

// --- plan state (checkpointable) --------------------------------------------

type planState struct {
	Status   map[string]string `json:"status"`
	Output   map[string]string `json:"output"`
	Attempts map[string]int    `json:"attempts"`
}

func newPlanState(order []string) *planState {
	s := &planState{Status: map[string]string{}, Output: map[string]string{}, Attempts: map[string]int{}}
	for _, id := range order {
		s.Status[id] = "pending"
	}
	return s
}

// loadPlanState restores plan state from the checkpointed State (resume) or
// initializes it. Nodes left "running" by a crash are reset to "pending".
func loadPlanState(rc *RunContext, order []string) *planState {
	raw, ok := rc.State.KV[planStateKey].(string)
	if !ok || raw == "" {
		return newPlanState(order)
	}
	var s planState
	if json.Unmarshal([]byte(raw), &s) != nil {
		return newPlanState(order)
	}
	if s.Status == nil {
		s.Status = map[string]string{}
	}
	if s.Output == nil {
		s.Output = map[string]string{}
	}
	if s.Attempts == nil {
		s.Attempts = map[string]int{}
	}
	for _, id := range order {
		if st, ok := s.Status[id]; !ok {
			s.Status[id] = "pending"
		} else if st == "running" {
			s.Status[id] = "pending"
		}
	}
	return &s
}

func approvalFor(s *core.State, id string) string {
	m, ok := s.KV[approvalsKey].(map[string]any)
	if !ok {
		return ""
	}
	if v, ok := m[id].(string); ok {
		return v
	}
	return ""
}

func planInput(msgs []core.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == core.RoleUser {
			return msgs[i].Text()
		}
	}
	return ""
}
