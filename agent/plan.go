package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// This file is the DAG plan executor. A Plan is a set of Nodes with dependency
// edges; NewPlan wraps it as an *Agent (Runnable), so it has the same
// Run/Stream/Resume surface as any agent. The executor:
//   - schedules nodes in topological order, running ready nodes concurrently;
//   - threads each node's output to its dependents via {{id}} templating;
//   - runs each node as an isolated sub-run (fresh State + its rendered task),
//     taking only its final text — no shared conversation between nodes;
//   - checkpoints plan state (per-node status/output + dynamic nodes) for resume;
//   - gates nodes (Node.Approve) and the whole plan (WithFinalApproval) for HITL;
//   - and, with WithReplanner, lets a replanner EXTEND the DAG when the current
//     plan goes quiescent — "rewrite the todos and execute the rewritten todos".
//
// Step 3 (replanning) is additive: a replanner adds new nodes (it does not edit
// or remove existing ones), bounded by WithMaxReplanRounds.

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
	worker          *Agent
	concurrency     int
	policy          ErrorPolicy
	finalApproval   bool
	replanner       *Agent
	maxReplanRounds int
}

// PlanOption configures a plan.
type PlanOption func(*planConfig)

// WithWorker sets the default agent that runs nodes lacking their own Worker.
// It also runs dynamically-added (replanned) nodes, which carry no Worker.
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

// WithReplanner enables dynamic replanning: when the current plan goes quiescent,
// the replanner agent is asked (with the task + results so far) whether more
// steps are needed; if so, the new nodes are merged into the DAG and executed.
func WithReplanner(a *Agent) PlanOption { return func(c *planConfig) { c.replanner = a } }

// WithMaxReplanRounds bounds how many times the replanner may extend the plan
// (default 3), preventing runaway replanning.
func WithMaxReplanRounds(n int) PlanOption {
	return func(c *planConfig) {
		if n >= 0 {
			c.maxReplanRounds = n
		}
	}
}

const (
	planStateKey   = "__plan__"
	approvalsKey   = "__approvals__"
	planInputKey   = "input"
	planApprovalID = "__plan__"
	replanNodeID   = "__replan__"
)

// NewPlan compiles a Plan into a runnable *Agent. DAG validity (no unknown deps,
// no cycles) is checked here; a violation surfaces as a run error.
func NewPlan(name string, plan Plan, opts ...PlanOption) *Agent {
	cfg := planConfig{concurrency: 8, policy: FailFast, maxReplanRounds: 3}
	for _, o := range opts {
		o(&cfg)
	}
	pr := &planRunner{name: name, cfg: cfg}
	pr.index(plan.Nodes)
	pr.buildErr = validateDAG(pr.nodes, pr.order, name)
	return wrapWorkflow(name, pr)
}

type planRunner struct {
	name     string
	nodes    map[string]Node // static nodes from the Plan
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
	nodes, order := p.materialize(st) // static + dynamic (from a prior replan / resume)
	for _, id := range order {
		if _, ok := st.Status[id]; !ok {
			st.Status[id] = "pending"
		}
	}

	type result struct {
		id, out string
		err     error
	}
	results := make(chan result)
	inflight := 0
	step := 0
	var failFastErr error

	for {
		// Launch every ready node up to the concurrency cap. Approve nodes wait
		// for a decision: allow → launch, reject → drop (cascades), undecided →
		// collect and pause once no other work is in flight.
		var awaiting []core.ApprovalRequest
		for _, id := range order {
			if failFastErr != nil || inflight >= p.cfg.concurrency {
				break
			}
			node := nodes[id]
			if st.Status[id] != "pending" || !depsDone(st, node) {
				continue
			}
			if node.Approve {
				switch approvalFor(rc.State, id) {
				case "allow":
				case "reject":
					st.Status[id] = "rejected"
					rc.publish(core.PlanNodeDone{NodeID: id, Status: "rejected"})
					continue
				default:
					awaiting = append(awaiting, core.ApprovalRequest{CallID: id, Tool: "approve_node", Args: []byte(node.Task)})
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
				p.save(rc, st, step)
				return runOutcome{Control: core.Directive{Kind: core.Interrupt}, Pending: awaiting}
			}
			if p.skipBlocked(nodes, order, st, rc) {
				continue
			}
			// Quiescent: ask the replanner whether to extend the plan.
			if added := p.maybeReplan(rc, nodes, st, order); len(added) > 0 {
				for _, ns := range added {
					nodes[ns.ID] = Node{ID: ns.ID, Task: ns.Task, DependsOn: ns.DependsOn, Approve: ns.Approve}
					order = append(order, ns.ID)
					st.Dynamic = append(st.Dynamic, ns)
					st.Status[ns.ID] = "pending"
				}
				st.ReplanRounds++
				step++
				p.save(rc, st, step)
				continue
			}
			break
		}

		d := <-results
		inflight--
		node := nodes[d.id]
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

	// Whole-plan final approval gate.
	if p.cfg.finalApproval {
		switch approvalFor(rc.State, planApprovalID) {
		case "allow":
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

	return runOutcome{Result: core.Result{Message: core.AssistantText(p.finalOutput(nodes, order, st))}}
}

// materialize builds the working node set: static nodes plus any dynamic nodes
// added by prior replanning (restored from plan state on resume).
func (p *planRunner) materialize(st *planState) (map[string]Node, []string) {
	nodes := make(map[string]Node, len(p.order)+len(st.Dynamic))
	order := make([]string, 0, len(p.order)+len(st.Dynamic))
	for _, id := range p.order {
		nodes[id] = p.nodes[id]
		order = append(order, id)
	}
	for _, ns := range st.Dynamic {
		nodes[ns.ID] = Node{ID: ns.ID, Task: ns.Task, DependsOn: ns.DependsOn, Approve: ns.Approve}
		order = append(order, ns.ID)
	}
	return nodes, order
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

// maybeReplan asks the replanner for additional nodes when the plan is quiescent.
// It returns validated new nodes to merge, or nil to finish.
func (p *planRunner) maybeReplan(rc *RunContext, nodes map[string]Node, st *planState, order []string) []nodeSpec {
	if p.cfg.replanner == nil || st.ReplanRounds >= p.cfg.maxReplanRounds {
		return nil
	}
	rc.publish(core.PlanNodeStarted{NodeID: replanNodeID})
	prompt := buildReplanPrompt(rc.State.KV[planInputKey], st.Output, order)
	out := p.cfg.replanner.runnable.run(rc.subRun(core.UserText(prompt)))
	if out.Err != nil {
		rc.publish(core.PlanNodeDone{NodeID: replanNodeID, Status: "failed", Err: out.Err})
		return nil
	}
	delta := parseDelta(out.Result.Message.Text())
	if delta == nil || delta.Done || len(delta.Nodes) == 0 || !validateDelta(nodes, delta.Nodes) {
		rc.publish(core.PlanNodeDone{NodeID: replanNodeID, Status: "done"})
		return nil
	}
	rc.publish(core.PlanNodeDone{NodeID: replanNodeID, Status: "done"})
	return delta.Nodes
}

// skipBlocked marks pending nodes whose deps failed/skipped/rejected as skipped.
func (p *planRunner) skipBlocked(nodes map[string]Node, order []string, st *planState, rc *RunContext) bool {
	changed := false
	for _, id := range order {
		if st.Status[id] != "pending" {
			continue
		}
		for _, d := range nodes[id].DependsOn {
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
func (p *planRunner) finalOutput(nodes map[string]Node, order []string, st *planState) string {
	dependedOn := map[string]bool{}
	for _, id := range order {
		for _, d := range nodes[id].DependsOn {
			dependedOn[d] = true
		}
	}
	var parts []string
	for _, id := range order {
		if !dependedOn[id] && st.Status[id] == "done" {
			parts = append(parts, st.Output[id])
		}
	}
	if len(parts) == 0 {
		for _, id := range order {
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

// --- DAG validation ---------------------------------------------------------

func validateDAG(nodes map[string]Node, order []string, name string) error {
	for _, id := range order {
		for _, d := range nodes[id].DependsOn {
			if _, ok := nodes[d]; !ok {
				return fmt.Errorf("plan %q: node %q depends on unknown node %q", name, id, d)
			}
		}
	}
	if cyc := findCycle(nodes); cyc != "" {
		return fmt.Errorf("plan %q: dependency cycle involving %q", name, cyc)
	}
	return nil
}

// findCycle returns a node involved in a cycle, or "" if the graph is acyclic.
func findCycle(nodes map[string]Node) string {
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(nodes))
	var found string
	var visit func(string) bool
	visit = func(id string) bool {
		color[id] = gray
		for _, d := range nodes[id].DependsOn {
			if _, ok := nodes[d]; !ok {
				continue
			}
			switch color[d] {
			case gray:
				found = d
				return true
			case white:
				if visit(d) {
					return true
				}
			}
		}
		color[id] = black
		return false
	}
	for id := range nodes {
		if color[id] == white && visit(id) {
			return found
		}
	}
	return ""
}

// validateDelta checks that new nodes can be merged: unique non-reserved IDs,
// resolvable deps, and no cycle in the merged graph.
func validateDelta(existing map[string]Node, add []nodeSpec) bool {
	merged := make(map[string]Node, len(existing)+len(add))
	for k, v := range existing {
		merged[k] = v
	}
	for _, ns := range add {
		if ns.ID == "" || strings.HasPrefix(ns.ID, "__") {
			return false
		}
		if _, dup := merged[ns.ID]; dup {
			return false // additive only: no overriding an existing node
		}
		merged[ns.ID] = Node{ID: ns.ID, Task: ns.Task, DependsOn: ns.DependsOn, Approve: ns.Approve}
	}
	for _, ns := range add {
		for _, d := range ns.DependsOn {
			if _, ok := merged[d]; !ok {
				return false
			}
		}
	}
	return findCycle(merged) == ""
}

// --- replan prompt + parsing ------------------------------------------------

type planDelta struct {
	Nodes []nodeSpec `json:"nodes"`
	Done  bool       `json:"done"`
}

func buildReplanPrompt(input any, output map[string]string, order []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "原始任务:%v\n\n已完成步骤的结果:\n", input)
	for _, id := range order {
		if out, ok := output[id]; ok {
			fmt.Fprintf(&b, "- %s: %s\n", id, out)
		}
	}
	b.WriteString("\n判断是否还需要更多步骤来完整完成原始任务。\n" +
		"- 若需要,只输出 JSON:" +
		`{"nodes":[{"id":"新ID","task":"步骤指令(可用 {{已有ID}} 引用上游结果)","depends_on":["依赖ID"]}],"done":false}` +
		"。新 id 必须与已有步骤不同。\n" +
		`- 若已完成,只输出 JSON:{"done":true}` +
		"\n只输出 JSON,不要其它文字。")
	return b.String()
}

// parseDelta tolerantly extracts a planDelta JSON object from model text
// (handles ```json fences and surrounding prose).
func parseDelta(text string) *planDelta {
	s := strings.TrimSpace(text)
	if i := strings.Index(s, "```"); i >= 0 {
		s = s[i+3:]
		if j := strings.IndexByte(s, '\n'); j >= 0 {
			s = s[j+1:]
		}
		if k := strings.LastIndex(s, "```"); k >= 0 {
			s = s[:k]
		}
		s = strings.TrimSpace(s)
	}
	if !strings.HasPrefix(s, "{") {
		a := strings.IndexByte(s, '{')
		b := strings.LastIndexByte(s, '}')
		if a < 0 || b <= a {
			return nil
		}
		s = s[a : b+1]
	}
	var d planDelta
	if json.Unmarshal([]byte(s), &d) != nil {
		return nil
	}
	return &d
}

// --- plan state (checkpointable) --------------------------------------------

type nodeSpec struct {
	ID        string   `json:"id"`
	Task      string   `json:"task"`
	DependsOn []string `json:"depends_on,omitempty"`
	Approve   bool     `json:"approve,omitempty"`
}

type planState struct {
	Status       map[string]string `json:"status"`
	Output       map[string]string `json:"output"`
	Attempts     map[string]int    `json:"attempts"`
	Dynamic      []nodeSpec        `json:"dynamic,omitempty"`
	ReplanRounds int               `json:"replan_rounds,omitempty"`
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
	for _, ns := range s.Dynamic {
		if st := s.Status[ns.ID]; st == "running" {
			s.Status[ns.ID] = "pending"
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
