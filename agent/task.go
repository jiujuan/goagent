package agent

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// TaskKind names the execution form of a runtime task. It is not a business
// task type: it only distinguishes the entry points that produce a run.
type TaskKind string

const (
	TaskKindChat     TaskKind = "chat"
	TaskKindWorkflow TaskKind = "workflow"
	TaskKindPlan     TaskKind = "plan"
)

// TaskInfo is a read-only runtime description of one task. It is never
// persisted: a resumed run gets a new root task under the same turn.
type TaskInfo struct {
	ID       string
	ParentID string
	ThreadID string
	TurnID   string
	Kind     TaskKind
	Depth    int
}

// TaskBudget bounds one task tree. A task tree shares these limits across
// every child task (sub-agents, parallel branches, plan nodes) and every model
// request made by those tasks. A pause/resume starts a new tree and therefore
// a fresh count, while a delegation transfer stays inside the same task.
//
// It is not middleware.RunBudget: this budget is a hard error at the point of
// use, has no token/cost dimensions, and never wraps up a reply.
type TaskBudget struct {
	// MaxDepth is the maximum task nesting depth; 0 uses the default (8).
	MaxDepth int
	// MaxUnits is the maximum number of child tasks in the tree; 0 uses the
	// default (64).
	MaxUnits int
	// MaxTurns is the maximum number of model requests in the tree; 0 means
	// no limit.
	MaxTurns int
}

// defaultTaskBudget is the budget a task tree uses when none is configured.
func defaultTaskBudget() TaskBudget {
	return TaskBudget{MaxDepth: 8, MaxUnits: 64}
}

// normalizeTaskBudget replaces the zero value of each bounded dimension with
// its default. MaxTurns keeps 0 as "unlimited".
func normalizeTaskBudget(b TaskBudget) TaskBudget {
	d := defaultTaskBudget()
	if b.MaxDepth == 0 {
		b.MaxDepth = d.MaxDepth
	}
	if b.MaxUnits == 0 {
		b.MaxUnits = d.MaxUnits
	}
	return b
}

// taskRuntime is the state one task tree shares: its budget and the counters
// every derived environment charges. The counters are atomic because parallel
// branches and sub-agent runs admit and consume concurrently.
type taskRuntime struct {
	rootID string
	budget TaskBudget
	units  atomic.Int64
	turns  atomic.Int64
}

// ErrTaskBudgetExceeded identifies every TaskBudgetError.
var ErrTaskBudgetExceeded = errors.New("agent: task budget exceeded")

// TaskBudgetError reports which dimension of a task tree budget ran out.
type TaskBudgetError struct {
	Dimension string // "depth" | "units" | "turns"
	Limit     int
	Used      int
	ParentID  string
	Kind      TaskKind
}

func (e *TaskBudgetError) Error() string {
	return fmt.Sprintf("agent: task budget exceeded: %s limit %d reached (used %d) at task %q (%s)",
		e.Dimension, e.Limit, e.Used, e.ParentID, e.Kind)
}

// Unwrap lets callers test errors.Is(err, ErrTaskBudgetExceeded).
func (e *TaskBudgetError) Unwrap() error { return ErrTaskBudgetExceeded }

// taskStatus maps a unit outcome to the TaskDone status. Pause wins over the
// error because a paused run carries no terminal error; a context cancellation
// (a canceled run or a canceled sibling branch) is reported as canceled.
func taskStatus(out runOutcome) string {
	switch {
	case out.Control.Kind == core.Interrupt:
		return "paused"
	case out.Err == nil:
		return "done"
	case errors.Is(out.Err, context.Canceled):
		return "canceled"
	default:
		return "failed"
	}
}

// taskKind reports the configured execution form. Agents built directly
// without New (workflow roots aside) fall back to chat.
func (a *Agent) taskKind() TaskKind {
	if a.cfg.taskKind == "" {
		return TaskKindChat
	}
	return a.cfg.taskKind
}

// taskInfo returns a copy of this environment's task identity.
func (rc *RunContext) taskInfo() TaskInfo {
	rc.taskMu.Lock()
	defer rc.taskMu.Unlock()
	return rc.info
}

// setTaskTurn fills the turn id resolved by prepare. A turn id is assigned
// once; an existing value (a resumed run) is never overwritten.
func (rc *RunContext) setTaskTurn(turnID string) {
	if turnID == "" {
		return
	}
	rc.taskMu.Lock()
	if rc.info.TurnID == "" {
		rc.info.TurnID = turnID
	}
	rc.taskMu.Unlock()
}

// ensureTask returns the shared task runtime, creating a root task for an
// environment that has none. It is concurrency-safe so parallel derivations of
// the same environment see one root.
func (rc *RunContext) ensureTask() *taskRuntime {
	rc.taskMu.Lock()
	defer rc.taskMu.Unlock()
	if rc.task == nil {
		rootID := rc.info.ID
		if rootID == "" {
			rootID = core.NewID("task")
		}
		rc.task = &taskRuntime{rootID: rootID, budget: defaultTaskBudget()}
	}
	if rc.info.ID == "" {
		rc.info.ID = rc.task.rootID
	}
	if rc.info.Kind == "" {
		rc.info.Kind = TaskKindChat
	}
	return rc.task
}

// taskState reads the shared runtime and identity together under taskMu so a
// derived environment never copies a half-updated pair.
func (rc *RunContext) taskState() (*taskRuntime, TaskInfo) {
	rc.taskMu.Lock()
	defer rc.taskMu.Unlock()
	return rc.task, rc.info
}

// admitChild registers a child task in the shared tree and returns its
// identity. ThreadID/TurnID are left for the caller: a shared-thread
// derivation fills the parent's, while an AsTool child run fills its own.
// Depth is checked before the units counter moves, so a rejected child costs
// no quota; the units increment is a CAS loop that never overshoots.
func (rc *RunContext) admitChild(kind TaskKind) (TaskInfo, error) {
	task := rc.ensureTask()
	parent := rc.taskInfo()
	if kind == "" {
		kind = TaskKindChat
	}
	depth := parent.Depth + 1
	if depth > task.budget.MaxDepth {
		return TaskInfo{}, &TaskBudgetError{
			Dimension: "depth",
			Limit:     task.budget.MaxDepth,
			Used:      depth,
			ParentID:  parent.ID,
			Kind:      kind,
		}
	}
	for {
		old := task.units.Load()
		if old+1 > int64(task.budget.MaxUnits) {
			return TaskInfo{}, &TaskBudgetError{
				Dimension: "units",
				Limit:     task.budget.MaxUnits,
				Used:      int(old),
				ParentID:  parent.ID,
				Kind:      kind,
			}
		}
		if task.units.CompareAndSwap(old, old+1) {
			break
		}
	}
	return TaskInfo{
		ID:       core.NewID("task"),
		ParentID: parent.ID,
		Kind:     kind,
		Depth:    depth,
	}, nil
}

// consumeTurn charges one model request to the shared tree budget. MaxTurns 0
// means unlimited and leaves the counter alone. A refused request does not
// increment, so Used always reports completed admissions.
func (rc *RunContext) consumeTurn() error {
	task := rc.ensureTask()
	info := rc.taskInfo()
	limit := int64(task.budget.MaxTurns)
	if limit == 0 {
		return nil
	}
	for {
		old := task.turns.Load()
		if old+1 > limit {
			return &TaskBudgetError{
				Dimension: "turns",
				Limit:     int(limit),
				Used:      int(old),
				ParentID:  info.ID,
				Kind:      info.Kind,
			}
		}
		if task.turns.CompareAndSwap(old, old+1) {
			return nil
		}
	}
}

// deriveChildTask admits a child and completes the identity a derived
// execution environment needs: the shared tree runtime plus the parent's
// thread and turn. A manually built parent without task state gets a root on
// demand, using its ThreadID for the child.
func (rc *RunContext) deriveChildTask(kind TaskKind) (TaskInfo, *taskRuntime, error) {
	child, err := rc.admitChild(kind)
	if err != nil {
		return TaskInfo{}, nil, err
	}
	task, parent := rc.taskState()
	child.ThreadID = parent.ThreadID
	if child.ThreadID == "" {
		child.ThreadID = rc.ThreadID
	}
	child.TurnID = parent.TurnID
	return child, task, nil
}

// adoptChildTask replaces the root identity of a run that was started as
// somebody else's child. The child run keeps its own thread; prepare fills its
// own turn later.
func (rc *RunContext) adoptChildTask(tree *taskRuntime, child TaskInfo, threadID string) {
	if tree == nil {
		return
	}
	child.ThreadID = threadID
	child.TurnID = ""
	rc.taskMu.Lock()
	rc.task = tree
	rc.info = child
	rc.taskMu.Unlock()
}

// runContextCarrier exposes the RunContext a tool call belongs to without
// changing the public tool.Context shape.
type runContextCarrier interface{ agentRunContext() *RunContext }

type runContextCarrierCtx struct {
	context.Context
	rc *RunContext
}

func (c runContextCarrierCtx) agentRunContext() *RunContext { return c.rc }

// UpdateTool forwards the Updater capability so wrapping a tool call context
// to carry its run does not hide partial tool updates.
func (c runContextCarrierCtx) UpdateTool(callID string, p core.Part) {
	if u, ok := c.Context.(tool.Updater); ok {
		u.UpdateTool(callID, p)
	}
}

func runContextOf(ctx context.Context) *RunContext {
	if c, ok := ctx.(runContextCarrier); ok {
		return c.agentRunContext()
	}
	return nil
}
