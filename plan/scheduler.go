package plan

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/session"
)

// scheduler executes a validated plan as a dependency graph. Each step runs in
// its own goroutine that first waits on its dependencies' done-channels, then
// (subject to a concurrency cap) runs the executor. This is the classic
// channel-DAG pattern: independent steps proceed concurrently, dependents block
// until every upstream signals done. Coordination is hand-rolled with a
// WaitGroup and a cancelable context — the same style as agent.ParallelAgent —
// so no extra dependency is pulled in.
type scheduler struct {
	plan     *Plan
	maxConc  int
	backend  Backend
	approver Approver
	author   string

	mu    sync.Mutex    // guards every step's runtime fields and snapshot reads
	state session.State // Session owns synchronization across workers and Runner
	ictx  agent.InvocationContext
}

func newScheduler(p *Plan, maxConc int, backend Backend, approver Approver, author string, ictx agent.InvocationContext) *scheduler {
	if maxConc <= 0 {
		maxConc = runtime.GOMAXPROCS(0)
		if maxConc < 1 {
			maxConc = 1
		}
	}
	return &scheduler{
		plan:     p,
		maxConc:  maxConc,
		backend:  backend,
		approver: approver,
		author:   author,
		state:    ictx.MutableState(),
		ictx:     ictx,
	}
}

// run executes every step, emitting an event for each state transition into
// emit, and closes emit when the whole plan has settled. It blocks until done.
func (s *scheduler) run(emit chan<- *core.Event) {
	defer close(emit)

	done := make(map[string]chan struct{}, len(s.plan.Steps))
	for _, st := range s.plan.Steps {
		done[st.ID] = make(chan struct{})
	}
	pool := newPool(s.backend, s.maxConc, len(s.plan.Steps))
	defer pool.close()
	ctx, cancel := context.WithCancel(s.ictx)
	defer cancel()

	var wg sync.WaitGroup
	for _, st := range s.plan.Steps {
		wg.Add(1)
		go func(st *Step) {
			defer wg.Done()
			defer close(done[st.ID]) // closed on every exit path, waking dependents

			// Already-terminal steps come from a resumed snapshot: don't re-run.
			if s.status(st).terminal() {
				return
			}

			// 1. Wait for every dependency to finish (or the plan to be canceled).
			for _, dep := range st.DependsOn {
				select {
				case <-done[dep]:
				case <-ctx.Done():
					s.settle(emit, st, Blocked, nil)
					return
				}
			}

			// 2. If an upstream broke under an aborting policy, this step is blocked.
			if s.upstreamBroken(st) {
				s.settle(emit, st, Blocked, nil)
				return
			}

			// 3. Approval gate.
			if dec, err := approveStep(ctx, s.approver, st); err != nil || !dec.Approve {
				s.settle(emit, st, Skipped, &StepResult{StepID: st.ID, Title: st.Name, Err: denyReason(dec, err)})
				return
			}

			// 4. Concurrency gate, then execute with retry + per-attempt timeout.
			// The pool bounds how many executors run at once; it returns false if
			// the plan was canceled before this step could start.
			ran := pool.run(ctx, func() {
				s.mark(st, Running)
				emit <- s.progressEvent(st)

				res, err := s.runWithRetry(ctx, st)
				if err != nil {
					result := &StepResult{StepID: st.ID, Title: st.Name, Err: err.Error()}
					switch st.OnError {
					case PolicySkip:
						s.settle(emit, st, Skipped, result)
					case PolicyContinue:
						s.settle(emit, st, Failed, result)
					default: // PolicyFail: abort the rest of the plan
						s.settle(emit, st, Failed, result)
						cancel()
					}
					return
				}
				s.state.Set(StepResultKey(st.ID), res.Output)
				s.settle(emit, st, Done, res)
			})
			if !ran {
				s.settle(emit, st, Blocked, nil)
			}
		}(st)
	}
	wg.Wait()
}

// runWithRetry runs the step's executor, retrying on error per its RetryPolicy.
// Each attempt is bounded by st.Timeout (when set). It honors ctx cancellation
// between and during attempts.
func (s *scheduler) runWithRetry(ctx context.Context, st *Step) (*StepResult, error) {
	var lastErr error
	for attempt := 0; attempt <= st.Retry.Max; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if attempt > 0 && st.Retry.Backoff > 0 {
			if !sleep(ctx, st.Retry.Backoff<<(attempt-1)) {
				return nil, ctx.Err()
			}
		}
		s.incAttempt(st)

		actx := ctx
		var cancelAttempt context.CancelFunc
		if st.Timeout > 0 {
			actx, cancelAttempt = context.WithTimeout(ctx, st.Timeout)
		}
		sc := &StepContext{Context: actx, State: s.state, Step: st, ictx: s.ictx}
		res, err := st.Exec.Execute(sc)
		if cancelAttempt != nil {
			cancelAttempt()
		}
		if err == nil {
			if res == nil {
				res = &StepResult{StepID: st.ID, Title: st.Name}
			}
			return res, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// upstreamBroken reports whether any direct dependency broke under a policy that
// blocks dependents. Dependencies are guaranteed terminal here (their done
// channels closed before this is called).
func (s *scheduler) upstreamBroken(st *Step) bool {
	index := s.plan.byID()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dep := range st.DependsOn {
		up := index[dep]
		if up == nil {
			continue
		}
		switch up.Status {
		case Blocked:
			return true
		case Failed:
			// Failed only persists for non-aborting policies (PolicyContinue),
			// which by definition do not block dependents — but a hard-failed
			// upstream under PolicyFail also shows as Failed before cancel
			// propagates, so consult the policy explicitly.
			if up.OnError.blocksDependents() {
				return true
			}
		}
	}
	return false
}

// --- runtime-state mutation (all guarded by s.mu) ---------------------------

func (s *scheduler) status(st *Step) StepStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return st.Status
}

func (s *scheduler) mark(st *Step, status StepStatus) {
	s.mu.Lock()
	st.Status = status
	s.mu.Unlock()
}

func (s *scheduler) incAttempt(st *Step) {
	s.mu.Lock()
	st.Attempts++
	s.mu.Unlock()
}

// settle records a step's terminal status and result, then emits a committed
// event carrying the updated plan snapshot so the Runner persists it.
func (s *scheduler) settle(emit chan<- *core.Event, st *Step, status StepStatus, res *StepResult) {
	s.mu.Lock()
	st.Status = status
	if res != nil {
		st.Result = res
	}
	delta := snapshotDelta(s.plan)
	s.mu.Unlock()
	emit <- s.stepEvent(st, status, delta)
}

// --- event construction -----------------------------------------------------

// progressEvent is a transient (Partial) event announcing a step started.
func (s *scheduler) progressEvent(st *Step) *core.Event {
	return &core.Event{
		ID:           core.NewID("evt"),
		InvocationID: s.ictx.InvocationID,
		Author:       s.author,
		Branch:       s.ictx.Branch,
		Partial:      true,
		Progress:     &core.Progress{JobID: st.ID, Kind: "plan_step", Status: string(Running)},
	}
}

// stepEvent is a committed event for a terminal step transition. It carries the
// plan snapshot as StateDelta (persisted by the Runner) and a human-readable
// message line.
func (s *scheduler) stepEvent(st *Step, status StepStatus, delta map[string]any) *core.Event {
	msg := core.AssistantText(stepLine(st, status))
	return &core.Event{
		ID:           core.NewID("evt"),
		InvocationID: s.ictx.InvocationID,
		Author:       s.author,
		Branch:       s.ictx.Branch,
		Message:      &msg,
		Progress:     &core.Progress{JobID: st.ID, Kind: "plan_step", Status: string(status)},
		Actions:      core.Actions{StateDelta: delta},
	}
}

// stepLine renders a one-line human summary of a settled step.
func stepLine(st *Step, status StepStatus) string {
	name := st.Name
	if name == "" {
		name = st.ID
	}
	line := "[" + string(status) + "] " + name
	switch {
	case st.Result != nil && st.Result.Err != "":
		line += " — " + st.Result.Err
	case st.Result != nil && st.Result.Output != "":
		line += " — " + st.Result.Output
	}
	return line
}

// denyReason extracts a reason string from a denial decision or approver error.
func denyReason(dec middleware.Decision, err error) string {
	if err != nil {
		return "approval error: " + err.Error()
	}
	if dec.Reason != "" {
		return "approval denied: " + dec.Reason
	}
	return "approval denied"
}

// sleep waits for d or ctx cancellation; returns false if canceled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
