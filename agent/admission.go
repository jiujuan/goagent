package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jiujuan/goagent/checkpoint"
)

// AdmissionPolicy selects what a run does when its thread already has an active
// run.
type AdmissionPolicy int

const (
	// AdmissionFail fails immediately when the thread is busy.
	AdmissionFail AdmissionPolicy = iota
	// AdmissionQueue waits for the active run to finish, in FIFO order.
	AdmissionQueue
)

// AdmissionOptions configures same-thread admission for one run.
type AdmissionOptions struct {
	Policy     AdmissionPolicy
	MaxWaiters int // per-thread queue bound; <= 0 normalizes to 16
}

const defaultAdmissionWaiters = 16

var (
	// ErrAdmissionQueueFull reports that a thread's waiter queue is at its bound.
	ErrAdmissionQueueFull = errors.New("agent: admission queue is full")
	// ErrAdmissionReentrant reports a run trying to enter its own thread from
	// inside that thread's active run.
	ErrAdmissionReentrant = errors.New("agent: reentrant admission on the same thread")
)

// WithAdmission sets this run's same-thread admission policy. Agent.Run/Stream
// default to AdmissionFail; Session.Send defaults to AdmissionQueue.
func WithAdmission(o AdmissionOptions) RunOption {
	if o.MaxWaiters <= 0 {
		o.MaxWaiters = defaultAdmissionWaiters
	}
	return func(r *RunConfig) { r.admission, r.admissionSet = o, true }
}

func validateAdmission(o AdmissionOptions) error {
	switch o.Policy {
	case AdmissionFail, AdmissionQueue:
		return nil
	default:
		return fmt.Errorf("agent: unknown admission policy %d", o.Policy)
	}
}

// threadScopeKey carries a run's thread through its context. A call made from
// inside that run inherits the value and can be recognized before it waits for
// a slot the run itself holds.
type threadScopeKey struct{}

type threadScope struct{ threadID string }

func withThreadScope(ctx context.Context, threadID string) context.Context {
	return context.WithValue(ctx, threadScopeKey{}, threadScope{threadID: threadID})
}

func checkReentrant(ctx context.Context, threadID string) error {
	if s, ok := ctx.Value(threadScopeKey{}).(threadScope); ok && s.threadID == threadID {
		return fmt.Errorf("agent: thread %q: %w", threadID, ErrAdmissionReentrant)
	}
	return nil
}

// acquireAdmission reserves this run's thread slot. A resume may already hold
// the slot and have attached its release to the handle, in which case it is
// reused rather than acquired twice.
func (r *Run) acquireAdmission() error {
	r.lifecycleMu.Lock()
	held := r.gateRelease != nil
	r.lifecycleMu.Unlock()
	if held {
		return nil
	}
	release, err := r.agent.gate.acquire(r.rc.Context, r.identity.threadID, r.admission.Policy, r.admission.MaxWaiters)
	if err != nil {
		return err
	}
	r.lifecycleMu.Lock()
	r.gateRelease = release
	r.lifecycleMu.Unlock()
	return nil
}

// releaseAdmission frees the thread slot if this run holds it. Every exit calls
// it; only the first call releases.
func (r *Run) releaseAdmission() {
	r.lifecycleMu.Lock()
	release := r.gateRelease
	r.gateRelease = nil
	r.lifecycleMu.Unlock()
	if release != nil {
		release()
	}
}

// threadGate serializes runs that target one thread. The zero value is ready to
// use. A slot is always held by at most one run; releasing it hands it directly
// to the oldest waiter so a newcomer cannot barge in.
type threadGate struct {
	mu      sync.Mutex
	busy    map[string]bool
	waiters map[string][]*gateWaiter
}

type gateWaiter struct {
	ready   chan struct{}
	granted bool
}

// acquire reserves threadID's slot or waits for it according to policy.
// maxWaiters bounds each thread's waiter queue and must be positive.
func (g *threadGate) acquire(ctx context.Context, threadID string, policy AdmissionPolicy, maxWaiters int) (func(), error) {
	g.mu.Lock()
	if g.busy == nil {
		g.busy = make(map[string]bool)
	}
	if g.waiters == nil {
		g.waiters = make(map[string][]*gateWaiter)
	}
	if !g.busy[threadID] {
		// Queue behind existing waiters even while idle, preserving FIFO.
		if policy == AdmissionQueue && len(g.waiters[threadID]) > 0 {
			return g.enqueueLocked(ctx, threadID, maxWaiters)
		}
		g.busy[threadID] = true
		g.mu.Unlock()
		return g.releaser(threadID), nil
	}
	if policy == AdmissionFail {
		g.mu.Unlock()
		return nil, fmt.Errorf("agent: thread %q is busy: %w", threadID, checkpoint.ErrClaimConflict)
	}
	return g.enqueueLocked(ctx, threadID, maxWaiters)
}

// enqueueLocked appends a waiter and waits for its grant. It is called with
// g.mu held and releases the lock before blocking.
func (g *threadGate) enqueueLocked(ctx context.Context, threadID string, maxWaiters int) (func(), error) {
	if len(g.waiters[threadID]) >= maxWaiters {
		g.mu.Unlock()
		return nil, ErrAdmissionQueueFull
	}
	w := &gateWaiter{ready: make(chan struct{})}
	g.waiters[threadID] = append(g.waiters[threadID], w)
	g.mu.Unlock()
	return g.wait(ctx, threadID, w)
}

func (g *threadGate) wait(ctx context.Context, threadID string, w *gateWaiter) (func(), error) {
	select {
	case <-w.ready:
		if ctx.Err() != nil {
			// Granted, but the caller gave up first: hand the slot on.
			g.release(threadID)
			return nil, ctx.Err()
		}
		return g.releaser(threadID), nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.granted {
			g.mu.Unlock()
			g.release(threadID)
			return nil, ctx.Err()
		}
		g.waiters[threadID] = removeGateWaiter(g.waiters[threadID], w)
		if len(g.waiters[threadID]) == 0 {
			delete(g.waiters, threadID)
		}
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}

func removeGateWaiter(q []*gateWaiter, w *gateWaiter) []*gateWaiter {
	for i, x := range q {
		if x == w {
			return append(q[:i], q[i+1:]...)
		}
	}
	return q
}

// releaser wraps release in sync.Once so one granted run releases exactly once.
func (g *threadGate) releaser(threadID string) func() {
	var once sync.Once
	return func() { once.Do(func() { g.release(threadID) }) }
}

func (g *threadGate) release(threadID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for len(g.waiters[threadID]) > 0 {
		w := g.waiters[threadID][0]
		g.waiters[threadID] = g.waiters[threadID][1:]
		if w.granted {
			continue
		}
		w.granted = true
		close(w.ready)
		return // stay busy: the slot transferred
	}
	delete(g.waiters, threadID)
	g.busy[threadID] = false
}
