package runtime

import (
	"context"
	"fmt"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/vfs"
)

// Agent is a compiled, runnable agent handle produced by Runtime.Compile.
type Agent struct {
	rt       *Runtime
	spec     agent.Spec
	compiled agent.Runnable
}

// RunRequest starts a turn on a thread.
type RunRequest struct {
	ThreadID string
	Message  core.Message
	// Files optionally supplies a virtual filesystem backend; defaults to an
	// in-state one when absent and none is restored from a checkpoint.
	Files core.FileStore
}

// Start begins a new turn for req.ThreadID with the given user message. It
// restores prior state from the latest checkpoint (if any), appends the
// message, and returns a non-blocking *Run. The loop is driven lazily on first
// observation (Iter/Wait), so no events are missed.
func (a *Agent) Start(ctx context.Context, req RunRequest) (*Run, error) {
	if req.ThreadID == "" {
		return nil, fmt.Errorf("runtime: RunRequest.ThreadID is required")
	}
	state, err := a.restore(ctx, req.ThreadID)
	if err != nil {
		return nil, err
	}
	if req.Files != nil {
		state.Files = req.Files
	} else if state.Files == nil {
		state.Files = vfs.NewInState()
	}
	state.Messages = append(state.Messages, req.Message)
	return a.newRun(ctx, req.ThreadID, state), nil
}

// Resume continues a thread from its latest checkpoint without a new user
// message (e.g. after a pause). HITL approval injection arrives in a later
// stage; for now it re-drives from the restored state.
func (a *Agent) Resume(ctx context.Context, threadID string) (*Run, error) {
	cp, err := a.rt.store.Latest(ctx, threadID)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, fmt.Errorf("runtime: no checkpoint to resume for thread %q", threadID)
	}
	state := cp.State
	if state.Files == nil {
		state.Files = vfs.NewInState()
	}
	return a.newRun(ctx, threadID, &state), nil
}

// restore loads the latest checkpoint's state for a thread, or a fresh State.
func (a *Agent) restore(ctx context.Context, threadID string) (*core.State, error) {
	cp, err := a.rt.store.Latest(ctx, threadID)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return &core.State{}, nil
	}
	st := cp.State
	return &st, nil
}

func (a *Agent) newRun(ctx context.Context, threadID string, state *core.State) *Run {
	runCtx, cancel := context.WithCancel(ctx)
	runID := core.NewID("run")
	topic := bus.Topic(runID)
	rc := &agent.RunContext{
		Context:  runCtx,
		RunID:    runID,
		ThreadID: threadID,
		Bus:      a.rt.bus,
		Topic:    topic,
		Store:    a.rt.store,
		State:    state,
	}
	return &Run{
		ID:       runID,
		ThreadID: threadID,
		bus:      a.rt.bus,
		topic:    topic,
		rc:       rc,
		compiled: a.compiled,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}
