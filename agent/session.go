package agent

import (
	"context"
	"fmt"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// Session is a thread-bound façade over an Agent: it opens nothing in the store,
// sends every input to the same thread, reads that thread's head on demand, and can
// branch off one of its committed checkpoints. It exists so callers do not have to
// hand-assemble Load, Fork, Save and OnThread — the combination that a checkpoint
// tree invites and that is easy to get subtly wrong.
//
// A Session holds no cached head: what a run continues from is resolved when that run
// actually starts (see Agent.Stream), so a session never parents onto a head that has
// since moved. Sessions are safe to use concurrently, as are the underlying entry
// points.
type Session struct {
	agent    *Agent
	threadID string
}

// OpenSession binds an agent to one thread. It validates the thread identifier and
// writes nothing: a thread that has never committed anything is a valid session, so a
// session is not evidence that a conversation exists.
func (a *Agent) OpenSession(ctx context.Context, threadID string) (*Session, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return nil, fmt.Errorf("agent: open session: %w", err)
	}
	return &Session{agent: a, threadID: threadID}, nil
}

// ThreadID is the thread this session is bound to.
func (s *Session) ThreadID() string { return s.threadID }

// Send runs one input on this session's thread and returns the structured outcome:
// identity, frozen result and the checkpoint boundary this run committed.
//
// The bound thread always wins: an OnThread option that names a different thread is
// rejected and nothing runs, because a session whose input silently moved to another
// thread would fork, read and resume the wrong history. Other run options, including
// WithRunFiles, pass through unchanged — an externally managed file backend still has
// to be supplied by the caller for each run it applies to (ADR-0027).
func (s *Session) Send(ctx context.Context, input string, opts ...RunOption) (RunResult, error) {
	rc := resolveRunConfig(input, s.threadID, opts...)
	if rc.ThreadID != s.threadID {
		return RunResult{ThreadID: s.threadID}, fmt.Errorf(
			"agent: session on thread %q: OnThread(%q) conflicts with the bound thread", s.threadID, rc.ThreadID)
	}
	run := s.agent.streamConfigured(ctx, rc)
	_, err := run.Wait()
	return run.DetailedResult(), err
}

// GetHead returns the thread's most recent checkpoint, or nil when it has committed
// nothing yet. It reads the store every time rather than caching, and it takes no
// claim, so opening or reading a session never blocks another owner.
func (s *Session) GetHead(ctx context.Context) (*checkpoint.Checkpoint, error) {
	return s.agent.store.Latest(ctx, s.threadID)
}

// ForkFrom starts a new thread off one of this session's committed checkpoints and
// returns a session bound to it.
//
// The source is read from this session's thread only, so a bare checkpoint ID cannot
// reach someone else's history. The copy is frozen and written as the new thread's
// first record with a compare-and-swap against an empty thread: the fork neither
// overwrites a target that already has checkpoints nor slips past a live resume claim,
// and no earlier "latest" lookup is consulted, so two concurrent forks to the same
// target cannot both believe they created it.
//
// A fork copies the snapshot, not the work: no tool re-runs, no approval decisions
// carry over, and the pause and terminal records of the source are cleared (ADR-0033 D4).
// File bytes that travelled with the checkpoint come along; an externally managed file
// backend does not, so the caller must supply the new thread's own backend to its first
// Send. Nothing is recorded on failure, and a failed fork returns no session.
func (s *Session) ForkFrom(ctx context.Context, checkpointID, newThreadID string) (*Session, error) {
	durable, ok := s.agent.store.(checkpoint.Durable)
	if !ok {
		return nil, fmt.Errorf("agent: fork from session on thread %q: %w", s.threadID, checkpoint.ErrDurableUnavailable)
	}
	if err := core.CheckThreadID(newThreadID); err != nil {
		return nil, fmt.Errorf("agent: fork from session on thread %q: %w", s.threadID, err)
	}
	source, err := durable.Load(ctx, s.threadID, checkpointID)
	if err != nil {
		return nil, fmt.Errorf("agent: fork from session on thread %q: load checkpoint %q: %w", s.threadID, checkpointID, err)
	}
	if source == nil || source.ID != checkpointID || source.ThreadID != s.threadID {
		return nil, fmt.Errorf("agent: checkpoint %q is not a record of session thread %q", checkpointID, s.threadID)
	}
	forked, err := checkpoint.Fork(source, newThreadID, core.NewID("cp"))
	if err != nil {
		return nil, fmt.Errorf("agent: fork session thread %q into %q: %w", s.threadID, newThreadID, err)
	}
	// Externally managed file backends belong to their source thread. Snapshot
	// bytes remain on FileSnapshot, while the target thread must inject its own
	// backend on a later Send.
	forked.State.Files = nil
	emptyThread := uint64(0)
	if _, err := durable.Commit(ctx, checkpoint.CommitRequest{
		Checkpoint:       forked,
		ExpectedRevision: &emptyThread,
	}); err != nil {
		return nil, fmt.Errorf("agent: fork session thread %q into %q: commit first checkpoint: %w", s.threadID, newThreadID, err)
	}
	return &Session{agent: s.agent, threadID: newThreadID}, nil
}
