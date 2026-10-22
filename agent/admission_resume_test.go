package agent_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// resumeCoordinatorSpy counts the store operations a resume and an ordinary
// run reach, so a test can tell a refusal at the thread gate (no store work)
// from a refusal by the durable claim (store work happened).
type resumeCoordinatorSpy struct {
	checkpoint.ThreadCoordinator
	mu     sync.Mutex
	begins int
	claims int
}

func (s *resumeCoordinatorSpy) BeginRun(ctx context.Context, req checkpoint.BeginRunRequest) (checkpoint.BeginRunResult, error) {
	s.mu.Lock()
	s.begins++
	s.mu.Unlock()
	return s.ThreadCoordinator.BeginRun(ctx, req)
}

func (s *resumeCoordinatorSpy) Claim(ctx context.Context, req checkpoint.ClaimRequest) (checkpoint.Claim, error) {
	s.mu.Lock()
	s.claims++
	s.mu.Unlock()
	return s.ThreadCoordinator.Claim(ctx, req)
}

func (s *resumeCoordinatorSpy) counts() (begins, claims int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.begins, s.claims
}

func newResumeSpyStore() *resumeCoordinatorSpy {
	return &resumeCoordinatorSpy{ThreadCoordinator: checkpoint.NewMemory()}
}

func TestResumeFailsFastWhenBusy(t *testing.T) {
	ctx := context.Background()
	const threadID = "resume-busy"
	store := newResumeSpyStore()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFirst()

	var calls atomic.Int32
	model := mock.New("resume-busy", func(*llm.Request) *llm.Response {
		if calls.Add(1) == 2 {
			entered <- struct{}{}
			<-release
		}
		return mock.Text("ok")
	})
	a, err := agent.New(agent.WithModel(model), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	// The thread now has a checkpoint a resume could claim if the gate let it.
	if _, err := a.Run(ctx, "first", agent.OnThread(threadID)); err != nil {
		t.Fatalf("first run: %v", err)
	}
	_, claimsBefore := store.counts()

	secondDone := make(chan error, 1)
	go func() {
		_, err := a.Run(ctx, "second", agent.OnThread(threadID))
		secondDone <- err
	}()
	waitSignal(t, entered, "second run to reach the model")

	type resumeResult struct {
		run *agent.Run
		err error
	}
	resumeDone := make(chan resumeResult, 1)
	go func() {
		run, err := a.Resume(ctx, threadID)
		resumeDone <- resumeResult{run: run, err: err}
	}()
	select {
	case got := <-resumeDone:
		if got.run != nil {
			t.Fatal("Resume on a busy thread returned a run handle")
		}
		if !errors.Is(got.err, checkpoint.ErrClaimConflict) {
			t.Fatalf("Resume on a busy thread error = %v, want ErrClaimConflict", got.err)
		}
	case <-time.After(2 * time.Second):
		releaseFirst()
		t.Fatal("Resume queued behind the active run instead of failing fast")
	}
	if _, claimsAfter := store.counts(); claimsAfter != claimsBefore {
		t.Fatalf("Resume claimed the thread (claims %d -> %d); want a refusal before any claim", claimsBefore, claimsAfter)
	}

	releaseFirst()
	if err := waitError(t, secondDone, "second run"); err != nil {
		t.Fatal(err)
	}
}

func TestResumeReleasesGateOnPreflightError(t *testing.T) {
	ctx := context.Background()

	t.Run("no checkpoint", func(t *testing.T) {
		const threadID = "resume-no-checkpoint"
		a, err := agent.New(
			agent.WithModel(mock.New("resume-empty", func(*llm.Request) *llm.Response { return mock.Text("ok") })),
			agent.WithCheckpointer(checkpoint.NewMemory()),
		)
		if err != nil {
			t.Fatal(err)
		}
		if run, err := a.Resume(ctx, threadID); err == nil || run != nil {
			t.Fatalf("Resume on an empty thread = (%v, %v), want a validation error", run, err)
		}
		if _, err := a.Run(ctx, "after failed resume", agent.OnThread(threadID)); err != nil {
			t.Fatalf("run after a failed Resume: %v; want the thread slot released", err)
		}
	})

	t.Run("in-flight preflight refuses the batch", func(t *testing.T) {
		const threadID = "resume-inflight-preflight"
		store := newResumeSpyStore()
		ghost := newRecordingTool("ghost")
		calls := []core.ToolCall{{ID: "c1", Name: "ghost"}}
		seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, ghost),
			[]core.Message{core.UserText("go"), pairFor(calls)})
		a, err := agent.New(
			agent.WithModel(mock.New("resume-inflight-preflight", func(*llm.Request) *llm.Response { return mock.Text("ok") })),
			agent.WithTools(newRecordingTool("rec")),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}
		_, err = a.Resume(ctx, threadID)
		if !errors.Is(err, agent.ErrToolUnavailable) {
			t.Fatalf("Resume with an unresolvable in-flight call = %v, want ErrToolUnavailable", err)
		}
		_, err = a.Run(ctx, "next", agent.OnThread(threadID))
		if !errors.Is(err, checkpoint.ErrResumeRequired) {
			t.Fatalf("run after a failed in-flight Resume = %v, want ErrResumeRequired", err)
		}
		if errors.Is(err, checkpoint.ErrClaimConflict) {
			t.Fatalf("the failed in-flight Resume kept the thread slot: %v", err)
		}
	})

	t.Run("preflight refuses the batch", func(t *testing.T) {
		const threadID = "resume-preflight"
		store := checkpoint.NewMemory()
		if _, err := store.Commit(ctx, checkpoint.CommitRequest{Checkpoint: &checkpoint.Checkpoint{
			ID:       core.NewID("seed-pause"),
			ThreadID: threadID,
			State:    core.State{Messages: []core.Message{core.UserText("go")}},
			Pending: &checkpoint.PendingHITL{Step: 0, Pending: []core.ToolCall{
				{ID: "c1", Name: "ghost"},
			}},
			Pause: &checkpoint.Pause{ID: "pause-1", Phase: "approval", Recovery: checkpoint.RecoveryResumeTools},
		}}); err != nil {
			t.Fatalf("seed paused head: %v", err)
		}
		a, err := agent.New(
			agent.WithModel(mock.New("resume-preflight", func(*llm.Request) *llm.Response { return mock.Text("ok") })),
			agent.WithTools(newRecordingTool("rec")),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}
		_, err = a.Resume(ctx, threadID)
		if !errors.Is(err, agent.ErrToolUnavailable) {
			t.Fatalf("Resume with an unresolvable pending call = %v, want ErrToolUnavailable", err)
		}
		_, err = a.Run(ctx, "next", agent.OnThread(threadID))
		if !errors.Is(err, checkpoint.ErrResumeRequired) {
			t.Fatalf("run after a failed Resume = %v, want ErrResumeRequired", err)
		}
		if errors.Is(err, checkpoint.ErrClaimConflict) {
			t.Fatalf("the failed Resume kept the thread slot: %v", err)
		}
	})
}

func TestResumeCancelBeforeDriveReleasesGate(t *testing.T) {
	ctx := context.Background()
	const threadID = "resume-cancel-undriven"
	store := newResumeSpyStore()
	model := mock.New("resume-cancel", func(*llm.Request) *llm.Response { return mock.Text("answer") })
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithMiddleware(&interruptAfterFinalOnce{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	paused := a.Stream(ctx, "start", agent.OnThread(threadID))
	if _, err := paused.Wait(); err != nil {
		t.Fatalf("pausing run: %v", err)
	}
	if !paused.Paused() {
		t.Fatal("first run did not pause")
	}

	resumed, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	// The undriven handle holds the thread slot and the claim; a run must be
	// turned away at the gate without reaching the store.
	beginsBefore, _ := store.counts()
	if _, err := a.Run(ctx, "blocked", agent.OnThread(threadID)); !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("run while the undriven resume holds the thread = %v, want ErrClaimConflict", err)
	}
	if beginsAfter, _ := store.counts(); beginsAfter != beginsBefore {
		t.Fatalf("the blocked run reached the store (begins %d -> %d); want a refusal at the gate", beginsBefore, beginsAfter)
	}

	resumed.Cancel()

	_, err = a.Run(ctx, "next", agent.OnThread(threadID))
	if !errors.Is(err, checkpoint.ErrResumeRequired) {
		t.Fatalf("run after canceling an undriven resume = %v, want ErrResumeRequired", err)
	}
	if errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("the canceled resume kept the thread slot or its claim: %v", err)
	}
	if beginsAfter, _ := store.counts(); beginsAfter != beginsBefore+1 {
		t.Fatalf("the run after Cancel did not reach the store (begins %d -> %d)", beginsBefore, beginsAfter)
	}
}

func TestResumeInFlightAdmission(t *testing.T) {
	ctx := context.Background()
	const threadID = "resume-inflight-admission"
	store := newResumeSpyStore()
	rec := newRecordingTool("rec")
	calls := recoveryCalls("c1", "c2")
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 0, calls[0]),
		pendingExecution(t, threadID, 0, calls[1]))

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := a.Resume(ctx, threadID)
	if err != nil {
		t.Fatalf("Resume on an in-flight head: %v", err)
	}

	// The resume holds the thread before its first drive; a second run must be
	// turned away by the gate without touching the store.
	beginsBefore, _ := store.counts()
	secondDone := make(chan error, 1)
	go func() {
		_, err := a.Run(ctx, "second", agent.OnThread(threadID))
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, checkpoint.ErrClaimConflict) {
			t.Fatalf("run while the resumed handle holds the thread = %v, want ErrClaimConflict", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a run while the resumed handle holds the thread waited instead of failing fast")
	}
	if beginsAfter, _ := store.counts(); beginsAfter != beginsBefore {
		t.Fatalf("the second run reached the store (begins %d -> %d); want a refusal at the gate", beginsBefore, beginsAfter)
	}

	if _, err := resumed.Wait(); err != nil {
		t.Fatalf("resumed in-flight run: %v", err)
	}
	if got := len(rec.invocations()); got != 2 {
		t.Fatalf("handler invocations = %d, want both pending calls executed once", got)
	}
}

func TestCrossAgentSameThreadUsesClaim(t *testing.T) {
	ctx := context.Background()
	const threadID = "cross-agent-claim"
	store := newResumeSpyStore()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFirst()

	first := mock.New("cross-agent-first", func(*llm.Request) *llm.Response {
		entered <- struct{}{}
		<-release
		return mock.Text("first done")
	})
	var secondCalls atomic.Int32
	second := mock.New("cross-agent-second", func(*llm.Request) *llm.Response {
		secondCalls.Add(1)
		return mock.Text("second done")
	})
	a1, err := agent.New(agent.WithModel(first), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	a2, err := agent.New(agent.WithModel(second), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := a1.Run(ctx, "first", agent.OnThread(threadID))
		firstDone <- err
	}()
	waitSignal(t, entered, "first agent to reach the model")

	beginsBefore, _ := store.counts()
	if _, err := a2.Run(ctx, "second", agent.OnThread(threadID)); !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("second agent on a busy thread = %v, want ErrClaimConflict", err)
	}
	if beginsAfter, _ := store.counts(); beginsAfter != beginsBefore+1 {
		t.Fatalf("second agent store claims = %d, want %d: it was turned away before the claim layer", beginsAfter, beginsBefore+1)
	}
	if got := secondCalls.Load(); got != 0 {
		t.Fatalf("second agent reached the model %d times, want 0", got)
	}

	releaseFirst()
	if err := waitError(t, firstDone, "first agent run"); err != nil {
		t.Fatal(err)
	}
}
