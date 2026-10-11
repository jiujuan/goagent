package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

type legacyCheckpointer struct {
	checkpoint.Checkpointer
}

type releaseFailCoordinator struct {
	checkpoint.ThreadCoordinator
	err error
}

type renewFailCoordinator struct {
	checkpoint.ThreadCoordinator
	err error
}

func (s *renewFailCoordinator) RenewClaim(context.Context, checkpoint.Claim, time.Duration) (checkpoint.Claim, error) {
	return checkpoint.Claim{}, s.err
}

func (s *releaseFailCoordinator) ReleaseClaim(ctx context.Context, claim checkpoint.Claim) error {
	if err := s.ThreadCoordinator.ReleaseClaim(ctx, claim); err != nil {
		return err
	}
	return s.err
}

func TestSnapshotConsistencyLazyRunsUseCurrentHead(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	a, err := agent.New(
		agent.WithCheckpointer(store),
		agent.WithModel(mock.New("test", func(*llm.Request) *llm.Response {
			return mock.Text("ok")
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Run(ctx, "seed", agent.OnThread("same")); err != nil {
		t.Fatal(err)
	}
	runA := a.Stream(ctx, "request-A", agent.OnThread("same"))
	runB := a.Stream(ctx, "request-B", agent.OnThread("same"))
	if _, err = runA.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err = runB.Wait(); err != nil {
		t.Fatal(err)
	}
	cp, err := store.Latest(ctx, "same")
	if err != nil {
		t.Fatal(err)
	}
	if cp == nil {
		t.Fatal("missing latest checkpoint")
	}
	var got []string
	for _, message := range cp.State.Messages {
		if message.Role == core.RoleUser {
			got = append(got, message.Text())
		}
	}
	want := []string{"seed", "request-A", "request-B"}
	if len(got) != len(want) {
		t.Fatalf("user messages = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("user messages = %v, want %v", got, want)
		}
	}
}

func TestSnapshotConsistencyRunMessageFrozenAtCreation(t *testing.T) {
	ctx := context.Background()
	var got core.Message
	a, err := agent.New(agent.WithModel(mock.New("test", func(req *llm.Request) *llm.Response {
		got = req.Messages[len(req.Messages)-1]
		return mock.Text("ok")
	})))
	if err != nil {
		t.Fatal(err)
	}

	imageData := []byte("original-image")
	toolArgs := json.RawMessage(`{"value":"original"}`)
	message := core.Message{Role: core.RoleUser, Parts: []core.Part{
		core.Image{MIME: "image/png", Data: imageData},
		core.ToolCall{ID: "call-1", Name: "inspect", Args: toolArgs},
	}}
	run := a.Stream(ctx, "ignored", agent.WithMessage(message))
	imageData[0] = 'X'
	toolArgs[10] = 'X'

	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	image := got.Parts[0].(core.Image)
	if string(image.Data) != "original-image" {
		t.Fatalf("image data = %q, want original-image", image.Data)
	}
	call := got.Parts[1].(core.ToolCall)
	if string(call.Args) != `{"value":"original"}` {
		t.Fatalf("tool args = %s, want original JSON", call.Args)
	}
}

func TestSnapshotConsistencyRequireCoordinator(t *testing.T) {
	model := mock.New("test", func(*llm.Request) *llm.Response { return mock.Text("ok") })
	_, err := agent.New(
		agent.WithModel(model),
		agent.WithCheckpointer(&legacyCheckpointer{Checkpointer: checkpoint.NewMemory()}),
		agent.WithThreadCoordinationRequired(true),
	)
	if !errors.Is(err, checkpoint.ErrCoordinationUnavailable) {
		t.Fatalf("New error = %v, want ErrCoordinationUnavailable", err)
	}

	if _, err := agent.New(
		agent.WithModel(model),
		agent.WithCheckpointer(checkpoint.NewMemory()),
		agent.WithThreadCoordinationRequired(true),
	); err != nil {
		t.Fatalf("New with coordinator: %v", err)
	}
}

func TestSnapshotConsistencyThreadOverlap(t *testing.T) {
	store := checkpoint.NewMemory()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var modelCalls atomic.Int32
	var toolCalls atomic.Int32

	model := mock.New("test", func(req *llm.Request) *llm.Response {
		modelCalls.Add(1)
		if req.Messages[len(req.Messages)-1].Role == core.RoleTool {
			return mock.Text("ok")
		}
		entered <- struct{}{}
		<-release
		user := req.Messages[len(req.Messages)-1].Text()
		return mock.CallTool("call-"+user, "side_effect", `{}`)
	})
	sideEffect := tool.New("side_effect", "count executions", func(*tool.Context, struct{}) (string, error) {
		toolCalls.Add(1)
		return "done", nil
	})
	a, err := agent.New(agent.WithModel(model), agent.WithTools(sideEffect), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	runA := a.Stream(context.Background(), "A", agent.OnThread("overlap"))
	resultA := make(chan error, 1)
	go func() {
		_, waitErr := runA.Wait()
		resultA <- waitErr
	}()
	waitSignal(t, entered, "first run to enter the model")

	if _, err := a.Stream(context.Background(), "B", agent.OnThread("overlap")).Wait(); !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("overlapping run error = %v, want ErrClaimConflict", err)
	}
	if got := modelCalls.Load(); got != 1 {
		t.Fatalf("model calls before releasing A = %d, want 1", got)
	}
	if got := toolCalls.Load(); got != 0 {
		t.Fatalf("tool calls before releasing A = %d, want 0", got)
	}

	close(release)
	if err := waitError(t, resultA, "first run"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Stream(context.Background(), "C", agent.OnThread("overlap")).Wait(); err != nil {
		t.Fatalf("run after release: %v", err)
	}
}

func TestSnapshotConsistencyDifferentThreads(t *testing.T) {
	store := checkpoint.NewMemory()
	entered := make(chan string, 2)
	release := make(chan struct{})
	model := mock.New("test", func(req *llm.Request) *llm.Response {
		entered <- req.Messages[len(req.Messages)-1].Text()
		<-release
		return mock.Text("ok")
	})
	a, err := agent.New(agent.WithModel(model), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 2)
	for _, threadID := range []string{"thread-a", "thread-b"} {
		run := a.Stream(context.Background(), threadID, agent.OnThread(threadID))
		go func() {
			_, waitErr := run.Wait()
			errs <- waitErr
		}()
	}
	waitSignal(t, entered, "first thread to enter the model")
	waitSignal(t, entered, "second thread to enter the model")
	close(release)
	for range 2 {
		if err := waitError(t, errs, "different-thread run"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshotConsistencyCleanupFailureIsTerminal(t *testing.T) {
	releaseErr := errors.New("release failed")
	store := &releaseFailCoordinator{ThreadCoordinator: checkpoint.NewMemory(), err: releaseErr}
	a, err := agent.New(
		agent.WithCheckpointer(store),
		agent.WithModel(mock.New("test", func(*llm.Request) *llm.Response { return mock.Text("ok") })),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Stream(context.Background(), "request", agent.OnThread("cleanup")).Wait(); !errors.Is(err, releaseErr) {
		t.Fatalf("Wait error = %v, want release failure", err)
	}
}

func TestSnapshotConsistencyPausedThreadRequiresResume(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var modelCalls atomic.Int32
	var toolCalls atomic.Int32
	model := mock.New("pause", func(req *llm.Request) *llm.Response {
		modelCalls.Add(1)
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "danger", `{}`)
	})
	danger := tool.New("danger", "dangerous operation", func(*tool.Context, struct{}) (string, error) {
		toolCalls.Add(1)
		return "done", nil
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(danger),
		agent.WithMiddleware(phase1ApprovalGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Stream(ctx, "pause", agent.OnThread("paused-thread")).Wait(); err != nil {
		t.Fatal(err)
	}
	before := modelCalls.Load()
	if _, err := a.Stream(ctx, "ordinary", agent.OnThread("paused-thread")).Wait(); !errors.Is(err, checkpoint.ErrResumeRequired) {
		t.Fatalf("ordinary run error = %v, want ErrResumeRequired", err)
	}
	if got := modelCalls.Load(); got != before {
		t.Fatalf("model calls after rejected ordinary run = %d, want %d", got, before)
	}
	if got := toolCalls.Load(); got != 0 {
		t.Fatalf("tool calls = %d, want 0", got)
	}
}

func TestSnapshotConsistencyLazyCancel(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var toolCalls atomic.Int32
	a := snapshotConsistencyPauseAgent(t, store, &toolCalls)
	if _, err := a.Stream(ctx, "pause", agent.OnThread("lazy-cancel")).Wait(); err != nil {
		t.Fatal(err)
	}

	prepared, err := a.Resume(ctx, "lazy-cancel", agent.Allow("c1"))
	if err != nil {
		t.Fatal(err)
	}
	prepared.Cancel()
	replacement, err := a.Resume(ctx, "lazy-cancel", agent.Allow("c1"))
	if err != nil {
		t.Fatalf("resume after lazy cancel: %v", err)
	}
	if _, err := prepared.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled prepared run error = %v, want context.Canceled", err)
	}
	if _, err := replacement.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := toolCalls.Load(); got != 1 {
		t.Fatalf("tool calls = %d, want 1 from replacement resume", got)
	}
}

func TestSnapshotConsistencyResumeExpiredBeforeStart(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	var toolCalls atomic.Int32
	pauser := snapshotConsistencyPauseAgent(t, base, &toolCalls)
	if _, err := pauser.Stream(ctx, "pause", agent.OnThread("expired-resume")).Wait(); err != nil {
		t.Fatal(err)
	}

	renewErr := checkpoint.ErrStaleFencingToken
	store := &renewFailCoordinator{ThreadCoordinator: base, err: renewErr}
	resumer := snapshotConsistencyPauseAgent(t, store, &toolCalls)
	prepared, err := resumer.Resume(ctx, "expired-resume", agent.Allow("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Wait(); !errors.Is(err, renewErr) {
		t.Fatalf("expired prepared resume error = %v, want ErrStaleFencingToken", err)
	}
	if got := toolCalls.Load(); got != 0 {
		t.Fatalf("tool calls = %d, want 0 before successful guard renewal", got)
	}
}

func TestSnapshotConsistencyLegacyStore(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.NewMemory()
	store := &legacyCheckpointer{Checkpointer: base}
	a, err := agent.New(
		agent.WithCheckpointer(store),
		agent.WithModel(mock.New("legacy", func(*llm.Request) *llm.Response { return mock.Text("ok") })),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "seed", agent.OnThread("legacy-thread")); err != nil {
		t.Fatal(err)
	}
	runA := a.Stream(ctx, "A", agent.OnThread("legacy-thread"))
	runB := a.Stream(ctx, "B", agent.OnThread("legacy-thread"))
	if _, err := runA.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := runB.Wait(); err != nil {
		t.Fatal(err)
	}
	cp, err := base.Latest(ctx, "legacy-thread")
	if err != nil {
		t.Fatal(err)
	}
	var users []string
	for _, message := range cp.State.Messages {
		if message.Role == core.RoleUser {
			users = append(users, message.Text())
		}
	}
	want := []string{"seed", "A", "B"}
	if len(users) != len(want) {
		t.Fatalf("legacy user messages = %v, want %v", users, want)
	}
	for i := range want {
		if users[i] != want[i] {
			t.Fatalf("legacy user messages = %v, want %v", users, want)
		}
	}
}

func snapshotConsistencyPauseAgent(t *testing.T, store checkpoint.Checkpointer, calls *atomic.Int32) *agent.Agent {
	t.Helper()
	model := mock.New("pause", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("done")
		}
		return mock.CallTool("c1", "danger", `{}`)
	})
	danger := tool.New("danger", "dangerous operation", func(*tool.Context, struct{}) (string, error) {
		calls.Add(1)
		return "done", nil
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(danger),
		agent.WithMiddleware(phase1ApprovalGate{}),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func waitSignal[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func waitError(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	return waitSignal(t, ch, what)
}
