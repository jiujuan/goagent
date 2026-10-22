package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// admissionLastUser returns the text of the newest user message in a request.
func admissionLastUser(req *llm.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == core.RoleUser {
			return req.Messages[i].Text()
		}
	}
	return ""
}

// admissionHasText reports whether any message in the history carries want.
func admissionHasText(msgs []core.Message, want string) bool {
	for _, m := range msgs {
		if m.Text() == want {
			return true
		}
	}
	return false
}

func TestSessionSendQueuesSameThread(t *testing.T) {
	ctx := context.Background()
	const threadID = "session-queue"
	store := checkpoint.NewMemory()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFirst()

	var calls atomic.Int32
	secondHistory := make(chan []core.Message, 1)
	model := mock.New("queue", func(req *llm.Request) *llm.Response {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
			return mock.Text("first answer")
		}
		secondHistory <- append([]core.Message(nil), req.Messages...)
		return mock.Text("second answer")
	})
	a, err := New(WithModel(model), WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := a.OpenSession(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := sess.Send(ctx, "first question")
		firstDone <- err
	}()
	waitSnapshotConsistencySignal(t, entered, "first send to reach the model")

	secondDone := make(chan error, 1)
	go func() {
		_, err := sess.Send(ctx, "second question")
		secondDone <- err
	}()
	waitGateQueued(t, &a.gate, threadID, 1)
	if got := calls.Load(); got != 1 {
		t.Fatalf("model calls while the second send waited = %d, want 1", got)
	}

	releaseFirst()
	if err := waitSnapshotConsistencyError(t, firstDone, "first send"); err != nil {
		t.Fatal(err)
	}
	if err := waitSnapshotConsistencyError(t, secondDone, "second send"); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("model calls after both sends = %d, want 2", got)
	}
	history := <-secondHistory
	if !admissionHasText(history, "first question") {
		t.Fatalf("second send did not see the first question: %v", history)
	}
	if !admissionHasText(history, "first answer") {
		t.Fatalf("second send did not see the first answer: %v", history)
	}
	if !admissionHasText(history, "second question") {
		t.Fatalf("second send did not carry its own question: %v", history)
	}
}

func TestAgentStreamFailsWhenBusy(t *testing.T) {
	ctx := context.Background()
	const threadID = "stream-busy"
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFirst()

	var calls atomic.Int32
	model := mock.New("busy", func(req *llm.Request) *llm.Response {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		return mock.Text("ok")
	})
	a, err := New(WithModel(model), WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := a.Stream(ctx, "first", OnThread(threadID)).Wait()
		firstDone <- err
	}()
	waitSnapshotConsistencySignal(t, entered, "first run to reach the model")

	_, err = a.Stream(ctx, "second", OnThread(threadID)).Wait()
	if !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("overlapping stream error = %v, want ErrClaimConflict", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("model calls with a busy thread = %d, want 1", got)
	}

	releaseFirst()
	if err := waitSnapshotConsistencyError(t, firstDone, "first run"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Run(ctx, "third", OnThread(threadID)); err != nil {
		t.Fatalf("run after the thread was released: %v", err)
	}
}

func TestSessionSendAdmissionFailOverride(t *testing.T) {
	ctx := context.Background()
	const threadID = "session-fail"
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFirst()

	var calls atomic.Int32
	model := mock.New("fail", func(req *llm.Request) *llm.Response {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		return mock.Text("ok")
	})
	a, err := New(WithModel(model), WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := a.OpenSession(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := sess.Send(ctx, "first")
		firstDone <- err
	}()
	waitSnapshotConsistencySignal(t, entered, "first send to reach the model")

	failDone := make(chan error, 1)
	go func() {
		_, err := sess.Send(ctx, "second", WithAdmission(AdmissionOptions{Policy: AdmissionFail}))
		failDone <- err
	}()
	select {
	case err := <-failDone:
		if !errors.Is(err, checkpoint.ErrClaimConflict) {
			t.Fatalf("fail-fast send error = %v, want ErrClaimConflict", err)
		}
	case <-time.After(2 * time.Second):
		releaseFirst()
		t.Fatal("session send with AdmissionFail queued instead of failing fast")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("model calls after the fail-fast send = %d, want 1", got)
	}

	releaseFirst()
	if err := waitSnapshotConsistencyError(t, firstDone, "first send"); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionReentrant(t *testing.T) {
	ctx := context.Background()
	const threadID = "reentrant"
	const childThreadID = "reentrant-child"

	var a *Agent
	innerErr := make(chan error, 1)
	childResult := make(chan string, 1)
	childErr := make(chan error, 1)

	reenter := tool.New("reenter", "reenter the same thread", func(tctx *tool.Context, _ struct{}) (string, error) {
		innerCtx, cancel := context.WithTimeout(tctx, 2*time.Second)
		defer cancel()
		_, err := a.Run(innerCtx, "inner", OnThread(threadID))
		innerErr <- err

		answer, err := a.Run(tctx, "child", OnThread(childThreadID))
		childResult <- answer
		childErr <- err
		return "reentered", nil
	})
	model := mock.New("reentrant", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("outer done")
		}
		switch admissionLastUser(req) {
		case "child":
			return mock.Text("child done")
		case "inner":
			return mock.Text("inner done")
		default:
			return mock.CallTool("reenter-call", "reenter", `{}`)
		}
	})
	var err error
	a, err = New(WithModel(model), WithTools(reenter), WithCheckpointer(checkpoint.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := a.Run(ctx, "outer", OnThread(threadID)); err != nil {
		t.Fatalf("outer run: %v", err)
	}
	if err := <-innerErr; !errors.Is(err, ErrAdmissionReentrant) {
		t.Fatalf("same-thread inner run error = %v, want ErrAdmissionReentrant", err)
	}
	if err := <-childErr; err != nil {
		t.Fatalf("different-thread inner run: %v", err)
	}
	if got := <-childResult; got != "child done" {
		t.Fatalf("different-thread inner answer = %q, want %q", got, "child done")
	}
}

func TestAdmissionWaiterCanceled(t *testing.T) {
	ctx := context.Background()
	const threadID = "canceled-waiter"
	store := checkpoint.NewMemory()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFirst()

	var calls atomic.Int32
	model := mock.New("cancel", func(req *llm.Request) *llm.Response {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		return mock.Text("ok")
	})
	a, err := New(WithModel(model), WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := a.OpenSession(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := sess.Send(ctx, "first")
		firstDone <- err
	}()
	waitSnapshotConsistencySignal(t, entered, "first send to reach the model")

	waiterCtx, cancelWaiter := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() {
		_, err := sess.Send(waiterCtx, "second")
		secondDone <- err
	}()
	waitGateQueued(t, &a.gate, threadID, 1)
	before, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}

	cancelWaiter()
	if err := waitSnapshotConsistencyError(t, secondDone, "canceled waiter"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}
	after, err := store.Latest(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if (before == nil) != (after == nil) || (before != nil && before.Revision != after.Revision) {
		t.Fatalf("canceled waiter changed the thread head: before %+v, after %+v", before, after)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("model calls after the waiter was canceled = %d, want 1", got)
	}

	thirdDone := make(chan error, 1)
	go func() {
		_, err := sess.Send(ctx, "third")
		thirdDone <- err
	}()
	waitGateQueued(t, &a.gate, threadID, 1)

	releaseFirst()
	if err := waitSnapshotConsistencyError(t, firstDone, "first send"); err != nil {
		t.Fatal(err)
	}
	if err := waitSnapshotConsistencyError(t, thirdDone, "next waiter"); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("model calls after first and next waiter = %d, want 2 (the canceled waiter never ran)", got)
	}
}

func TestAdmissionDifferentThreadsDoNotBlock(t *testing.T) {
	ctx := context.Background()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFirst()

	model := mock.New("threads", func(req *llm.Request) *llm.Response {
		if admissionLastUser(req) == "A" {
			entered <- struct{}{}
			<-release
		}
		return mock.Text("ok")
	})
	a, err := New(WithModel(model))
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := a.Stream(ctx, "A", OnThread("thread-a")).Wait()
		firstDone <- err
	}()
	waitSnapshotConsistencySignal(t, entered, "thread A to reach the model")

	secondDone := make(chan error, 1)
	go func() {
		_, err := a.Stream(ctx, "B", OnThread("thread-b")).Wait()
		secondDone <- err
	}()
	if err := waitSnapshotConsistencyError(t, secondDone, "thread B run"); err != nil {
		t.Fatalf("a run on another thread was blocked: %v", err)
	}

	releaseFirst()
	if err := waitSnapshotConsistencyError(t, firstDone, "thread A run"); err != nil {
		t.Fatal(err)
	}
}

type admissionApprovalGate struct{ BaseMiddleware }

func (admissionApprovalGate) BeforeTool(lc *LoopContext, call *core.ToolCall) (core.Directive, error) {
	if lc.IsApproved(call) {
		return core.Directive{}, nil
	}
	return core.Directive{Kind: core.Interrupt, Reason: "approval required"}, nil
}

func TestAdmissionReleasedOnPause(t *testing.T) {
	ctx := context.Background()
	const threadID = "pause-release"
	var calls atomic.Int32
	model := mock.New("pause", func(req *llm.Request) *llm.Response {
		calls.Add(1)
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("after tool")
		}
		return mock.CallTool("pause-call", "danger", `{}`)
	})
	danger := tool.New("danger", "needs approval", func(*tool.Context, struct{}) (string, error) {
		return "ran", nil
	})
	a, err := New(
		WithModel(model),
		WithTools(danger),
		WithMiddleware(admissionApprovalGate{}),
		WithCheckpointer(checkpoint.NewMemory()),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "pause", OnThread(threadID))
	if _, err := run.Wait(); err != nil {
		t.Fatalf("pausing run: %v", err)
	}
	if !run.Paused() {
		t.Fatal("first run did not pause")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("model calls before the second run = %d, want 1", got)
	}

	_, err = a.Stream(ctx, "second", OnThread(threadID)).Wait()
	if !errors.Is(err, checkpoint.ErrResumeRequired) {
		t.Fatalf("run after the pause error = %v, want ErrResumeRequired", err)
	}
	if errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("run after the pause was turned away by the admission gate: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("run after the pause reached the model: calls = %d, want 1", got)
	}
}
