package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func TestHandleWaitWithoutWorkerDoesNotStartRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		a, err := agent.New(agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
			calls.Add(1)
			return mock.Text("worker only")
		})))
		if err != nil {
			t.Fatal(err)
		}
		h, err := EnqueueAgent(context.Background(), NewMemQueue(1), a, "go")
		if err != nil {
			t.Fatal(err)
		}
		defer h.Cancel()

		waited := make(chan error, 1)
		go func() {
			_, err := h.Wait()
			waited <- err
		}()
		synctest.Wait()

		if got := calls.Load(); got != 0 {
			t.Fatalf("Handle.Wait started the run without a worker: model calls = %d", got)
		}
		select {
		case err := <-waited:
			t.Fatalf("Handle.Wait returned before a worker or cancellation: %v", err)
		default:
		}
		assertWorkAlive(t, h, "waiting without a worker")

		h.Cancel()
		if err := <-waited; !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait after prepared cancellation = %v, want context.Canceled", err)
		}
		assertWorkReleased(t, h, "prepared cancellation")
	})
}

func TestWorkerStartsRunBeforeAwait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newSettleStore()
		a := durableTextAgent(t, store, "started by worker")
		q := &recordQueue{}
		h, err := EnqueueAgent(context.Background(), q, a, "go")
		if err != nil {
			t.Fatal(err)
		}
		defer h.Cancel()
		_, jobs := q.snapshot()
		workerDone := make(chan error, 1)
		go func() { workerDone <- runQueueJob(jobs[0]) }()
		synctest.Wait()

		// No caller has waited on or otherwise driven the run.
		select {
		case err := <-workerDone:
			if err != nil {
				t.Fatalf("worker = %v", err)
			}
		default:
			t.Fatal("worker did not start the run before awaiting settlement")
		}
		res, err := h.WaitRecorded()
		if err != nil || res.Message.Text() != "started by worker" {
			t.Fatalf("WaitRecorded = %q, %v", res.Message.Text(), err)
		}
		if got := len(store.settleCommits()); got != 1 {
			t.Fatalf("terminal commits = %d, want 1", got)
		}
		assertWorkReleased(t, h, "worker settlement")
	})
}

func TestHandleWaitersRespectPoolLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const jobs, waitersPerJob, workers = 12, 16, 3
		release := make(chan struct{})
		releaseModels := sync.OnceFunc(func() { close(release) })
		var active, peak, calls atomic.Int32
		store := newSettleStore()
		a, err := agent.New(
			agent.WithCheckpointer(store),
			agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
				calls.Add(1)
				current := active.Add(1)
				for previous := peak.Load(); current > previous; previous = peak.Load() {
					if peak.CompareAndSwap(previous, current) {
						break
					}
				}
				defer active.Add(-1)
				<-release
				return mock.Text("pool owned")
			})),
		)
		if err != nil {
			t.Fatal(err)
		}
		q := NewMemQueue(jobs)
		handles := make([]*Handle, 0, jobs)
		defer func() {
			releaseModels()
			for _, h := range handles {
				h.Cancel()
			}
		}()
		waited := make(chan error, jobs*waitersPerJob)
		for i := range jobs {
			h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread(fmt.Sprintf("ownership-%d", i)))
			if err != nil {
				t.Fatal(err)
			}
			handles = append(handles, h)
			for range waitersPerJob {
				go func() {
					_, err := h.Wait()
					waited <- err
				}()
			}
		}
		q.Close()
		synctest.Wait()
		if got := calls.Load(); got != 0 {
			t.Fatalf("waiters started %d models before the pool dequeued any jobs", got)
		}
		if got := len(waited); got != 0 {
			t.Fatalf("%d waiters returned before the pool started", got)
		}

		poolDone := make(chan struct{})
		go func() {
			NewPool(q, workers).Run(context.Background())
			close(poolDone)
		}()
		synctest.Wait()
		if got := active.Load(); got != workers {
			t.Fatalf("active models at the gate = %d, want %d", got, workers)
		}
		if got := calls.Load(); got != workers {
			t.Fatalf("model calls before releasing pool slots = %d, want %d", got, workers)
		}
		if got := len(waited); got != 0 {
			t.Fatalf("%d waiters returned while all model gates were closed", got)
		}

		releaseModels()
		<-poolDone
		for range jobs * waitersPerJob {
			if err := <-waited; err != nil {
				t.Fatalf("Wait = %v", err)
			}
		}
		if got := peak.Load(); got > workers {
			t.Fatalf("peak model concurrency = %d, pool limit = %d", got, workers)
		}
		if got := calls.Load(); got != jobs {
			t.Fatalf("model calls = %d, want %d", got, jobs)
		}
		if got := active.Load(); got != 0 {
			t.Fatalf("active models after pool drain = %d", got)
		}
		for _, h := range handles {
			res, err := h.WaitRecorded()
			if err != nil || res.Message.Text() != "pool owned" {
				t.Fatalf("WaitRecorded = %q, %v", res.Message.Text(), err)
			}
		}
		if got := len(store.settleCommits()); got != jobs {
			t.Fatalf("terminal commits = %d, want %d", got, jobs)
		}
	})
}

func TestHandleWaitCanceledBeforeWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		a, err := agent.New(agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
			calls.Add(1)
			return mock.Text("must not run")
		})))
		if err != nil {
			t.Fatal(err)
		}
		h, err := EnqueueAgent(context.Background(), NewMemQueue(1), a, "go")
		if err != nil {
			t.Fatal(err)
		}
		h.Cancel()
		if _, err := h.Wait(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait after prepared cancellation = %v", err)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("canceled prepared run called the model %d times", got)
		}
		assertWorkReleased(t, h, "cancel before worker")
	})
}

func TestHandleWaitStillReleasesWorkContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newSettleStore()
		store.gateOn = true
		a := durableTextAgent(t, store, "settled")
		q := NewMemQueue(1)
		h, err := EnqueueAgent(context.Background(), q, a, "go")
		if err != nil {
			t.Fatal(err)
		}
		defer h.Cancel()
		q.Close()
		poolDone := make(chan struct{})
		go func() {
			NewPool(q, 1).Run(context.Background())
			close(poolDone)
		}()
		releaseRecord := sync.OnceFunc(func() { close(store.release) })
		defer releaseRecord()
		<-store.entered

		if _, err := h.Wait(); err != nil {
			t.Fatal(err)
		}
		assertWorkAlive(t, h, "record still in progress after Wait")
		releaseRecord()
		<-poolDone
		assertWorkReleased(t, h, "worker releases context after Wait")
	})
}

func TestHandleWaitRecordedWaitsForRecordAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls, recordCalls atomic.Int32
		store := newSettleStore()
		a, err := agent.New(
			agent.WithCheckpointer(store),
			agent.WithModel(mock.New("m", func(*llm.Request) *llm.Response {
				calls.Add(1)
				return mock.Text("must not run")
			})),
		)
		if err != nil {
			t.Fatal(err)
		}
		q := NewMemQueue(1)
		h, err := EnqueueAgent(context.Background(), q, a, "go")
		if err != nil {
			t.Fatal(err)
		}
		defer h.Cancel()
		boundRecord := h.recordCompletion
		h.recordCompletion = func(ctx context.Context, run *agent.Run) error {
			recordCalls.Add(1)
			return boundRecord(ctx, run)
		}
		q.Close()

		h.Cancel()
		if _, err := h.Wait(); !errors.Is(err, context.Canceled) {
			t.Fatalf("prepared cancellation did not settle memory Wait: %v", err)
		}
		recorded := recordedWait(h)
		synctest.Wait()
		select {
		case err := <-recorded:
			t.Fatalf("WaitRecorded returned without a worker's record attempt: %v", err)
		default:
		}
		if got := recordCalls.Load(); got != 0 {
			t.Fatalf("record calls before worker = %d", got)
		}
		if got := len(store.settleCommits()); got != 0 {
			t.Fatalf("terminal commits before worker = %d", got)
		}

		poolDone := make(chan struct{})
		go func() {
			NewPool(q, 1).Run(context.Background())
			close(poolDone)
		}()
		<-poolDone
		err = <-recorded
		if !errors.Is(err, context.Canceled) || !errors.Is(err, agent.ErrNoCommittedBoundary) || errors.Is(err, agent.ErrRunNotFinished) {
			t.Fatalf("late worker must preserve cancellation and report no boundary: %v", err)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("late worker executed the canceled prepared run: model calls = %d", got)
		}
		if got := recordCalls.Load(); got != 1 {
			t.Fatalf("record attempts = %d, want 1", got)
		}
		if got := len(store.settleCommits()); got != 0 {
			t.Fatalf("run without a boundary made %d terminal commits", got)
		}
		assertWorkReleased(t, h, "late worker after prepared cancellation")
	})
}

// Hold real claim cleanup before the run freezes its terminal outcome.
// Embedding settleStore keeps the existing terminal Commit spy intact.
type ownershipCleanupStore struct {
	*settleStore
	cleanupEntered chan struct{}
	releaseCleanup <-chan struct{}
}

func (s *ownershipCleanupStore) ReleaseClaim(ctx context.Context, claim checkpoint.Claim) error {
	close(s.cleanupEntered)
	<-s.releaseCleanup
	return s.settleStore.ReleaseClaim(ctx, claim)
}

func TestCanceledRunIsNotRecordedBeforeSettlement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cleanupEntered := make(chan struct{})
		cleanupRelease := make(chan struct{})
		releaseCleanup := sync.OnceFunc(func() { close(cleanupRelease) })
		defer releaseCleanup()
		store := &ownershipCleanupStore{
			settleStore:    newSettleStore(),
			cleanupEntered: cleanupEntered,
			releaseCleanup: cleanupRelease,
		}
		toolEntered := make(chan struct{})
		toolRelease := make(chan struct{})
		defer close(toolRelease)
		a := durableGateAgent(t, store, toolEntered, toolRelease)
		q := &recordQueue{}
		const threadID = "ownership-cancel-cleanup"
		h, err := EnqueueAgent(context.Background(), q, a, "go", agent.OnThread(threadID))
		if err != nil {
			t.Fatal(err)
		}
		defer h.Cancel()

		// Install the entry probe before the worker starts; never replace it
		// concurrently with the worker's call to RecordCompletion.
		var recordCalls atomic.Int32
		boundRecord := h.recordCompletion
		h.recordCompletion = func(ctx context.Context, run *agent.Run) error {
			recordCalls.Add(1)
			return boundRecord(ctx, run)
		}
		_, jobs := q.snapshot()
		workerDone := make(chan error, 1)
		go func() { workerDone <- runQueueJob(jobs[0]) }()
		recorded := recordedWait(h)
		<-toolEntered

		// Prove this run already committed a boundary. A no-boundary run could
		// reject an early record without ever reaching the terminal Commit spy.
		boundary, err := store.Latest(context.Background(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if boundary == nil || boundary.RunID != h.run.ID || boundary.Revision == 0 || boundary.Completion != nil {
			t.Fatalf("missing this run's committed execution boundary: %+v", boundary)
		}

		h.Cancel()
		<-cleanupEntered
		synctest.Wait()
		assertWorkReleased(t, h, "execution canceled while cleanup is blocked")
		if got := recordCalls.Load(); got != 0 {
			t.Errorf("RecordCompletion entered %d times before cleanup settled", got)
		}
		if got := len(store.settleCommits()); got != 0 {
			t.Errorf("terminal commits before cleanup settled = %d", got)
		}
		select {
		case err := <-workerDone:
			t.Errorf("worker returned before cleanup settled: %v", err)
		default:
		}
		select {
		case err := <-recorded:
			t.Errorf("WaitRecorded returned before cleanup settled: %v", err)
		default:
		}
		if t.Failed() {
			return
		}

		releaseCleanup()
		workerErr, recordErr := <-workerDone, <-recorded
		for _, err := range []error{workerErr, recordErr} {
			if !errors.Is(err, context.Canceled) || errors.Is(err, agent.ErrRunNotFinished) || errors.Is(err, agent.ErrNoCommittedBoundary) {
				t.Fatalf("settled canceled run lost its boundary or recorded too early: %v", err)
			}
		}
		if got := recordCalls.Load(); got != 1 {
			t.Fatalf("record calls after cleanup = %d, want 1", got)
		}
		if got := len(store.settleCommits()); got != 1 {
			t.Fatalf("terminal commits after cleanup = %d, want 1", got)
		}
		head, err := store.Latest(context.Background(), threadID)
		if err != nil {
			t.Fatal(err)
		}
		if head.Completion == nil || head.Completion.RunID != h.run.ID || head.Completion.Kind != checkpoint.CompletionFailed {
			t.Fatalf("cancellation terminal record = %+v", head.Completion)
		}
		_, recordCtxErr, _ := store.observedSettle()
		if recordCtxErr != nil {
			t.Fatalf("terminal record inherited execution cancellation: %v", recordCtxErr)
		}
	})
}

func TestHandleWaitRecordedCloneFailureNeverSharesResult(t *testing.T) {
	originalErr := errors.New("record failed")
	h := &Handle{settle: make(chan struct{})}
	h.recordSettled(core.Result{Message: core.Message{Parts: []core.Part{&core.Image{Data: []byte{1}}}}}, originalErr)
	result, err := h.WaitRecorded()
	if !errors.Is(err, originalErr) || !errors.Is(err, core.ErrStateNotCloneable) {
		t.Fatalf("error=%v", err)
	}
	if len(result.Message.Parts) != 0 {
		t.Fatal("uncopyable result escaped to a waiter")
	}
}
