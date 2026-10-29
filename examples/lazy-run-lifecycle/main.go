// Command lazy-run-lifecycle asserts the Run and queue lifecycle entirely offline.
// Models are mocks, checkpoints are in memory, and channel/context barriers replace
// sleeps. A terminal Commit is held open to distinguish Wait from WaitRecorded.
//
//	go run ./examples/lazy-run-lifecycle
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/queue"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel() // Failure guard for channel receives, not a scheduling delay.
	demonstrateStart(ctx)
	demonstrateIter(ctx)
	demonstrateQueue(ctx)
	fmt.Println("All offline lifecycle assertions passed.")
}

func demonstrateStart(ctx context.Context) {
	var calls atomic.Int32
	entered, responseGate := make(chan struct{}), make(chan struct{})
	releaseResponse := sync.OnceFunc(func() { close(responseGate) })
	defer releaseResponse()
	a := newAgent(mock.New("offline-start", func(*llm.Request) *llm.Response {
		calls.Add(1)
		close(entered)
		select {
		case <-responseGate:
		case <-ctx.Done():
		}
		return mock.Text("started once")
	}))

	run := a.Stream(ctx, "hello")
	defer run.Cancel()
	check(calls.Load() == 0, "Stream started the model")
	events, unsubscribe := run.Events(bus.Lossless)
	defer unsubscribe() // Also needed on an early return from raw Events.
	check(calls.Load() == 0, "Events started the model")

	// Await on a prepared Run neither starts it nor cancels it.
	expired, stopWaiting := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer stopWaiting()
	_, err := run.Await(expired)
	check(errors.Is(err, context.DeadlineExceeded), "prepared Await: %v", err)
	check(calls.Load() == 0, "prepared Await started the model")

	run.Start() // Events -> Start -> Await; Start returns before the model does.
	run.Start() // Idempotent even before the first response.
	receive(ctx, entered)
	check(calls.Load() == 1, "Start model calls = %d", calls.Load())
	// The model is inside its response barrier. This timeout stops only us.
	_, err = run.Await(expired)
	check(errors.Is(err, context.DeadlineExceeded), "started Await: %v", err)
	releaseResponse()
	result, err := run.Await(ctx)
	check(err == nil && result.Message.Text() == "started once", "Await canceled execution: %v", err)

	// Settlement is not a drain barrier. Keep receiving until channel closure;
	// do not infer a complete tail from a nonblocking select/default drain.
	observed := readAll(ctx, events)
	check(len(observed) >= 2, "missing live terminal events")
	assertSuccessTail(observed[len(observed)-2:])

	late, stopLate := run.Events(bus.Lossless)
	defer stopLate()
	assertSuccessTail(readAll(ctx, late))
	assertSuccessTail(iterEvents(run))
	run.Start() // A sealed Run never restarts.
	check(calls.Load() == 1, "late observation reran the model")
	fmt.Println("Stream/Events/Await: modelcalls=0; Start twice -> 1; Await timeout did not cancel.")
	fmt.Println("Late Events/Iter: root TaskDone + RunDone, then closed; no progress replay or rerun.")
}

func demonstrateIter(ctx context.Context) {
	var calls atomic.Int32
	a := newAgent(mock.New("offline-iter", func(*llm.Request) *llm.Response {
		calls.Add(1)
		return mock.Text("iterated once")
	}))
	run := a.Stream(ctx, "hello")
	defer run.Cancel()
	seq := run.Iter()
	check(calls.Load() == 0, "creating an iterator started the model")
	for _, err := range seq { // Consumption subscribes first, then starts once.
		check(err == nil, "Iter: %v", err)
	}
	result, err := run.Wait() // Convenience Start + Await; no new subscription.
	check(err == nil && result.Message.Text() == "iterated once", "Wait: %v", err)
	assertSuccessTail(iterEvents(run))
	check(calls.Load() == 1, "Iter/Wait model calls = %d", calls.Load())
	fmt.Println("Iter: modelcalls=0 before consumption, 1 after consumption and repeated Wait/Iter.")
}

func demonstrateQueue(ctx context.Context) {
	store := &recordBarrier{
		Memory:  checkpoint.NewMemory(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	releaseRecord := sync.OnceFunc(func() { close(store.release) })
	defer releaseRecord()
	var calls atomic.Int32
	a := newAgent(mock.New("offline-worker", func(*llm.Request) *llm.Response {
		calls.Add(1)
		return mock.Text("worker only")
	}), agent.WithCheckpointer(store))
	q := queue.NewMemQueue(2)
	defer q.Close()
	canceled, err := queue.EnqueueAgent(ctx, q, a, "cancel before pickup", agent.OnThread("canceled"))
	check(err == nil, "enqueue canceled job: %v", err)
	defer canceled.Cancel()
	normal, err := queue.EnqueueAgent(ctx, q, a, "worker job", agent.OnThread("normal"))
	check(err == nil, "enqueue normal job: %v", err)
	defer normal.Cancel()
	q.Close()

	normalWait := waitInBackground(normal.Wait)
	// Handle.Wait has no wait context. Observe its result channel under a
	// deadline instead: this bounds the no-worker observation, without sleep.
	expectWaiting(ctx, normalWait, "Handle.Wait without a worker")
	check(calls.Load() == 0, "Handle.Wait bypassed the worker: modelcalls=%d", calls.Load())
	fmt.Println("Queue without worker: Wait remains pending, modelcalls=0.")

	canceledRecord := waitInBackground(canceled.WaitRecorded)
	canceled.Cancel() // Prepared cancellation settles memory synchronously.
	canceledResult := receive(ctx, waitInBackground(canceled.Wait))
	check(errors.Is(canceledResult.err, context.Canceled), "prepared Cancel/Wait: %v", canceledResult.err)
	tail, stopTail := canceled.Events(bus.Lossless)
	defer stopTail()
	canceledTail := readAll(ctx, tail)
	check(len(canceledTail) == 1, "prepared cancellation tail length = %d", len(canceledTail))
	failed, ok := canceledTail[0].(core.RunFailed)
	check(ok && errors.Is(failed.Err, context.Canceled), "prepared cancellation tail = %T", canceledTail[0])
	expectWaiting(ctx, canceledRecord, "WaitRecorded after prepared cancellation without a worker")
	check(calls.Load() == 0, "prepared cancellation invoked a model")
	fmt.Println("Prepared Cancel: Wait returned cancellation; only RunFailed; WaitRecorded still pending.")

	normalRecord := waitInBackground(normal.WaitRecorded)
	workersDone := make(chan struct{})
	pool := queue.NewPool(q, 1)
	go func() {
		defer close(workersDone)
		pool.Run(ctx)
	}()
	defer func() {
		releaseRecord()
		normal.Cancel()
		receive(ctx, workersDone)
	}()

	receive(ctx, store.entered) // Worker is now blocked inside terminal Commit.
	execution := receive(ctx, normalWait)
	check(execution.err == nil && execution.result.Message.Text() == "worker only", "worker Wait: %v", execution.err)
	check(calls.Load() == 1, "worker modelcalls=%d; canceled job must not execute", calls.Load())
	// The Commit barrier proves recording cannot finish before release. This
	// checks a result channel, not an event-tail drain or a scheduling guess.
	select {
	case got := <-normalRecord:
		panic(fmt.Sprintf("WaitRecorded crossed blocked Commit: %v", got.err))
	default:
	}
	fmt.Println("Pool started: modelcalls=1; Wait returned while terminal Commit is blocked.")

	releaseRecord()
	recorded := receive(ctx, normalRecord)
	check(recorded.err == nil && recorded.result.Message.Text() == "worker only", "WaitRecorded: %v", recorded.err)
	canceledRecorded := receive(ctx, canceledRecord)
	check(errors.Is(canceledRecorded.err, context.Canceled), "lost cancellation: %v", canceledRecorded.err)
	check(errors.Is(canceledRecorded.err, agent.ErrNoCommittedBoundary), "invented a boundary: %v", canceledRecorded.err)
	receive(ctx, workersDone)
	fmt.Println("WaitRecorded confirmed; canceled job kept ErrNoCommittedBoundary; workers exited.")
}

// recordBarrier blocks only Completion commits. Ordinary model/checkpoint work
// proceeds, so execution can settle while the worker's record is still pending.
type recordBarrier struct {
	*checkpoint.Memory
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *recordBarrier) Commit(ctx context.Context, req checkpoint.CommitRequest) (checkpoint.CommitReceipt, error) {
	if req.Checkpoint != nil && req.Checkpoint.Completion != nil {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return checkpoint.CommitReceipt{}, ctx.Err()
		}
	}
	return s.Memory.Commit(ctx, req)
}

type outcome struct {
	result core.Result
	err    error
}

func waitInBackground(wait func() (core.Result, error)) <-chan outcome {
	done := make(chan outcome, 1)
	go func() {
		result, err := wait()
		done <- outcome{result, err}
	}()
	return done
}

func expectWaiting(ctx context.Context, done <-chan outcome, label string) {
	observation, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	select {
	case got := <-done:
		panic(fmt.Sprintf("%s returned early: %v", label, got.err))
	case <-observation.Done():
		check(ctx.Err() == nil, "%s: failure guard expired: %v", label, ctx.Err())
	}
}

func newAgent(model llm.Model, opts ...agent.Option) *agent.Agent {
	a, err := agent.New(append([]agent.Option{agent.WithModel(model)}, opts...)...)
	check(err == nil, "new agent: %v", err)
	return a
}

func receive[T any](ctx context.Context, ch <-chan T) T {
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		panic(fmt.Sprintf("waiting for channel: %v", ctx.Err()))
	}
}

func readAll(ctx context.Context, ch <-chan core.Event) []core.Event {
	var events []core.Event
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-ctx.Done():
			panic(fmt.Sprintf("reading events to closure: %v", ctx.Err()))
		}
	}
}

func iterEvents(run *agent.Run) []core.Event {
	var events []core.Event
	for ev, err := range run.Iter() {
		check(err == nil, "late Iter: %v", err)
		events = append(events, ev)
	}
	return events
}

func assertSuccessTail(events []core.Event) {
	check(len(events) == 2, "success tail length=%d, want root TaskDone + RunDone", len(events))
	task, ok := events[0].(core.TaskDone)
	check(ok && task.Status == "done", "first tail event = %#v", events[0])
	_, ok = events[1].(core.RunDone)
	check(ok, "second tail event = %T", events[1])
}

func check(ok bool, format string, args ...any) {
	if !ok {
		panic(fmt.Sprintf(format, args...))
	}
}
