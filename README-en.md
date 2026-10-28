# goagent

A Go agent framework with typed model/tool loops, streaming events, checkpoints, approval and resume, workflows, and background queues. Requires Go 1.25 or newer. See the [Chinese README](README.md) for the full feature and example catalog.

## Offline quick start

Run the existing mock example without provider credentials:

```sh
go run ./examples/quickstart
```

Build an Agent with `agent.New` and `agent.WithModel`. `Agent.Run` starts execution and returns the answer; `Agent.Stream` creates a lazy Run. Production providers and tools use the same execution path as the mock examples.

## Starting, waiting, and observing a Run

For the lifecycle decisions and migration steps, see [ADR-0035](docs/adr/ADR-0035-lazy-run-lifecycle.md) and the [migration note](docs/changes/2026-10-28-lazy-run-lifecycle.md).

An ordinary Stream freezes input when it creates the handle. It remains lazy: checkpoint history is restored only after execution starts and admission is acquired. Resume has its own preparation path and can acquire admission before returning its handle.

| API | Starts execution | Responsibility |
| --- | --- | --- |
| `run.Start()` | Once | Idempotent and nonblocking; no subscription or wait |
| `run.Await(waitCtx)` | No | Waits for in-memory settlement; each caller gets an independent result copy |
| `run.Wait()` | Yes | Start + Await(context.Background()), without an event subscription |
| Consuming `run.Iter()` | Yes | Subscribes before Start; creating the iterator alone does not start it |
| `run.Events(mode)` | No | Subscribes only; the caller owns unsubscribe |
| `handle.Wait()` | No | Awaits execution settlement; a worker must start ordinary queued work |
| `handle.WaitRecorded()` | No | Awaits the worker's settlement and terminal-record attempt |

Keep the Run if you may stop reading early:

```go
run := a.Stream(ctx, "hello")
defer run.Cancel()
for ev, err := range run.Iter() {
    if err != nil {
        return err
    }
    _ = ev
    // A break unsubscribes this iterator. Execution continues until completion
    // or cancellation; call run.Cancel() if you intend to stop the work too.
}
_, err := run.Await(ctx)
return err
```

For raw events, subscribe before starting and keep consuming:

```go
run := a.Stream(ctx, "hello")
events, unsubscribe := run.Events(bus.Lossless)
defer unsubscribe() // Also required when leaving the receive loop early.
run.Start()
for ev := range events {
    _ = ev
}
result, err := run.Await(ctx)
_ = result
_ = err
```

Canceling or timing out `Await(waitCtx)` only stops that waiter: it neither starts nor cancels execution. To stop execution and wait for it, call `run.Cancel()` followed by `run.Await(context.Background())`. A prepared Cancel synchronously settles in memory and emits only RunFailed, with no RunStarted or root TaskDone. Once started, Cancel only requests cancellation; external models, tools, hooks, and stores must cooperate with context cancellation. If settlement and the wait context are both observable, Await prefers the settled result.

Iter cleans up its subscription on break. Events callers should immediately `defer unsubscribe()`; stopping reads without unsubscribing can retain a goroutine delivering the tail. Ordinary Lossless progress still applies backpressure. Child TaskDone remains an ordinary, cancelable progress event.

A late observer of a live Run receives future events and its terminal tail, without earlier progress. A sealed Run provides only a finite tail: root TaskDone followed by one RunDone, RunFailed, or Interrupted; failures before a root task starts provide only RunFailed. The channel closes after the tail. Repeated Iter/Events calls do not rerun the model or replay progress.

Wait returning does not guarantee that observers have read the tail, or even that it has entered their channel. Read until channel closure when you need the complete stream. Do not infer completion with a nonblocking select/default drain after Wait.

RecordApproval's durable decision receipt remains authoritative. Recording an approval on a sealed Run does not reopen its event stream. Use the receipt to confirm persistence and Resume to continue execution.

## Background queue ownership and recording

Start a Pool/worker before expecting ordinary queued work to finish. Handle.Wait is now a pure wait, so code that previously relied on it to start a lazy Run must migrate. Use Agent.Run for direct execution. Handle has no Start or Await method; stop a job with Handle.Cancel.

```go
q := queue.NewMemQueue(1)
pool := queue.NewPool(q, 1)
workersDone := make(chan struct{})
go func() {
    defer close(workersDone)
    pool.Run(ctx)
}()
h, err := queue.EnqueueAgent(ctx, q, a, "hello")
if err != nil {
    q.Close()
    <-workersDone
    return err
}
q.Close()
_, runErr := h.Wait()           // In-memory execution settlement only.
_, recordErr := h.WaitRecorded() // Worker has attempted the terminal record.
<-workersDone                   // The Pool has also exited.
return errors.Join(runErr, recordErr)
```

Submission context cancellation only governs submission. After acceptance the queue owns the execution context; canceling the Pool context stops fetching and waits for in-flight jobs. Handle.Cancel stops the selected job.

A prepared job canceled before pickup can return from Wait with a cancellation outcome even without a worker. WaitRecorded still waits for a worker's recording attempt. A later worker must not execute the model for that canceled job; without a committed boundary from that run, recording retains `agent.ErrNoCommittedBoundary` instead of inventing a terminal checkpoint.

WaitRecorded returns an independent result and `errors.Join(runErr, recordErr)`. A completed run with nil error confirms recording; failures can contain both execution and record errors. Paused runs skip the terminal record. RunStatus is a read of checkpoint storage facts and does not assert that no new run is active. Execution settlement, event delivery, and durable recording are separate milestones.

## Lifecycle observability

Lifecycle events add creation, start request, actual admission, first valid model response, and settlement timestamps to the existing context span. They do not create a new span or change model/tool span parenting.

Lifecycle metrics use the global `otel.GetMeterProvider()`: configure the application with `otel.SetMeterProvider(...)`. An independently configured `obs/otel.WithMeterProvider` only affects its middleware and does not configure these lifecycle metrics.

| Histogram | Unit | Interval |
| --- | --- | --- |
| `agent.run.creation_to_start` | seconds | Creation to first start request |
| `agent.run.start_to_admission` | seconds | First start request to actual admission |
| `agent.run.admission_to_first_response` | seconds | Admission to first valid model response |
| `agent.run.start_to_settlement` | seconds | First start request to settlement |

Missing stages and negative intervals are omitted. Resume may acquire admission before creation/start, so its true admission time is preserved and a negative start-to-admission interval is skipped. The first response is the first non-nil, error-free response, whether partial or final; it is not a first-token measurement. Metrics have no runID, threadID, prompt, or tool-argument labels; run identity belongs only in span event attributes.

## Examples and checks

| Example | Command | Purpose |
| --- | --- | --- |
| [quickstart](examples/quickstart/main.go) | `go run ./examples/quickstart` | Offline mock tool loop |
| [lazy-run-lifecycle](examples/lazy-run-lifecycle/main.go) | `go run ./examples/lazy-run-lifecycle` after T10 | Offline assertions for lazy creation, one start, finite tail, waiter timeout, and worker ownership |
| [background](examples/background/main.go) | `go run ./examples/background` after T10 | Offline Pool, Wait, WaitRecorded, and worker exit |
| [background-status](examples/background-status/main.go) | `go run ./examples/background-status` | Terminal records and RunStatus |
| [quickstart-stream](examples/quickstart-stream/main.go) | `go run ./examples/quickstart-stream` | Real-provider template; requires AGNES_API_KEY and may make paid requests |

Compile all examples without running providers:

```sh
go test ./examples/... -run '^$'
git diff --check
```

Compiling examples does not validate lifecycle behavior. Run the offline lifecycle example after T10 is complete; observability and concurrency/race validation remain part of the primary implementation.
