package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/runstore"
)

// Managed runs travel through the queue as a versioned Type/Payload instead of a closure:
// the submitting process is often gone before a worker picks the job up, so nothing about
// the agent, its model, its credentials or a file backend may be carried in the job. A
// worker resolves all of that from AgentKey (ADR-0034 D5).
//
// The task record is created before the job is dispatched, and the RunID comes back even
// when the dispatch fails — that identity is what a reconnecting caller uses to look the
// run up later through RunClient.

// ManagedRunJobType is the Job.Type every managed run is delivered under. One Handler for
// it serves every agent, because the payload names the agent it belongs to.
const ManagedRunJobType = "agent.managed_run"

// ManagedRunPayloadVersion is the payload shape this build sends and accepts. A worker
// refuses any other version rather than guessing at fields it does not know: executing a
// misread payload could run the wrong prompt under someone else's identity.
const ManagedRunPayloadVersion = 1

// Submission and payload errors.
var (
	// ErrInvalidManagedSubmission means the request or payload cannot describe a run:
	// no agent key, no input, or an id that cannot name a run record or a thread.
	ErrInvalidManagedSubmission = errors.New("queue: invalid managed run submission")
	// ErrUnknownPayloadVersion means a payload carries a version this build does not
	// speak. The job is poison, not retryable: a queue should move it to its dead
	// letter handling rather than redeliver it.
	ErrUnknownPayloadVersion = errors.New("queue: unknown managed run payload version")
	// ErrDispatchRefused is returned by a Queue that can prove it did not accept the
	// job. Only then may a run be marked failed for not having been dispatched: an
	// error that leaves the outcome open must not end a run that may actually be
	// running (ADR-0034 D5).
	ErrDispatchRefused = errors.New("queue: dispatch refused")
	// ErrDispatchNotQueued means a re-dispatch was refused because the record is no
	// longer queued: a worker holds it, or the run already ended.
	ErrDispatchNotQueued = errors.New("queue: run is not queued for re-dispatch")
)

// ManagedRunRequest is what a caller supplies to submit one managed run. RunID, ThreadID,
// TurnID and JobID may be left empty for the queue to assign; supplying them is how a caller
// retries the very same run or keeps an id it already published.
//
// Input is the user text. Message takes precedence when set, so a run can start from a
// multi-part message. Nothing here is a closure, an option or a store handle: RunOption
// values cannot cross a process, so per-run choices (files, approvals) belong to the agent
// the resolver returns, not to the request.
type ManagedRunRequest struct {
	AgentKey string
	RunID    string
	ThreadID string
	TurnID   string
	JobID    string
	Input    string
	Message  core.Message
}

// ManagedRunPayload is the serialized job payload: the run's identity, the agent key that
// resolves its configuration, and the input. It is exported because a worker's dead-letter
// inspection and an application's own queue both need to read it.
type ManagedRunPayload struct {
	Version  int           `json:"version"`
	AgentKey string        `json:"agent_key"`
	RunID    string        `json:"run_id"`
	ThreadID string        `json:"thread_id"`
	TurnID   string        `json:"turn_id,omitempty"`
	Input    string        `json:"input,omitempty"`
	Message  *core.Message `json:"message,omitempty"`
}

// Validate checks the payload against the same rules the run record and the agent use, so
// a payload that cannot identify a run is refused before any worker touches a record.
func (p ManagedRunPayload) Validate() error {
	if p.AgentKey == "" {
		return fmt.Errorf("queue: managed run payload has no agent key: %w", ErrInvalidManagedSubmission)
	}
	if p.Input == "" && p.Message == nil {
		return fmt.Errorf("queue: managed run %q has no input: %w", p.RunID, ErrInvalidManagedSubmission)
	}
	spec := agent.ManagedRunSpec{RunID: p.RunID, ThreadID: p.ThreadID, TurnID: p.TurnID}
	if err := spec.Validate(); err != nil {
		return fmt.Errorf("queue: managed run payload: %w: %w", ErrInvalidManagedSubmission, err)
	}
	if err := runstore.CheckRunID(p.RunID); err != nil {
		return fmt.Errorf("queue: managed run payload: %w: %w", ErrInvalidManagedSubmission, err)
	}
	return nil
}

// Submission is one run's delivery receipt. RunID and JobID are exported so a caller can
// report, look up and cancel the run; the job itself stays private, because re-dispatch
// must send exactly what was sent the first time rather than a reconstruction that might
// differ.
type Submission struct {
	RunID string
	JobID string
	job   Job
}

// EnqueueManagedAgent records the run as queued, then dispatches it. It returns the run's
// identity even when the dispatch fails, and the dispatch error on its own.
//
// A Queue that proves it refused the job (an error wrapping ErrDispatchRefused) ends the
// run as failed/enqueue_failed, because nothing will ever run it. Any other enqueue error
// leaves the record queued with Error=dispatch_unknown and Retryable: the job may or may
// not have been accepted, and asserting either way would either strand a run that is
// actually running or run the same task twice. Use RetryDispatch with this Submission to
// send it again, or a new RunID for a genuinely new attempt.
//
// ctx owns submission only: cancelling it after the queue accepted the job does not stop
// the run, and a cancelled ctx before any of this happens creates neither record nor job.
// Creating the same RunID with the same content again is the store's own idempotent replay;
// a different request under one RunID is refused with runstore.ErrConflict.
func EnqueueManagedAgent(ctx context.Context, q Queue, store runstore.Store, req ManagedRunRequest) (Submission, error) {
	if q == nil {
		return Submission{}, fmt.Errorf("queue: managed run submission needs a queue")
	}
	if store == nil {
		return Submission{}, fmt.Errorf("queue: managed run submission needs a run store")
	}

	payload, job, err := managedRunJob(req)
	if err != nil {
		return Submission{}, err
	}
	sub := Submission{RunID: payload.RunID, JobID: job.ID, job: job}
	if err := ctx.Err(); err != nil {
		return sub, err
	}

	record := runstore.RunRecord{
		RunID:    payload.RunID,
		ThreadID: payload.ThreadID,
		TurnID:   payload.TurnID,
		JobID:    job.ID,
		Status:   runstore.StatusQueued,
	}
	if _, err := store.Create(ctx, record); err != nil {
		return Submission{}, err
	}

	if err := q.Enqueue(ctx, job); err != nil {
		return sub, recordDispatchOutcome(ctx, store, sub.RunID, err)
	}
	return sub, nil
}

// RetryDispatch sends the same job again — same RunID, same JobID, same payload bytes — and
// creates no record. A queued record can be re-dispatched as often as needed; a record a
// worker already holds, or that already ended, is refused with ErrDispatchNotQueued and
// reported back unchanged.
//
// The record is left as it stands on a successful send, including any dispatch_unknown
// error from the attempt that failed: the worker's own terminal write replaces it.
func RetryDispatch(ctx context.Context, q Queue, store runstore.Store, sub Submission) (runstore.RunRecord, error) {
	if q == nil {
		return runstore.RunRecord{}, fmt.Errorf("queue: managed run re-dispatch needs a queue")
	}
	if store == nil {
		return runstore.RunRecord{}, fmt.Errorf("queue: managed run re-dispatch needs a run store")
	}
	if sub.job.Type != ManagedRunJobType || len(sub.job.Payload) == 0 {
		return runstore.RunRecord{}, fmt.Errorf("queue: submission for run %q carries no managed run job to re-dispatch: %w",
			sub.RunID, ErrInvalidManagedSubmission)
	}

	current, err := store.Get(ctx, sub.RunID)
	if err != nil {
		return runstore.RunRecord{}, err
	}
	if current.Status != runstore.StatusQueued || current.WorkerID != "" {
		return current, fmt.Errorf("queue: run %q is %s, only a queued run can be re-dispatched: %w",
			current.RunID, current.Status, ErrDispatchNotQueued)
	}
	if err := q.Enqueue(ctx, sub.job); err != nil {
		return current, recordDispatchOutcome(ctx, store, sub.RunID, err)
	}
	return current, nil
}

// AgentResolver gives a worker the agent configuration for one payload's AgentKey. It is
// the application's: an external file backend, model credentials and middleware all get
// rebuilt here, which is why none of them travel in the job.
type AgentResolver func(ctx context.Context, agentKey string) (*agent.Agent, error)

// ManagedRunHandler is the Registry Handler for ManagedRunJobType: it decodes and validates
// the payload, resolves the agent, and runs one ManagedRunWorker over the record the payload
// identifies.
//
// A payload of an unknown version, or one that cannot describe a run, is reported before any
// record is read: nothing may be claimed or executed on it. An error from the run comes back
// unchanged, so a queue that acks only on success will redeliver it — and the redelivery is
// the harmless case the worker's claim already handles.
func ManagedRunHandler(store runstore.Store, workerID string, resolve AgentResolver, opts ...WorkerOption) (Handler, error) {
	if store == nil {
		return nil, fmt.Errorf("queue: managed run handler needs a run store")
	}
	if workerID == "" {
		return nil, fmt.Errorf("queue: managed run handler needs a worker id")
	}
	if resolve == nil {
		return nil, fmt.Errorf("queue: managed run handler needs an agent resolver")
	}
	return func(ctx context.Context, payload []byte) error {
		p, err := decodeManagedRunPayload(payload)
		if err != nil {
			return err
		}
		a, err := resolve(ctx, p.AgentKey)
		if err != nil {
			return fmt.Errorf("queue: managed run %q resolves agent %q: %w", p.RunID, p.AgentKey, err)
		}
		w, err := NewManagedRunWorker(a, store, workerID, opts...)
		if err != nil {
			return err
		}
		_, err = w.Execute(ctx, p.RunID, p.Input, managedRunRunOptions(p)...)
		return err
	}, nil
}

// decodeManagedRunPayload reads and checks one payload. Version is judged first and alone:
// fields of a shape this build does not know must not be interpreted.
func decodeManagedRunPayload(data []byte) (ManagedRunPayload, error) {
	var p ManagedRunPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return p, fmt.Errorf("queue: managed run payload: %w", err)
	}
	if p.Version != ManagedRunPayloadVersion {
		return p, fmt.Errorf("queue: managed run payload version %d, this build speaks %d: %w",
			p.Version, ManagedRunPayloadVersion, ErrUnknownPayloadVersion)
	}
	if err := p.Validate(); err != nil {
		return p, err
	}
	return p, nil
}

// managedRunJob fills in the ids a request leaves out, then encodes the payload and the job
// that carries it. The same function is used for a first send and a re-dispatch, so the two
// cannot drift apart.
func managedRunJob(req ManagedRunRequest) (ManagedRunPayload, Job, error) {
	runID := req.RunID
	if runID == "" {
		runID = core.NewID("run")
	}
	threadID := req.ThreadID
	if threadID == "" {
		threadID = core.NewID("thread")
	}
	jobID := req.JobID
	if jobID == "" {
		jobID = core.NewID("job")
	}

	p := ManagedRunPayload{
		Version:  ManagedRunPayloadVersion,
		AgentKey: req.AgentKey,
		RunID:    runID,
		ThreadID: threadID,
		TurnID:   req.TurnID,
		Input:    req.Input,
	}
	if req.Message.Role != "" {
		message := req.Message
		p.Message = &message
		p.Input = ""
	}
	if err := p.Validate(); err != nil {
		return ManagedRunPayload{}, Job{}, err
	}

	data, err := json.Marshal(p)
	if err != nil {
		return ManagedRunPayload{}, Job{}, fmt.Errorf("queue: managed run payload: %w", err)
	}
	return p, Job{ID: jobID, Key: req.AgentKey, Type: ManagedRunJobType, Payload: data}, nil
}

// managedRunRunOptions turns a payload's input into run options. A message arrives as the
// run's own message; text needs none, since Execute hands the input to the agent directly.
func managedRunRunOptions(p ManagedRunPayload) []agent.RunOption {
	if p.Message == nil {
		return nil
	}
	return []agent.RunOption{agent.WithMessage(*p.Message)}
}

// recordDispatchOutcome reports what is known about a failed dispatch on the record, and
// always returns the dispatch error itself: the caller's problem is the delivery, not this
// bookkeeping write.
//
// A record a worker already holds is left alone — that is proof the job did get through,
// whatever the enqueue call reported. A bookkeeping write that fails is joined to the
// dispatch error rather than swallowed.
func recordDispatchOutcome(ctx context.Context, store runstore.Store, runID string, dispatchErr error) error {
	refused := errors.Is(dispatchErr, ErrDispatchRefused)
	current, err := store.Get(ctx, runID)
	if err != nil {
		return errors.Join(dispatchErr, err)
	}
	if current.Status != runstore.StatusQueued || current.WorkerID != "" {
		return dispatchErr
	}

	next := current
	next.Error = &runstore.RunError{
		Code:      runstore.ErrCodeDispatchUnknown,
		Message:   dispatchErr.Error(),
		Retryable: true,
	}
	if refused {
		next.Status = runstore.StatusFailed
		next.FinishReason = runstore.FinishReasonFailed
		next.Result = nil
		next.Error = &runstore.RunError{
			Code:      runstore.ErrCodeEnqueueFailed,
			Message:   dispatchErr.Error(),
			Retryable: false,
		}
	}
	if _, err := store.CompareAndSwap(ctx, runID, current.Version, next); err != nil {
		return errors.Join(dispatchErr, err)
	}
	return dispatchErr
}
