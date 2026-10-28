// Package runobs records run lifecycle timing on the caller's existing span.
// Lifecycle metrics use the global MeterProvider, independently of any
// MeterProvider configured on the obs/otel middleware.
package runobs

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Snapshot contains only copies of observed times. Zero means unobserved.
type Snapshot struct {
	CreatedAt            time.Time
	StartRequestedAt     time.Time
	AdmittedAt           time.Time
	FirstModelResponseAt time.Time
	SettledAt            time.Time
}

type phase uint8

const (
	created phase = iota
	startRequested
	admitted
	firstModelResponse
	settled
	phaseCount
)

var phaseNames = [...]string{
	"created", "start_requested", "admitted", "first_model_response", "settled",
}

var intervals = [...]struct {
	name       string
	start, end phase
}{
	{"agent.run.creation_to_start", created, startRequested},
	{"agent.run.start_to_admission", startRequested, admitted},
	{"agent.run.admission_to_first_response", admitted, firstModelResponse},
	{"agent.run.start_to_settlement", startRequested, settled},
}

// Recorder records each phase and each observed, nonnegative interval once.
// Its methods, including Snapshot, are safe for concurrent use and nil receivers.
type Recorder struct {
	mu       sync.Mutex
	times    [phaseCount]time.Time
	recorded [len(intervals)]bool

	ctx      context.Context
	span     trace.Span
	runID    string
	threadID string
	metrics  [len(intervals)]metric.Float64Histogram
}

// New records Created on the span already present in ctx; it creates no spans.
func New(ctx context.Context, runID, threadID string) *Recorder {
	at := time.Now()
	if ctx == nil {
		ctx = context.Background()
	}
	r := &Recorder{
		ctx: ctx, span: trace.SpanFromContext(ctx), runID: runID, threadID: threadID,
	}
	meter := otel.GetMeterProvider().Meter("github.com/jiujuan/goagent/internal/runobs")
	for i, interval := range intervals {
		histogram, err := meter.Float64Histogram(interval.name, metric.WithUnit("s"))
		if err != nil {
			otel.Handle(err)
			continue
		}
		r.metrics[i] = histogram
	}
	r.record(created, at)
	return r
}

// StartRequested records the first request to start execution.
func (r *Recorder) StartRequested() { r.record(startRequested, time.Now()) }

// Admitted records the actual admission time, which may precede New or StartRequested.
// A zero time is unobserved and ignored.
func (r *Recorder) Admitted(at time.Time) { r.record(admitted, at) }

// FirstModelResponse records the first successful model response, including partials.
func (r *Recorder) FirstModelResponse() { r.record(firstModelResponse, time.Now()) }

// Settled records the first settlement time, whether or not execution started.
func (r *Recorder) Settled() { r.record(settled, time.Now()) }

// Snapshot copies the observed times without exporting any telemetry.
func (r *Recorder) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.Lock()
	times := r.times
	r.mu.Unlock()
	return Snapshot{
		CreatedAt:            times[created],
		StartRequestedAt:     times[startRequested],
		AdmittedAt:           times[admitted],
		FirstModelResponseAt: times[firstModelResponse],
		SettledAt:            times[settled],
	}
}

func (r *Recorder) record(p phase, at time.Time) {
	if r == nil || at.IsZero() {
		return
	}
	var emit [len(intervals)]bool
	var seconds [len(intervals)]float64
	r.mu.Lock()
	if !r.times[p].IsZero() {
		r.mu.Unlock()
		return
	}
	r.times[p] = at
	// Claim all newly complete intervals under the lock. Either endpoint can
	// arrive last, and a later phase must never re-emit an earlier interval.
	for i, interval := range intervals {
		start, end := r.times[interval.start], r.times[interval.end]
		if r.recorded[i] || start.IsZero() || end.IsZero() || end.Before(start) {
			continue
		}
		r.recorded[i] = true
		emit[i] = true
		seconds[i] = end.Sub(start).Seconds()
	}
	r.mu.Unlock()

	// SDK callbacks and exporters must never run while holding the state lock.
	if r.span != nil {
		name := phaseNames[p]
		r.span.AddEvent("agent.run."+name,
			trace.WithTimestamp(at),
			trace.WithAttributes(
				attribute.String("run_id", r.runID),
				attribute.String("thread_id", r.threadID),
				attribute.String(name+"_at", at.Format(time.RFC3339Nano)),
			),
		)
	}
	for i, histogram := range r.metrics {
		if emit[i] && histogram != nil {
			histogram.Record(r.ctx, seconds[i])
		}
	}
}
