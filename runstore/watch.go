package runstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiujuan/goagent/internal/watch"
)

// RunWatcher is the optional notification view over task records (ADR-0034 D7): a hint that a
// run this client is following reached a state it had not seen. It carries a summary, never a
// result body, and it is not how a run is waited on — WaitRun reads the record and is correct
// with no watcher running at all.
//
// P1 gives polling only: no durable event log, nothing that survives a restart, and a client
// that reconnects reads the current record first. The loop the notifications come from is the
// same one the checkpoint watch uses, so "already delivered" means the same thing in both.
type RunWatcher interface {
	// WatchRuns returns a channel of notifications for the runs named in the request, closed
	// when ctx ends.
	WatchRuns(ctx context.Context, req RunWatchRequest) (<-chan RunNotification, error)
}

// RunWatchRequest names the runs to follow. A run id may be watched before its record exists —
// a client that submitted work through a queue knows the id first — and the record appearing
// is itself a change to report.
type RunWatchRequest struct {
	RunIDs []string
}

// RunNotification is one observed state of one run, as a list row.
type RunNotification struct {
	RunID   string     `json:"run_id"`
	Summary RunSummary `json:"summary"`

	// Coalesced counts the further states this hint also stands for, which is what a consumer
	// that is not reading gets: the newest state rather than a backlog it can never clear.
	Coalesced int `json:"coalesced,omitempty"`
}

// RunWatchStats says what a poller has done. A watch that never notifies is only
// distinguishable from a watch that cannot read through these numbers.
type RunWatchStats struct {
	Looks         int
	Passes        int
	Outages       int
	LastOutage    error
	ActiveWatches int
}

// RunPoller answers a RunWatcher by reading each watched record on an interval. It drives
// nothing: a poll reads, and never claims, renews or repairs.
type RunPoller struct {
	store    Store
	interval time.Duration
	buffer   int

	// newTicker is the wait between two looks. Production uses a timer; WithRunWake lets a
	// caller be woken by something else, and a test steps the loop by hand.
	newTicker func(time.Duration) watch.Ticker

	mu      sync.Mutex
	looks   int
	passes  int
	outages int
	lastErr error
	active  atomic.Int64
}

var _ RunWatcher = (*RunPoller)(nil)

// RunWatchOption tunes a RunPoller.
type RunWatchOption func(*RunPoller)

// WithRunPollInterval sets how long the poller waits between two looks. It must be positive.
func WithRunPollInterval(d time.Duration) RunWatchOption {
	return func(p *RunPoller) { p.interval = d }
}

// WithRunNotificationBuffer sets how many notifications may wait unread before they start
// merging. It must be positive.
func WithRunNotificationBuffer(n int) RunWatchOption {
	return func(p *RunPoller) { p.buffer = n }
}

// WithRunWake makes the poller look again for every value on the channel instead of waiting on
// its own timer. The channel never needs closing: the watch ends with its context.
func WithRunWake(ch <-chan time.Time) RunWatchOption {
	return func(p *RunPoller) {
		p.newTicker = func(time.Duration) watch.Ticker { return wakeTicker{ch: ch} }
	}
}

type wakeTicker struct{ ch <-chan time.Time }

func (t wakeTicker) Chan() <-chan time.Time { return t.ch }
func (t wakeTicker) Stop()                  {}

// NewRunPoller binds a watcher to one task store.
func NewRunPoller(store Store, opts ...RunWatchOption) (*RunPoller, error) {
	if store == nil {
		return nil, fmt.Errorf("runstore: poller needs a run store")
	}
	p := &RunPoller{
		store:    store,
		interval: watch.DefaultInterval,
		buffer:   watch.DefaultBuffer,
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.interval <= 0 {
		return nil, fmt.Errorf("runstore: poll interval must be positive, got %v", p.interval)
	}
	if p.buffer <= 0 {
		return nil, fmt.Errorf("runstore: notification buffer must be positive, got %d", p.buffer)
	}
	if p.newTicker == nil {
		p.newTicker = func(d time.Duration) watch.Ticker { return watch.RuntimeTicker{Ticker: time.NewTicker(d)} }
	}
	return p, nil
}

// WatchRuns starts one walk over the requested runs.
func (p *RunPoller) WatchRuns(ctx context.Context, req RunWatchRequest) (<-chan RunNotification, error) {
	if ctx == nil {
		return nil, fmt.Errorf("runstore: watch needs a context")
	}
	if len(req.RunIDs) == 0 {
		return nil, fmt.Errorf("runstore: watch needs at least one run id: %w", ErrInvalidQuery)
	}
	for _, runID := range req.RunIDs {
		if err := CheckRunID(runID); err != nil {
			return nil, err
		}
	}

	loop := &watch.Loop[RunSummary]{
		Keys:      append([]string(nil), req.RunIDs...),
		Read:      p.look,
		Interval:  p.interval,
		NewTicker: p.newTicker,
		OnError:   p.noteOutage,
		OnPass:    p.notePass,
	}
	out := make(chan RunNotification, p.buffer)
	p.active.Add(1)
	go func() {
		defer p.active.Add(-1)
		defer close(out)
		loop.Run(ctx, p.deliver(out))
	}()
	return out, nil
}

// deliver is the one place a notification leaves the loop. It never blocks on the caller: a
// full buffer refuses, and the loop keeps the newest state waiting.
func (p *RunPoller) deliver(out chan<- RunNotification) func(watch.Notification[RunSummary]) bool {
	return func(note watch.Notification[RunSummary]) bool {
		select {
		case out <- RunNotification{RunID: note.Key, Summary: note.Value, Coalesced: note.Coalesced}:
			return true
		default:
			return false
		}
	}
}

// look reads one watched record. A record that does not exist yet is not an outage: it is a run
// that has not been recorded, and the record appearing is the change to report.
func (p *RunPoller) look(ctx context.Context, runID string) (watch.Reading[RunSummary], error) {
	p.noteLook()
	record, err := p.store.Get(ctx, runID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return watch.Reading[RunSummary]{Found: false}, nil
		}
		return watch.Reading[RunSummary]{}, err
	}
	return watch.Reading[RunSummary]{
		Value:   runSummaryOf(record),
		Version: record.Version,
		Found:   true,
	}, nil
}

// Stats reports what the poller has seen. ActiveWatches is the leak evidence: it must return to
// zero once every watch's context has ended.
func (p *RunPoller) Stats() RunWatchStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return RunWatchStats{
		Looks:         p.looks,
		Passes:        p.passes,
		Outages:       p.outages,
		LastOutage:    p.lastErr,
		ActiveWatches: int(p.active.Load()),
	}
}

func (p *RunPoller) notePass() {
	p.mu.Lock()
	p.passes++
	p.mu.Unlock()
}

func (p *RunPoller) noteLook() {
	p.mu.Lock()
	p.looks++
	p.mu.Unlock()
}

func (p *RunPoller) noteOutage(err error) {
	p.mu.Lock()
	p.outages++
	p.lastErr = err
	p.mu.Unlock()
}
