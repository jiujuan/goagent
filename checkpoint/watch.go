package checkpoint

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/internal/watch"
)

// CheckpointWatcher is the optional notification view over a checkpointer (ADR-0034 D7). It
// says "this thread has a snapshot you have not seen"; it does not carry the snapshot's state,
// promise delivery, or replace a read.
//
// P1 gives polling only. There is no durable event log, a process that restarts loses every
// walk in flight, and a client that reconnects reads the current state first and then watches
// for what comes after — which is why the first pass of a watch is silent. WaitRun stays
// correct with no watcher at all, and an in-process bus is not a cross-process subscription.
type CheckpointWatcher interface {
	// WatchCheckpoints returns a channel of notifications for the threads named in the
	// request. The channel is closed when ctx ends.
	WatchCheckpoints(ctx context.Context, req CheckpointWatchRequest) (<-chan CheckpointNotification, error)
}

// CheckpointWatchRequest names the threads to follow. A watch never enumerates the store on
// the caller's behalf: discovering threads is what ListSessions is for, and a client that
// wants a thread it did not know about starts another watch.
type CheckpointWatchRequest struct {
	ThreadIDs []string
}

// CheckpointNotification is a hint that one thread's head moved, carrying the summary of the
// snapshot that is now most visible.
type CheckpointNotification struct {
	ThreadID string             `json:"thread_id"`
	Metadata CheckpointMetadata `json:"metadata"`

	// Coalesced counts the further moves this hint also stands for. It is zero for a consumer
	// that keeps up: a slow reader gets the newest revision rather than a queue of everything
	// it missed, which is the right shape for a hint and the only shape that cannot fall
	// behind forever.
	Coalesced int `json:"coalesced,omitempty"`
}

// WatchStats says what a poller has done since it was built. A watch that never notifies is
// only distinguishable from a watch that cannot read through these numbers.
type WatchStats struct {
	Looks         int
	Passes        int
	Outages       int
	LastOutage    error
	ActiveWatches int
}

// CheckpointPoller answers a CheckpointWatcher by reading each watched thread's head on an
// interval. It works against any store that reports checkpoint metadata, in memory or on
// disk, and it drives nothing: a poll is a read, never a claim, a renewal or a repair.
type CheckpointPoller struct {
	store    MetadataReader
	interval time.Duration
	buffer   int

	// newTicker is the wait between two looks. Production uses a timer; WithCheckpointWake
	// lets a caller be woken by something else, and a test steps the loop by hand.
	newTicker func(time.Duration) watch.Ticker

	mu      sync.Mutex
	looks   int
	passes  int
	outages int
	lastErr error
	active  atomic.Int64
}

var _ CheckpointWatcher = (*CheckpointPoller)(nil)

// CheckpointWatchOption tunes a CheckpointPoller.
type CheckpointWatchOption func(*CheckpointPoller)

// WithCheckpointPollInterval sets how long the poller waits between two looks. It must be
// positive.
func WithCheckpointPollInterval(d time.Duration) CheckpointWatchOption {
	return func(p *CheckpointPoller) { p.interval = d }
}

// WithCheckpointNotificationBuffer sets how many notifications may wait unread before they
// start merging. It must be positive.
func WithCheckpointNotificationBuffer(n int) CheckpointWatchOption {
	return func(p *CheckpointPoller) { p.buffer = n }
}

// WithCheckpointWake makes the poller look again for every value on the channel instead of
// waiting on its own timer. It is how a caller that already has a signal — a queue delivery, a
// test that wants deterministic steps — drives the watch. The channel never needs closing: the
// watch ends with its context.
func WithCheckpointWake(ch <-chan time.Time) CheckpointWatchOption {
	return func(p *CheckpointPoller) {
		p.newTicker = func(time.Duration) watch.Ticker { return wakeTicker{ch: ch} }
	}
}

type wakeTicker struct{ ch <-chan time.Time }

func (t wakeTicker) Chan() <-chan time.Time { return t.ch }
func (t wakeTicker) Stop()                  {}

// NewCheckpointPoller binds a watcher to one checkpointer. A store that does not report
// checkpoint metadata is refused here rather than at the first poll: a caller that configured
// a full checkpointer and gets no notifications would otherwise have no way to tell an
// unavailable view from a thread that never moved.
func NewCheckpointPoller(store Checkpointer, opts ...CheckpointWatchOption) (*CheckpointPoller, error) {
	if store == nil {
		return nil, fmt.Errorf("checkpoint: poller needs a checkpointer")
	}
	reader, ok := store.(MetadataReader)
	if !ok {
		return nil, fmt.Errorf("checkpoint: poller over %T: %w", store, ErrCapabilityUnavailable)
	}
	p := &CheckpointPoller{
		store:    reader,
		interval: watch.DefaultInterval,
		buffer:   watch.DefaultBuffer,
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.interval <= 0 {
		return nil, fmt.Errorf("checkpoint: poll interval must be positive, got %v", p.interval)
	}
	if p.buffer <= 0 {
		return nil, fmt.Errorf("checkpoint: notification buffer must be positive, got %d", p.buffer)
	}
	if p.newTicker == nil {
		p.newTicker = func(d time.Duration) watch.Ticker { return watch.RuntimeTicker{Ticker: time.NewTicker(d)} }
	}
	return p, nil
}

// WatchCheckpoints starts one walk over the requested threads.
func (p *CheckpointPoller) WatchCheckpoints(ctx context.Context, req CheckpointWatchRequest) (<-chan CheckpointNotification, error) {
	if ctx == nil {
		return nil, fmt.Errorf("checkpoint: watch needs a context")
	}
	if len(req.ThreadIDs) == 0 {
		return nil, fmt.Errorf("checkpoint: watch needs at least one thread: %w", ErrInvalidQuery)
	}
	for _, threadID := range req.ThreadIDs {
		if err := core.CheckThreadID(threadID); err != nil {
			return nil, err
		}
	}

	loop := &watch.Loop[CheckpointMetadata]{
		Keys:      append([]string(nil), req.ThreadIDs...),
		Read:      p.look,
		Interval:  p.interval,
		NewTicker: p.newTicker,
		OnError:   p.noteOutage,
		OnPass:    p.notePass,
	}
	out := make(chan CheckpointNotification, p.buffer)
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
func (p *CheckpointPoller) deliver(out chan<- CheckpointNotification) func(watch.Notification[CheckpointMetadata]) bool {
	return func(note watch.Notification[CheckpointMetadata]) bool {
		select {
		case out <- CheckpointNotification{ThreadID: note.Key, Metadata: note.Value, Coalesced: note.Coalesced}:
			return true
		default:
			return false
		}
	}
}

// look reads one watched thread's head. A thread that has committed nothing yet has no state to
// report, which is not an outage: the first snapshot it commits is a change worth telling
// about.
func (p *CheckpointPoller) look(ctx context.Context, threadID string) (watch.Reading[CheckpointMetadata], error) {
	p.noteLook()
	metadata, found, err := p.store.ReadSessionHead(ctx, threadID)
	if err != nil {
		return watch.Reading[CheckpointMetadata]{}, err
	}
	if !found {
		return watch.Reading[CheckpointMetadata]{Found: false}, nil
	}
	return watch.Reading[CheckpointMetadata]{
		Value:   metadata,
		Version: metadata.Revision,
		Found:   true,
	}, nil
}

// Stats reports what the poller has seen. ActiveWatches is the leak evidence: it must return
// to zero once every watch's context has ended.
func (p *CheckpointPoller) Stats() WatchStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return WatchStats{
		Looks:         p.looks,
		Passes:        p.passes,
		Outages:       p.outages,
		LastOutage:    p.lastErr,
		ActiveWatches: int(p.active.Load()),
	}
}

func (p *CheckpointPoller) notePass() {
	p.mu.Lock()
	p.passes++
	p.mu.Unlock()
}

func (p *CheckpointPoller) noteLook() {
	p.mu.Lock()
	p.looks++
	p.mu.Unlock()
}

func (p *CheckpointPoller) noteOutage(err error) {
	p.mu.Lock()
	p.outages++
	p.lastErr = err
	p.mu.Unlock()
}
