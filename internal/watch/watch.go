// Package watch is the one polling loop behind the optional notification views of the
// checkpoint and run stores (ADR-0034 D7).
//
// A notification is a hint to read again, never a record of what happened: there is no
// durable event log behind it, a process that restarts loses every walk in flight, and a
// consumer that falls behind gets the newest state per key instead of a queue of everything
// it missed. Both stores need exactly that behaviour, and two copies agreeing separately on
// "this version was already delivered" is how one store starts re-notifying a state the
// other stopped notifying.
//
// What stays with each store is what it reads and how a state is named: a checkpoint watch
// asks a thread for its head revision, a task watch asks a run for its record version.
package watch

import (
	"context"
	"time"
)

const (
	// DefaultInterval is how long the loop waits between two looks at its keys.
	DefaultInterval = 250 * time.Millisecond

	// DefaultBuffer is how many notifications the channel holds before they start merging.
	DefaultBuffer = 1
)

// Ticker is what the loop waits on. A test injects its own so a watch can cross several
// intervals without real time passing.
type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

// RuntimeTicker adapts the standard timer to Ticker.
type RuntimeTicker struct{ *time.Ticker }

func (t RuntimeTicker) Chan() <-chan time.Time { return t.C }

// Reading is one key's current state as the store reported it.
type Reading[T any] struct {
	Value   T
	Version uint64
	// Found is false while the key has no state yet — a run whose record was never created,
	// a thread that has committed nothing. That is not an outage, and the key stays worth
	// watching: state that appears later is a change.
	Found bool
}

// Read looks one key up. An error means the store could not answer, not that the key is
// gone; the loop reports it and keeps watching either way.
type Read[T any] func(ctx context.Context, key string) (Reading[T], error)

// Notification says one key reached a state this walk had not seen. Value is what the store
// said at that moment, which may already be history by the time a caller reads it — the
// caller re-reads, that is the point of the hint.
type Notification[T any] struct {
	Key     string
	Value   T
	Version uint64
	// Coalesced counts the further changes this notification also stands for. It is zero for
	// a consumer that keeps up, and grows while one does not read.
	Coalesced int
}

// Loop polls a fixed set of keys and delivers at most one undelivered notification per key,
// always the newest state.
type Loop[T any] struct {
	Keys      []string
	Read      Read[T]
	Interval  time.Duration
	NewTicker func(time.Duration) Ticker

	// OnError observes a read the store could not answer. It is optional; a watch that never
	// notifies is distinguishable from a watch that cannot read only through it.
	OnError func(error)

	// OnPass observes a finished look at every key, deliveries included. A caller that reports
	// its own progress can answer "has this pass run yet?" — which is what a test stepping the
	// loop by hand needs, and what an operator watching a stalled poller wants too.
	OnPass func()
}

// Run works until ctx ends, calling deliver once per pending key per pass. The caller owns
// the channel it delivers into and closes it when Run returns — which is the only exit, and
// nothing here outlives the context.
//
// The first pass takes the baseline — the state each key is in when the watch starts is not a
// change to report, so a client that reconnects and re-reads current state first is not told
// about it twice.
//
// deliver answers whether the notification was taken. Refusing one (a full buffer) is not a
// loss: the key stays pending, and the next pass replaces it with something newer, which is
// how a slow consumer ends up with the newest state instead of a backlog.
func (l *Loop[T]) Run(ctx context.Context, deliver func(Notification[T]) bool) {
	ticker := l.newTicker()
	defer ticker.Stop()

	seen := make(map[string]uint64, len(l.Keys))
	pending := make(map[string]Notification[T], len(l.Keys))
	baselined := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
		}

		for _, key := range l.Keys {
			reading, err := l.Read(ctx, key)
			if err != nil {
				if l.OnError != nil {
					l.OnError(err)
				}
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if !reading.Found {
				continue
			}
			previous, known := seen[key]
			if known && reading.Version <= previous {
				// The same version again — an idempotent retry that changed nothing — or a
				// reading that went backwards. Neither is a new state, and neither moves the
				// marker this walk compares against.
				continue
			}
			seen[key] = reading.Version
			if !known && !baselined {
				// First sight of this key before the baseline: the state a watch starts from
				// is not a change to report. After the baseline, a key that was not there and
				// now is has changed.
				continue
			}

			note := Notification[T]{Key: key, Value: reading.Value, Version: reading.Version}
			if held, waiting := pending[key]; waiting {
				note.Coalesced = held.Coalesced + 1
			}
			pending[key] = note
		}
		baselined = true

		l.flush(deliver, pending)
		if l.OnPass != nil {
			l.OnPass()
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// flush hands out what is pending without ever blocking the loop on a slow consumer, in the
// order the caller asked for the keys.
func (l *Loop[T]) flush(deliver func(Notification[T]) bool, pending map[string]Notification[T]) {
	for _, key := range l.Keys {
		note, waiting := pending[key]
		if !waiting {
			continue
		}
		if deliver(note) {
			delete(pending, key)
		}
	}
}

func (l *Loop[T]) newTicker() Ticker {
	interval := l.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	if l.NewTicker != nil {
		return l.NewTicker(interval)
	}
	return RuntimeTicker{Ticker: time.NewTicker(interval)}
}
