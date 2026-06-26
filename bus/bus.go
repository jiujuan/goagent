// Package bus is v2's observational event bus: a topic-based publish/subscribe
// fan-out over Go channels. It replaces v1's iter.Seq2 as the streaming
// primitive for OBSERVATION only (durability is the Checkpointer's job). Many
// subscribers — UI, JSONL tracing, a progress bar — can watch one run without
// tee-ing an iterator.
//
// The producer (the AgentLoop) calls Publish from a single goroutine, so events
// are totally ordered per topic. Each subscriber gets its own buffered channel
// and a DeliveryMode:
//
//   - Lossy:    drop when the buffer is full (UIs that can skip partials).
//   - Lossless: block the publisher when full, giving back-pressure (tracing /
//     persistence sinks that must not drop).
//
// See ADR 0023. A stalled Lossless subscriber back-pressures the whole bus by
// design; refine with per-subscriber timeouts in the real implementation.
package bus

import (
	"sync"

	"github.com/jiujuan/goagent/event"
)

// Topic identifies a stream of events, e.g. one run or one (app,user,session).
type Topic string

// DeliveryMode selects a subscriber's overflow policy.
type DeliveryMode int

const (
	// Lossy drops events for this subscriber when its buffer is full.
	Lossy DeliveryMode = iota
	// Lossless blocks the publisher until this subscriber has room.
	Lossless
)

const defaultBufferSize = 64

type subscriber struct {
	ch   chan event.Event
	mode DeliveryMode
}

// Bus is a topic-based pub/sub fan-out. The zero value is not usable; call New.
type Bus struct {
	mu      sync.RWMutex
	subs    map[Topic]map[*subscriber]struct{}
	bufSize int
}

// Option configures a Bus.
type Option func(*Bus)

// WithBufferSize sets the per-subscriber channel buffer (default 64).
func WithBufferSize(n int) Option {
	return func(b *Bus) {
		if n > 0 {
			b.bufSize = n
		}
	}
}

// New constructs a Bus.
func New(opts ...Option) *Bus {
	b := &Bus{
		subs:    map[Topic]map[*subscriber]struct{}{},
		bufSize: defaultBufferSize,
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Subscribe registers a subscriber on topic and returns its receive channel
// plus a cancel func. cancel removes the subscriber and closes the channel; it
// is safe to call once and idempotent thereafter.
//
// Publish (under RLock) and cancel (under Lock) are mutually exclusive, so a
// send can never race a close — no send-on-closed panic.
func (b *Bus) Subscribe(topic Topic, mode DeliveryMode) (<-chan event.Event, func()) {
	s := &subscriber{ch: make(chan event.Event, b.bufSize), mode: mode}

	b.mu.Lock()
	if b.subs[topic] == nil {
		b.subs[topic] = map[*subscriber]struct{}{}
	}
	b.subs[topic][s] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			if set := b.subs[topic]; set != nil {
				if _, ok := set[s]; ok {
					delete(set, s)
					close(s.ch)
				}
				if len(set) == 0 {
					delete(b.subs, topic)
				}
			}
			b.mu.Unlock()
		})
	}
	return s.ch, cancel
}

// Publish fans ev out to every subscriber on topic. Call it from a single
// goroutine per topic to preserve ordering.
func (b *Bus) Publish(topic Topic, ev event.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs[topic] {
		switch s.mode {
		case Lossy:
			select {
			case s.ch <- ev:
			default: // buffer full: drop
			}
		default: // Lossless: block for back-pressure
			s.ch <- ev
		}
	}
}

// Subscribers reports the live subscriber count on topic (useful in tests and
// for shedding work when nobody is watching).
func (b *Bus) Subscribers(topic Topic) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[topic])
}
