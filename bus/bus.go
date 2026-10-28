// Package bus is v2's observational event bus: a topic-based publish/subscribe
// fan-out over Go channels. It is the streaming primitive for OBSERVATION only
// (durability is the Checkpointer's job). Many subscribers — UI, JSONL tracing,
// a progress bar — can watch one run without tee-ing an iterator.
//
// The producer (the AgentLoop) calls Publish from a single goroutine, so events
// are totally ordered per topic. Each subscriber gets its own buffered channel
// and a DeliveryMode:
//
//   - Lossy:    drop when the buffer is full (UIs that can skip partials).
//   - Lossless: block the publisher when full, giving back-pressure (tracing /
//     persistence sinks that must not drop).
//
// A stalled Lossless subscriber back-pressures its publisher. Index operations
// and publishers on other topics remain independent.
package bus

import (
	"context"
	"sync"

	"github.com/jiujuan/goagent/core"
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

// Subscribe registers an observer. The returned idempotent cancel function
// stops blocked sends and waits until the channel is closed. Always cancel
// when abandoning an observer.
func (b *Bus) Subscribe(topic Topic, mode DeliveryMode) (<-chan core.Event, func()) {
	s := newSubscriber(b.bufSize, mode)
	b.mu.Lock()
	if b.subs[topic] == nil {
		b.subs[topic] = map[*subscriber]struct{}{}
	}
	b.subs[topic][s] = struct{}{}
	b.mu.Unlock()
	return s.ch, func() {
		s.cancel(func() {
			b.mu.Lock()
			if set := b.subs[topic]; set != nil {
				delete(set, s)
				if len(set) == 0 {
					delete(b.subs, topic)
				}
			}
			b.mu.Unlock()
		})
	}
}

// Publish sends with a background context. Use PublishContext for cancellable
// back-pressure. Publish calls must be serialized per topic to preserve order.
func (b *Bus) Publish(topic Topic, ev core.Event) {
	_ = b.PublishContext(context.Background(), topic, ev)
}

// PublishContext fans ev out without holding the topic index lock while sending.
// Cancellation can occur after delivery to some subscribers; retrying may
// duplicate events. An already canceled context permits only immediately ready
// sends and still returns its error. A canceled subscription is skipped.
func (b *Bus) PublishContext(ctx context.Context, topic Topic, ev core.Event) error {
	startErr := ctx.Err()
	b.mu.RLock()
	subs := make([]*subscriber, 0, len(b.subs[topic]))
	for s := range b.subs[topic] {
		subs = append(subs, s)
	}
	b.mu.RUnlock()
	// Preserve immediately deliverable cancellation diagnostics, while a canceled
	// publisher never waits for buffer space. Delivery remains best effort.
	if startErr != nil {
		for _, s := range subs {
			s.trySend(ev)
		}
		return startErr
	}
	for _, s := range subs {
		if err := s.send(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// Subscribers reports the live subscriber count on topic.
func (b *Bus) Subscribers(topic Topic) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[topic])
}

// FinishTopic detaches current subscribers and schedules their ordered terminal
// tail after in-flight normal sends exit. It never waits for observers. Tail
// payloads must be normalized core values (as produced by Run settlement).
// Subscribers must drain the channel or cancel. Later generic subscriptions
// are new subscriptions: Bus retains no terminal history.
func (b *Bus) FinishTopic(topic Topic, tail ...core.Event) {
	b.mu.Lock()
	subs := b.subs[topic]
	delete(b.subs, topic)
	b.mu.Unlock()
	for s := range subs {
		s.seal(cloneTerminalTail(tail))
	}
}
