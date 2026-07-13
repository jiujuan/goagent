package queue

import (
	"sync"

	"github.com/jiujuan/goagent/core"
)

// Bus is a key-scoped publish/subscribe channel for events — the cross-process
// progress side of the queue. A worker publishes a background run's events
// under a key; a frontend (possibly in another process, over Redis) subscribes
// to that key to watch progress live.
//
// Delivery is lossy and advisory (a slow subscriber's events are dropped, never
// blocking the publisher; a late subscriber misses earlier events). The
// authoritative final result is persisted by the job, not the bus.
type Bus interface {
	// Publish delivers ev to every current subscriber of key without blocking.
	Publish(key string, ev core.Event)
	// Subscribe returns a channel of events for key and a cancel func.
	Subscribe(key string) (<-chan core.Event, func())
}

// Bridge forwards events from src onto b under key until src is closed. Use it
// in a worker to mirror an agent run's events (run.Events) onto a (Redis) Bus so
// another process can watch progress:
//
//	ch, cancel := run.Events(bus.Lossy)
//	defer cancel()
//	queue.Bridge(progressBus, key, ch)
func Bridge(b Bus, key string, src <-chan core.Event) {
	for ev := range src {
		b.Publish(key, ev)
	}
}

// MemBus is an in-process Bus. Delivery is non-blocking: a full subscriber
// buffer drops events rather than stalling the publisher.
type MemBus struct {
	mu   sync.Mutex
	subs map[string]map[int]chan core.Event
	next int
}

// NewMemBus creates an empty in-process bus.
func NewMemBus() *MemBus {
	return &MemBus{subs: map[string]map[int]chan core.Event{}}
}

// Subscribe implements Bus.
func (b *MemBus) Subscribe(key string) (<-chan core.Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs[key] == nil {
		b.subs[key] = map[int]chan core.Event{}
	}
	id := b.next
	b.next++
	ch := make(chan core.Event, 64)
	b.subs[key][id] = ch

	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if m := b.subs[key]; m != nil {
			if c, ok := m[id]; ok {
				delete(m, id)
				close(c)
			}
			if len(m) == 0 {
				delete(b.subs, key)
			}
		}
	}
	return ch, cancel
}

// Publish implements Bus.
func (b *MemBus) Publish(key string, ev core.Event) {
	b.mu.Lock()
	chans := make([]chan core.Event, 0, len(b.subs[key]))
	for _, ch := range b.subs[key] {
		chans = append(chans, ch)
	}
	b.mu.Unlock()

	for _, ch := range chans {
		select {
		case ch <- ev:
		default: // slow subscriber: drop
		}
	}
}

var _ Bus = (*MemBus)(nil)
