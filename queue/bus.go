package queue

import (
	"sync"

	"github.com/jiujuan/goagent/core"
)

// Bus is a key-scoped publish/subscribe channel for events. Workers publish a
// job's progress and result under the job's Key; a frontend subscribes to that
// key to receive the live stream.
type Bus interface {
	// Publish delivers ev to every current subscriber of key. It must not block
	// the caller on a slow subscriber.
	Publish(key string, ev *core.Event)

	// Subscribe returns a channel of events for key and a function that cancels
	// the subscription (closing the channel). Multiple subscribers per key are
	// allowed.
	Subscribe(key string) (<-chan *core.Event, func())
}

// MemBus is an in-process Bus. Delivery is non-blocking: if a subscriber's
// buffer is full (a slow consumer), events are dropped for that subscriber
// rather than stalling the publisher. Progress is advisory; a caller that needs
// every event should persist the final result separately (the worker does not).
type MemBus struct {
	mu   sync.Mutex
	subs map[string]map[int]chan *core.Event
	next int
}

// NewMemBus creates an empty in-process bus.
func NewMemBus() *MemBus {
	return &MemBus{subs: map[string]map[int]chan *core.Event{}}
}

// Subscribe implements Bus.
func (b *MemBus) Subscribe(key string) (<-chan *core.Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs[key] == nil {
		b.subs[key] = map[int]chan *core.Event{}
	}
	id := b.next
	b.next++
	ch := make(chan *core.Event, 64)
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
func (b *MemBus) Publish(key string, ev *core.Event) {
	b.mu.Lock()
	chans := make([]chan *core.Event, 0, len(b.subs[key]))
	for _, ch := range b.subs[key] {
		chans = append(chans, ch)
	}
	b.mu.Unlock()

	for _, ch := range chans {
		select {
		case ch <- ev:
		default: // slow subscriber: drop rather than block the worker
		}
	}
}

var _ Bus = (*MemBus)(nil)
