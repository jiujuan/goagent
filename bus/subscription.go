package bus

import (
	"context"
	"sync"

	"github.com/jiujuan/goagent/core"
)

type subscriptionPhase uint8

const (
	subscriptionActive subscriptionPhase = iota
	subscriptionSealing
	subscriptionCanceling
	subscriptionClosed
)

type subscriber struct {
	mu           sync.Mutex
	phase        subscriptionPhase
	ch           chan core.Event
	mode         DeliveryMode
	normalWG     sync.WaitGroup
	sendStop     chan struct{}
	cancelSignal chan struct{}
	closed       chan struct{}
	tailDone     chan struct{}
}

func newSubscriber(size int, mode DeliveryMode) *subscriber {
	return &subscriber{ch: make(chan core.Event, size), mode: mode,
		sendStop: make(chan struct{}), cancelSignal: make(chan struct{}), closed: make(chan struct{})}
}

// Register under the phase lock so Wait cannot race a new Add.
func (s *subscriber) beginSend() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != subscriptionActive {
		return false
	}
	s.normalWG.Add(1)
	return true
}
func (s *subscriber) endSend() { s.normalWG.Done() }

func (s *subscriber) send(ctx context.Context, ev core.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.beginSend() {
		return nil
	}
	defer s.endSend()
	if s.mode == Lossy {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.sendStop:
			return nil
		case s.ch <- ev:
			return nil
		default:
			return nil
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.sendStop:
		return nil
	case s.ch <- ev:
		return nil
	}
}

// trySend preserves ready diagnostics after cancellation without restoring
// back-pressure. Registration still protects the send from channel closure.
func (s *subscriber) trySend(ev core.Event) {
	if !s.beginSend() {
		return
	}
	defer s.endSend()
	select {
	case <-s.sendStop:
	case s.ch <- ev:
	default:
	}
}

func (s *subscriber) cancel(remove func()) {
	s.mu.Lock()
	if s.phase == subscriptionCanceling || s.phase == subscriptionClosed {
		done := s.closed
		if s.tailDone != nil {
			done = s.tailDone
		}
		s.mu.Unlock()
		<-done
		return
	}
	if s.phase == subscriptionActive {
		close(s.sendStop)
	}
	s.phase = subscriptionCanceling
	close(s.cancelSignal)
	tailDone := s.tailDone
	s.mu.Unlock()
	remove()
	if tailDone != nil {
		<-tailDone
		return
	}
	s.normalWG.Wait()
	s.finish()
}

func (s *subscriber) seal(tail []core.Event) {
	s.mu.Lock()
	if s.phase != subscriptionActive {
		s.mu.Unlock()
		return
	}
	s.phase = subscriptionSealing
	close(s.sendStop)
	s.tailDone = make(chan struct{})
	s.mu.Unlock()
	go func() {
		defer close(s.tailDone)
		defer s.finish()
		s.normalWG.Wait()
		for _, ev := range tail {
			select {
			case <-s.cancelSignal:
				return
			default:
			}
			select {
			case s.ch <- ev:
			case <-s.cancelSignal:
				return
			}
		}
	}()
}

// Only the cancel owner (or, after sealing, the finisher) closes the channel.
func (s *subscriber) finish() {
	s.mu.Lock()
	s.phase = subscriptionClosed
	close(s.ch)
	close(s.closed)
	s.mu.Unlock()
}
