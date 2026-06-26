package middleware

import (
	"context"
	"sync"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Steering lets an external caller inject messages into a running agent. Queued
// messages are drained into the request just before the next model call, so
// they steer the in-flight run (e.g. a user adding clarification or a "stop and
// focus on X" nudge mid-task) without waiting for the turn to finish.
//
// Construct one, pass Middleware() to agent.Config.Middleware, and call Steer
// from another goroutine to enqueue. Injected messages are added to the model
// context for the current run; they are not committed to the session as their
// own events.
type Steering struct {
	mu    sync.Mutex
	queue []core.Message
}

// NewSteering creates an empty steering queue.
func NewSteering() *Steering { return &Steering{} }

// Steer enqueues messages to inject before the next model call. Safe to call
// from any goroutine.
func (s *Steering) Steer(msgs ...core.Message) {
	s.mu.Lock()
	s.queue = append(s.queue, msgs...)
	s.mu.Unlock()
}

// SteerText is a convenience for enqueueing a single user message.
func (s *Steering) SteerText(text string) { s.Steer(core.UserText(text)) }

// Pending reports how many messages are queued.
func (s *Steering) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (s *Steering) drain() []core.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return nil
	}
	out := s.queue
	s.queue = nil
	return out
}

// Middleware returns the steering Middleware bound to this queue.
func (s *Steering) Middleware() Middleware {
	return BeforeModel(func(_ context.Context, req *llm.Request) error {
		if pending := s.drain(); len(pending) > 0 {
			req.Messages = append(req.Messages, pending...)
		}
		return nil
	})
}
