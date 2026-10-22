package agent

import (
	"context"
	"fmt"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// Deliver records a message for durable, at-least-once delivery before a later
// model turn on threadID. The producer supplies msg.ID and must reuse it when a
// delivery attempt is retried after an ambiguous failure. Steer remains the
// intentionally volatile, in-process alternative for a live Run.
//
// The message keeps msg.Phase; an empty phase means core.InputBeforeModel. Use
// DeliverWith to state the phase separately from the record.
func (a *Agent) Deliver(ctx context.Context, threadID string, msg checkpoint.InboxMessage) (checkpoint.DeliveryReceipt, error) {
	return a.DeliverWith(ctx, threadID, msg, msg.Phase)
}

// DeliverWith records a message that may be delivered no later than phase. A
// next_turn message waits for a turn to restart instead of arriving mid-turn,
// which is what makes the durable channel able to hold a steering-style
// instruction as long as the volatile one does. An unknown phase is refused
// before anything is written.
func (a *Agent) DeliverWith(ctx context.Context, threadID string, msg checkpoint.InboxMessage, phase core.InputPhase) (checkpoint.DeliveryReceipt, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return checkpoint.DeliveryReceipt{}, err
	}
	if msg.ID == "" {
		return checkpoint.DeliveryReceipt{}, fmt.Errorf("agent: durable delivery requires a message ID")
	}
	msg.Phase = phase
	durable, ok := a.store.(checkpoint.Durable)
	if !ok {
		return checkpoint.DeliveryReceipt{}, checkpoint.ErrDurableUnavailable
	}
	return durable.Deliver(ctx, threadID, msg)
}
