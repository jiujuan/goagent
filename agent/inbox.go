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
func (a *Agent) Deliver(ctx context.Context, threadID string, msg checkpoint.InboxMessage) (checkpoint.DeliveryReceipt, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return checkpoint.DeliveryReceipt{}, err
	}
	if msg.ID == "" {
		return checkpoint.DeliveryReceipt{}, fmt.Errorf("agent: durable delivery requires a message ID")
	}
	durable, ok := a.store.(checkpoint.Durable)
	if !ok {
		return checkpoint.DeliveryReceipt{}, checkpoint.ErrDurableUnavailable
	}
	return durable.Deliver(ctx, threadID, msg)
}
