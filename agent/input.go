package agent

import (
	"context"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// inputQueue is the loop's one read surface over the two input channels: the
// run's volatile steering queue and the thread's durable inbox. It is a view
// built from what RunContext already holds, not a third store — so a derived
// context (deeper, subRun, forBranch) reads its own queue and the shared inbox,
// exactly as the loop did before.
type inputQueue struct {
	rc      *RunContext
	durable checkpoint.Durable
}

func (rc *RunContext) inputs() inputQueue {
	durable, _ := rc.durableStore()
	return inputQueue{rc: rc, durable: durable}
}

// takeVolatile removes the steering entries this delivery point may deliver.
func (q inputQueue) takeVolatile(newTurn bool) []steerEntry {
	return q.rc.steering.takeReady(newTurn)
}

// takeVolatilePhase removes only the steering entries with the given phase.
// Preemption uses it, so a canceled model call retries with exactly the input
// that asked for it.
func (q inputQueue) takeVolatilePhase(phase core.InputPhase) []steerEntry {
	return q.rc.steering.takePhase(phase)
}

// takeDurable reads the thread's inbox, hiding messages this run already
// registered for acknowledgement but has not committed yet, and returns the
// ones this delivery point may deliver. It does not acknowledge anything: the
// acknowledgement commits with the next checkpoint, which is what makes input
// delivery and the conversation it influenced one atomic step.
//
// newTurn is not consulted yet. Durable records carry no delivery phase until
// the inbox stores one, so every inbox message is eligible now.
func (q inputQueue) takeDurable(ctx context.Context, newTurn bool) ([]checkpoint.InboxMessage, error) {
	if q.durable == nil {
		return nil, nil
	}
	inbox, err := q.durable.Inbox(ctx, q.rc.ThreadID)
	if err != nil {
		return nil, err
	}
	if len(inbox) == 0 {
		return nil, nil
	}
	acked := q.rc.pendingInboxAcks()
	out := make([]checkpoint.InboxMessage, 0, len(inbox))
	for _, msg := range inbox {
		if _, dup := acked[msg.ID]; dup {
			continue
		}
		out = append(out, msg)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// publishDeliveredVolatile reports the steering messages a delivery point
// appended, grouped by phase so a consumer can tell what arrived early from
// what reached the window it asked for.
func (rc *RunContext) publishDeliveredVolatile(step int, entries []steerEntry, preempted bool) {
	for _, phase := range steerEntryPhases(entries) {
		count := 0
		for _, e := range entries {
			if e.phase == phase {
				count++
			}
		}
		rc.publish(core.InputDelivered{
			Step:      step,
			Source:    core.InputVolatile,
			Phase:     phase,
			Count:     count,
			Preempted: preempted,
		})
	}
}

// publishDeliveredDurable reports the inbox messages a delivery point appended.
// MessageIDs lets a producer correlate its own delivery with the step that read
// it; the acknowledgement itself only lands with the next checkpoint.
func (rc *RunContext) publishDeliveredDurable(step int, msgs []checkpoint.InboxMessage, preempted bool) {
	if len(msgs) == 0 {
		return
	}
	ids := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		ids = append(ids, msg.ID)
	}
	rc.publish(core.InputDelivered{
		Step:       step,
		Source:     core.InputDurable,
		Phase:      core.InputBeforeModel,
		Count:      len(msgs),
		MessageIDs: ids,
		Preempted:  preempted,
	})
}

// steerEntryPhases lists the distinct phases in first-seen order.
func steerEntryPhases(entries []steerEntry) []core.InputPhase {
	seen := make(map[core.InputPhase]struct{}, len(entries))
	out := make([]core.InputPhase, 0, len(entries))
	for _, e := range entries {
		if _, ok := seen[e.phase]; ok {
			continue
		}
		seen[e.phase] = struct{}{}
		out = append(out, e.phase)
	}
	return out
}
