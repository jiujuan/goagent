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
// newTurn filters by phase exactly like the volatile queue: mid-turn steps keep
// a next_turn message in the inbox. Any other phase, including the empty value
// a record written before phases existed carries, means "by the next
// PrepareTurn" and is eligible now.
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
		if !newTurn && msg.Phase == core.InputNextTurn {
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

// publishDeliveredDurable reports the inbox messages a delivery point appended,
// grouped by phase so a consumer can tell a message that reached the window it
// asked for from one that arrived early. MessageIDs let a producer correlate its
// own delivery with the step that read it; the acknowledgement itself only lands
// with the next checkpoint.
func (rc *RunContext) publishDeliveredDurable(step int, msgs []checkpoint.InboxMessage, preempted bool) {
	for _, phase := range inboxMessagePhases(msgs) {
		var ids []string
		for _, msg := range msgs {
			if msg.Phase == phase {
				ids = append(ids, msg.ID)
			}
		}
		rc.publish(core.InputDelivered{
			Step:       step,
			Source:     core.InputDurable,
			Phase:      phase,
			Count:      len(ids),
			MessageIDs: ids,
			Preempted:  preempted,
		})
	}
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

// inboxMessagePhases lists the distinct phases of inbox records in first-seen
// order, the same rule the volatile grouping uses.
func inboxMessagePhases(msgs []checkpoint.InboxMessage) []core.InputPhase {
	seen := make(map[core.InputPhase]struct{}, len(msgs))
	out := make([]core.InputPhase, 0, len(msgs))
	for _, msg := range msgs {
		if _, ok := seen[msg.Phase]; ok {
			continue
		}
		seen[msg.Phase] = struct{}{}
		out = append(out, msg.Phase)
	}
	return out
}
