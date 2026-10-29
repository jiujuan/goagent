package agent

import (
	"slices"

	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/core"
)

func (r *Run) subscribeRun(mode bus.DeliveryMode) (<-chan core.Event, func()) {
	r.observationMu.Lock()
	defer r.observationMu.Unlock()
	if !r.eventsSealed {
		return r.bus.Subscribe(r.topic, mode)
	}
	tail := copyRunTail(r.eventTail)
	ch := make(chan core.Event, len(tail))
	for _, ev := range tail {
		ch <- ev
	}
	close(ch)
	return ch, func() {}
}

// Only normalized, privately owned payloads enter the observation cache.
func (r *Run) sealEvents(tail []core.Event) {
	r.observationMu.Lock()
	defer r.observationMu.Unlock()
	if r.eventsSealed {
		return
	}
	r.eventsSealed = true
	r.eventTail = tail
	r.bus.FinishTopic(r.topic, tail...)
}

func copyRunTail(tail []core.Event) []core.Event {
	out := slices.Clone(tail)
	for i, ev := range out {
		switch value := ev.(type) {
		case core.RunDone:
			result, err := cloneResult(value.Result)
			if err != nil {
				panic("agent: unnormalized terminal result: " + err.Error())
			}
			value.Result = result
			out[i] = value
		case core.Interrupted:
			value.Pending = slices.Clone(value.Pending)
			for j := range value.Pending {
				value.Pending[j].Args = slices.Clone(value.Pending[j].Args)
			}
			out[i] = value
		}
	}
	return out
}
