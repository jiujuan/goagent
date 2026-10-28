package bus

import (
	"slices"

	"github.com/jiujuan/goagent/core"
)

// Settlement validates message variants before sealing. This is a structural
// copy only: no serialization or business validation occurs in a finisher.
func cloneTerminalTail(tail []core.Event) []core.Event {
	out := make([]core.Event, len(tail))
	for i, ev := range tail {
		switch value := ev.(type) {
		case core.RunDone:
			message, err := core.CloneMessage(value.Result.Message)
			if err != nil {
				panic("bus: terminal tail must contain normalized core parts: " + err.Error())
			}
			value.Result.Message = message
			out[i] = value
		case core.Interrupted:
			value.Pending = slices.Clone(value.Pending)
			for j := range value.Pending {
				value.Pending[j].Args = slices.Clone(value.Pending[j].Args)
			}
			out[i] = value
		default:
			out[i] = ev
		}
	}
	return out
}
