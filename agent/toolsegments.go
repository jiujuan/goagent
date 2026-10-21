package agent

import "github.com/jiujuan/goagent/core"

// toolCallSegment is one dispatch unit of a step's batch. A serial segment
// carries exactly one call and runs alone; a parallel segment runs its calls
// concurrently, and the next segment starts only after all of them settle.
type toolCallSegment struct {
	serial  bool
	indexes []int
}

// buildToolCallSegments plans the order a batch runs in. It is a pure function
// of the calls, the frozen tool view and the execution mode: no run state, no
// goroutines, no events.
//
// ToolSequential makes every call its own serial segment. Otherwise a call
// whose frozen implementation declares SequentialTool is a boundary: the
// consecutive calls before it form one parallel segment, it gets a serial
// segment of its own, and the calls after it start a new parallel segment.
// Unknown names do not form a boundary; callOne rejects them later.
func buildToolCallSegments(calls []core.ToolCall, tools *ToolView, mode ToolExecMode) []toolCallSegment {
	if len(calls) == 0 {
		return nil
	}
	if mode == ToolSequential {
		segments := make([]toolCallSegment, 0, len(calls))
		for i := range calls {
			segments = append(segments, toolCallSegment{serial: true, indexes: []int{i}})
		}
		return segments
	}

	var segments []toolCallSegment
	var group []int
	flush := func() {
		if len(group) > 0 {
			segments = append(segments, toolCallSegment{indexes: group})
			// A later append must not reuse the array this segment owns.
			group = nil
		}
	}
	for i, c := range calls {
		if tools.Sequential(c.Name) {
			flush()
			segments = append(segments, toolCallSegment{serial: true, indexes: []int{i}})
			continue
		}
		group = append(group, i)
	}
	flush()
	return segments
}
