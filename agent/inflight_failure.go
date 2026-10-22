package agent

import (
	"context"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// completeFailedBatch settles a tool batch that wrote its in-flight checkpoint
// and then failed partway. Every planned call gets exactly one result so the
// failure checkpoint keeps the assistant message and its results paired;
// fail()'s seam only trims a trailing assistant message, which would otherwise
// erase the batch's only durable description.
//
// Already reported results are kept as the handlers returned them. Missing ones
// are settled from the ledger without starting work: a completed call replays
// its stored result, a claimed or unknown call is reported as unknown, and a
// pending or absent call is reported as not executed. Reading the ledger uses a
// context detached from cancellation: the failure is why this runs, and the
// conversation record must still be completed.
func (l *AgentLoop) completeFailedBatch(rc *RunContext, step int, calls []core.ToolCall, results []core.Part) []core.Part {
	if len(calls) == 0 {
		return results
	}
	byCall := make(map[string]core.ToolResult, len(results))
	for _, p := range results {
		if tr, ok := p.(core.ToolResult); ok {
			byCall[tr.CallID] = tr
		}
	}
	ctx := context.Background()
	if rc.Context != nil {
		ctx = context.WithoutCancel(rc.Context)
	}
	durable, hasDurable := rc.durableStore()
	out := make([]core.Part, 0, len(calls))
	for _, call := range calls {
		if tr, ok := byCall[call.ID]; ok {
			out = append(out, tr)
			continue
		}
		if !hasDurable {
			out = append(out, errResult(call, "not executed: the run stopped before this call started"))
			continue
		}
		invocationID, _ := toolInvocation(rc.ThreadID, step, call)
		ex, found, err := durable.ToolExecution(ctx, rc.ThreadID, invocationID)
		switch {
		case err != nil:
			out = append(out, errResult(call, "not executed: the tool ledger could not be read: "+err.Error()))
		case found && ex.State == checkpoint.ToolCompleted:
			if tr, ok := resultFromExecution(ex, call); ok {
				out = append(out, tr)
			} else {
				out = append(out, errResult(call, "stored tool result is invalid and cannot be replayed"))
			}
		case found && (ex.State == checkpoint.ToolClaimed || ex.State == checkpoint.ToolUnknown):
			out = append(out, unknownToolResult(call, ex.IdempotencyKey))
		default:
			out = append(out, errResult(call, "not executed: the run stopped before this call started"))
		}
	}
	return out
}
