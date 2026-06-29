package eval

import (
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
)

// record.go bridges runtime to eval: it subscribes to a run's event stream and
// assembles a Trajectory from the observable events (TurnStarted, MessageDone,
// ToolStarted/ToolDone), then settles the run for its Result. It is a pure
// observer, like middleware.Tracing — it drives the run via Iter and changes
// nothing about execution.

// Record drives a run to completion while collecting its trajectory. It returns
// the assembled Trajectory, the run's terminal Result, and any run error.
//
// Trajectory.Input is left empty — the originating user message is not on the
// event stream; callers that need it for a judge set traj.Input themselves
// (Harness does this from Case.Input).
func Record(run *agent.Run) (*Trajectory, core.Result, error) {
	traj := &Trajectory{}
	calls := map[string]core.ToolCall{} // callID → originating call
	started := map[string]time.Time{}   // callID → start time (for latency)

	for ev, err := range run.Iter() {
		if err != nil {
			continue // terminal error is reported via Wait below
		}
		switch e := ev.(type) {
		case core.TurnStarted:
			traj.Steps++
		case core.MessageDone:
			traj.Messages = append(traj.Messages, e.Message)
			if e.Usage != nil {
				traj.Usage.InputTokens += e.Usage.InputTokens
				traj.Usage.OutputTokens += e.Usage.OutputTokens
			}
		case core.ToolStarted:
			calls[e.Call.ID] = e.Call
			started[e.Call.ID] = time.Now()
		case core.ToolDone:
			ep := ToolEpisode{Call: calls[e.Result.CallID], Result: e.Result}
			if t, ok := started[e.Result.CallID]; ok {
				ep.Latency = time.Since(t)
			}
			traj.Tools = append(traj.Tools, ep)
			// Keep the transcript faithful: the tool result follows its call.
			traj.Messages = append(traj.Messages, core.Message{
				Role:  core.RoleTool,
				Parts: []core.Part{e.Result},
			})
		}
	}

	res, err := run.Wait()
	traj.Final = res
	return traj, res, err
}
