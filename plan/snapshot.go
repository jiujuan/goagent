package plan

import (
	"encoding/json"

	"github.com/jiujuan/goagent/session"
)

// snapshot is the serializable runtime state of a plan: enough to resume a run
// by skipping completed steps and re-entering from the failure frontier. The
// executors themselves are code and do not serialize; on resume they are
// re-bound from a fresh plan template by Merge.
type snapshot struct {
	ID    string         `json:"id"`
	Goal  string         `json:"goal"`
	Steps []stepSnapshot `json:"steps"`
}

type stepSnapshot struct {
	ID       string      `json:"id"`
	Status   StepStatus  `json:"status"`
	Attempts int         `json:"attempts"`
	Result   *StepResult `json:"result,omitempty"`
}

// PlanStateKey is the session-state key under which a plan's snapshot is stored.
// The Runner persists it (via Actions.StateDelta on committed events), so a
// FileStore-backed session can reconstruct the snapshot on the next run.
func PlanStateKey(planID string) string { return "plan:" + planID }

// PlanFailureKey is the session-state key under which the last run's failure
// summary is written before a replan, so a planner can read what went wrong.
const PlanFailureKey = "plan:last_failure"

// recordFailure writes a human-readable summary of the failed steps to state,
// for the planner to consult when regenerating the plan.
func recordFailure(state session.State, p *Plan) {
	var msg string
	for _, s := range p.Steps {
		if s.Status == Failed && s.Result != nil && s.Result.Err != "" {
			if msg != "" {
				msg += "; "
			}
			name := s.Name
			if name == "" {
				name = s.ID
			}
			msg += name + ": " + s.Result.Err
		}
	}
	state.Set(PlanFailureKey, msg)
}

// snapshotOf captures the current runtime state of a plan.
func snapshotOf(p *Plan) snapshot {
	snap := snapshot{ID: p.ID, Goal: p.Goal, Steps: make([]stepSnapshot, len(p.Steps))}
	for i, s := range p.Steps {
		snap.Steps[i] = stepSnapshot{ID: s.ID, Status: s.Status, Attempts: s.Attempts, Result: s.Result}
	}
	return snap
}

// snapshotDelta builds the StateDelta payload that persists a plan's snapshot.
// The value is a JSON string so it round-trips cleanly through any State backend
// (including the JSONL FileStore).
func snapshotDelta(p *Plan) map[string]any {
	b, err := json.Marshal(snapshotOf(p))
	if err != nil {
		return nil
	}
	return map[string]any{PlanStateKey(p.ID): string(b)}
}

// loadSnapshot reads a previously-persisted snapshot for planID from state. The
// bool is false when no snapshot exists (a fresh run).
func loadSnapshot(state session.State, planID string) (snapshot, bool) {
	v, ok := state.Get(PlanStateKey(planID))
	if !ok {
		return snapshot{}, false
	}
	raw, ok := v.(string)
	if !ok {
		return snapshot{}, false
	}
	var snap snapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return snapshot{}, false
	}
	return snap, true
}

// Merge resolves the plan to run on a resumed session. It clones the static
// template (which carries the live, non-serializable Executors), then overlays
// any persisted runtime state: steps that finished (Done/Skipped) are kept
// terminal and will be skipped by the scheduler; interrupted or failed steps
// (Running/Failed/Blocked/Pending) are reset to Pending so they re-run from the
// failure frontier. Completed steps' outputs are restored into session state so
// downstream steps can read them. The bool reports whether a snapshot was found.
func Merge(template *Plan, state session.State) (*Plan, bool) {
	p := clonePlan(template)
	snap, ok := loadSnapshot(state, template.ID)
	if !ok {
		return p, false
	}
	byID := p.byID()
	for _, ss := range snap.Steps {
		st := byID[ss.ID]
		if st == nil {
			continue // template changed; ignore stale step
		}
		switch ss.Status {
		case Done, Skipped:
			st.Status = ss.Status
			st.Attempts = ss.Attempts
			st.Result = ss.Result
			if ss.Status == Done && ss.Result != nil {
				state.Set(StepResultKey(st.ID), ss.Result.Output)
			}
		default:
			// Running/Failed/Blocked/Pending: re-run from a clean slate.
			st.Status = Pending
		}
	}
	return p, true
}
