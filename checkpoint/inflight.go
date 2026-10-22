package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jiujuan/goagent/core"
)

// InFlightBatch marks a tool batch that was planned and committed but has not
// produced results yet. A run that dies inside it cannot tell which side effects
// happened, so the batch is stored with the step that planned it and recovery
// replays it from a per-call ledger instead of running it again blindly.
type InFlightBatch struct {
	Step  int             `json:"step"`
	Calls []core.ToolCall `json:"calls"`
}

// ErrInFlightConflict reports a checkpoint whose work state cannot be trusted:
// an in-flight batch alongside a pause or a terminal record, or a batch that
// does not describe the step it claims to belong to.
var ErrInFlightConflict = errors.New("checkpoint: in-flight batch conflicts with pause or terminal state")

// RequiresResume reports whether a thread must be resumed before an ordinary new
// run may start on it. It covers the approval pause, the durable pause and an
// in-flight batch; it does not say the thread is paused, only that starting a new
// run here would leave recorded work unexplained.
func RequiresResume(cp *Checkpoint) bool {
	if cp == nil {
		return false
	}
	return cp.Pending != nil || cp.Pause != nil || cp.InFlight != nil
}

// validateWorkState gates the in-flight boundary. It lives in the storage layer
// rather than only in the run loop because every writer passes through
// freezeCheckpoint — a custom store, a hand-built checkpoint or a fork would
// otherwise be able to record a batch that no recovery path can interpret. The
// commit digest is computed after this check, so it only ever hashes a batch
// whose step and calls are consistent.
func validateWorkState(cp *Checkpoint) error {
	if cp == nil || cp.InFlight == nil {
		return nil
	}
	if cp.Pending != nil || cp.Pause != nil || cp.Completion != nil {
		return fmt.Errorf("%w: in-flight batch cannot coexist with pending, pause or completion", ErrInFlightConflict)
	}
	if cp.InFlight.Step != cp.Step {
		return fmt.Errorf("%w: in-flight step %d does not match checkpoint step %d", ErrInFlightConflict, cp.InFlight.Step, cp.Step)
	}
	if len(cp.InFlight.Calls) == 0 {
		return fmt.Errorf("%w: in-flight batch has no calls", ErrInFlightConflict)
	}
	if cp.Snapshot == nil || cp.Snapshot.Step != cp.Step || cp.Snapshot.ToolDigest == "" {
		return fmt.Errorf("%w: in-flight batch requires the step snapshot that produced it", ErrInFlightConflict)
	}
	advertised := make(map[string]struct{}, len(cp.Snapshot.Advertised))
	for _, name := range cp.Snapshot.Advertised {
		advertised[name] = struct{}{}
	}
	for _, call := range cp.InFlight.Calls {
		if call.ID == "" || call.Name == "" {
			return fmt.Errorf("%w: in-flight call requires ID and name", ErrInFlightConflict)
		}
		if _, ok := advertised[call.Name]; !ok {
			return fmt.Errorf("%w: in-flight call %q uses tool %q that the step did not advertise", ErrInFlightConflict, call.ID, call.Name)
		}
	}
	return nil
}

// cloneInFlight deep-copies the batch. Call IDs are deliberately not required to
// be unique: a model can emit the same ID twice and recovery matches calls in
// order, so a uniqueness rule here would reject a batch the run can execute.
func cloneInFlight(in *InFlightBatch) *InFlightBatch {
	if in == nil {
		return nil
	}
	out := *in
	if in.Calls == nil {
		return &out
	}
	out.Calls = make([]core.ToolCall, len(in.Calls))
	for i, call := range in.Calls {
		call.Args = json.RawMessage(cloneSnapshotBytes(call.Args))
		out.Calls[i] = call
	}
	return &out
}
