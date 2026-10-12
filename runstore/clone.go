package runstore

import (
	"encoding/json"
	"fmt"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// clone returns a deep copy of the record, so nothing a caller can reach through a
// returned record points into stored state. Times and scalars are values; only pointers
// and message payloads need work.
//
// A message that cannot be cloned without changing its JSON form is refused rather than
// shared: handing out an alias would break the isolation every Store promises.
func (r RunRecord) clone() (RunRecord, error) {
	out := r
	out.Error = cloneError(r.Error)
	out.RecordError = cloneError(r.RecordError)
	out.LatestCheckpointRef = cloneRef(r.LatestCheckpointRef)
	out.CompletionRef = cloneRef(r.CompletionRef)
	if r.Result != nil {
		msg, err := core.CloneMessage(r.Result.Message)
		if err != nil {
			return RunRecord{}, fmt.Errorf("runstore: run %q result cannot be copied: %w", r.RunID, err)
		}
		out.Result = &core.Result{Message: msg}
	}
	return out, nil
}

func cloneError(e *RunError) *RunError {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}

func cloneRef(ref *checkpoint.Ref) *checkpoint.Ref {
	if ref == nil {
		return nil
	}
	c := *ref
	return &c
}

// equal compares two frozen outcomes. The result message is compared by its encoded
// content, so an idempotent terminal retry must carry the same answer and not merely the
// same shape.
func (d terminalDecision) equal(other terminalDecision) bool {
	if d.Status != other.Status ||
		d.FinishReason != other.FinishReason ||
		d.LatestCheckpointID != other.LatestCheckpointID ||
		d.CheckpointRevision != other.CheckpointRevision ||
		d.CompletionID != other.CompletionID {
		return false
	}
	if !equalError(d.Error, other.Error) {
		return false
	}
	if !equalError(d.RecordError, other.RecordError) {
		return false
	}
	if d.Result == nil || other.Result == nil {
		return d.Result == other.Result
	}
	return equalMessage(d.Result.Message, other.Result.Message)
}

func equalError(a, b *RunError) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func equalMessage(a, b core.Message) bool {
	if a.Role != b.Role {
		return false
	}
	if len(a.Parts) == 0 && len(b.Parts) == 0 {
		return true
	}
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(left) == string(right)
}
