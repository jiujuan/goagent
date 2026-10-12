package checkpoint

import (
	"encoding/json"

	"github.com/jiujuan/goagent/core"
)

// freezeCheckpoint takes ownership of a checkpoint's snapshot data before it
// becomes visible in a store. A snapshottable active file handle is the source
// of truth; otherwise an existing FileSnapshot is retained. Externally managed
// file handles remain shared because their contents cannot be captured.
func freezeCheckpoint(cp *Checkpoint) (*Checkpoint, error) {
	out, err := cloneCheckpoint(cp)
	if err != nil || cp == nil {
		return out, err
	}
	if files, ok := cp.State.Files.(core.Snapshottable); ok {
		out.FileSnapshot = cloneFileSnapshot(files.Snapshot())
		out.State.Files = nil
	}
	return out, nil
}

// cloneCheckpoint returns a checkpoint whose mutable snapshot data is
// independent from cp.
func cloneCheckpoint(cp *Checkpoint) (*Checkpoint, error) {
	if cp == nil {
		return nil, nil
	}

	out := *cp
	state, err := core.CloneStateData(cp.State)
	if err != nil {
		return nil, err
	}
	out.State = state
	out.Pending = clonePending(cp.Pending)
	out.Pause = clonePause(cp.Pause)
	out.Completion, err = cloneCompletion(cp.Completion)
	if err != nil {
		return nil, err
	}
	out.FileSnapshot = cloneFileSnapshot(cp.FileSnapshot)
	return &out, nil
}

func clonePending(in *PendingHITL) *PendingHITL {
	if in == nil {
		return nil
	}
	out := *in
	if in.Pending == nil {
		return &out
	}
	out.Pending = make([]core.ToolCall, len(in.Pending))
	for i, call := range in.Pending {
		call.Args = json.RawMessage(cloneSnapshotBytes(call.Args))
		out.Pending[i] = call
	}
	return &out
}

func clonePause(in *Pause) *Pause {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

// cloneCompletion copies a terminal record. The result message is deep-copied
// because its parts carry mutable byte payloads; Err and Kind are strings, so the
// record itself stays shallow.
func cloneCompletion(in *Completion) (*Completion, error) {
	if in == nil {
		return nil, nil
	}
	out := *in
	if in.Result == nil {
		return &out, nil
	}
	result, err := core.CloneMessage(in.Result.Message)
	if err != nil {
		return nil, err
	}
	out.Result = &core.Result{Message: result}
	return &out, nil
}

func cloneFileSnapshot(in map[string][]byte) map[string][]byte {
	if in == nil {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for path, data := range in {
		out[path] = cloneSnapshotBytes(data)
	}
	return out
}

func cloneSnapshotBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneToolExecution(in ToolExecution) (ToolExecution, error) {
	out := in
	if in.Result == nil {
		return out, nil
	}
	result, err := core.CloneMessage(*in.Result)
	if err != nil {
		return ToolExecution{}, err
	}
	out.Result = &result
	return out, nil
}

func cloneToolExecutions(in []ToolExecution) ([]ToolExecution, error) {
	if in == nil {
		return nil, nil
	}
	out := make([]ToolExecution, len(in))
	for i, execution := range in {
		cloned, err := cloneToolExecution(execution)
		if err != nil {
			return nil, err
		}
		out[i] = cloned
	}
	return out, nil
}
