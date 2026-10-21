package agent

import (
	"errors"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
)

func TestStepSnapshotNoStoreDoesNotFail(t *testing.T) {
	loop := &AgentLoop{}
	rc := &RunContext{}
	snapshot := &checkpoint.StepSnapshot{Step: 2, Advertised: []string{"a"}}
	if err := loop.checkpoint(rc, commitOptions{Step: 2, Snapshot: snapshot}); err != nil {
		t.Fatalf("snapshot without store: %v", err)
	}
	if err := loop.checkpoint(rc, commitOptions{
		Step: 2, Snapshot: snapshot, Pending: &checkpoint.PendingHITL{Step: 2},
	}); !errors.Is(err, ErrCheckpointUnavailable) {
		t.Fatalf("pending without store error = %v, want ErrCheckpointUnavailable", err)
	}
	if err := loop.checkpoint(rc, commitOptions{
		Step: 2, Snapshot: snapshot, Pause: &checkpoint.Pause{Phase: "before_tool"},
	}); !errors.Is(err, ErrCheckpointUnavailable) {
		t.Fatalf("pause without store error = %v, want ErrCheckpointUnavailable", err)
	}
}
