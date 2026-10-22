package checkpoint

import (
	"encoding/json"
	"testing"

	"github.com/jiujuan/goagent/core"
)

// TestInFlightCloneIsolatesArgs proves a stored batch cannot be rewritten from
// the caller's copy: the tool arguments are byte payloads, and the ledger the
// recovery path reads has to say what was committed, not what the caller later
// decided to write into the same slice.
func TestInFlightCloneIsolatesArgs(t *testing.T) {
	cp := inFlightStoredFixture("thread", "cp")
	cloned, err := cloneCheckpoint(cp)
	if err != nil {
		t.Fatalf("cloneCheckpoint: %v", err)
	}
	// A value copy, not a slice header: the point of the test is that the two
	// batches do not share bytes, so the expectation must survive aliasing.
	original := string(cp.InFlight.Calls[0].Args)
	copy(cloned.InFlight.Calls[0].Args, []byte(`{"query":"xxxxx"}`))
	if string(cp.InFlight.Calls[0].Args) != original {
		t.Fatalf("writing the clone changed the source batch: %s", cp.InFlight.Calls[0].Args)
	}
	if string(cloned.InFlight.Calls[0].Args) == original {
		t.Fatal("the clone kept the source arguments, so nothing was written to prove isolation")
	}

	cloned.InFlight.Calls[0].Name = "write"
	if cp.InFlight.Calls[0].Name != "search" {
		t.Fatalf("the clone shares the source call entries: %q", cp.InFlight.Calls[0].Name)
	}

	cloned.InFlight.Calls = append(cloned.InFlight.Calls, core.ToolCall{ID: "call-2", Name: "search"})
	if len(cp.InFlight.Calls) != 1 {
		t.Fatalf("appending to the clone changed the source batch: %d calls", len(cp.InFlight.Calls))
	}
}

// inFlightStoredFixture is the internal-package twin of the contract test's
// legal record: it needs the same shape but must stay usable from package
// checkpoint tests.
func inFlightStoredFixture(threadID, id string) *Checkpoint {
	call := core.ToolCall{ID: "call-1", Name: "search", Args: json.RawMessage(`{"query":"first"}`)}
	return &Checkpoint{
		ID:       id,
		ThreadID: threadID,
		Step:     2,
		State: core.State{Messages: []core.Message{
			core.UserText("find something"),
			{Role: core.RoleAssistant, Parts: []core.Part{call}},
		}},
		Snapshot: &StepSnapshot{
			Step:           2,
			Advertised:     []string{"search"},
			ToolDigest:     "tool-digest",
			SettingsDigest: "settings-digest",
			Model:          "test-model",
		},
		InFlight: &InFlightBatch{Step: 2, Calls: []core.ToolCall{call}},
	}
}

func TestInFlightFreezeKeepsSourceIntact(t *testing.T) {
	cp := inFlightStoredFixture("thread", "cp")
	frozen, err := freezeCheckpoint(cp)
	if err != nil {
		t.Fatalf("freezeCheckpoint: %v", err)
	}
	frozen.InFlight.Calls[0].Name = "write"
	frozen.Snapshot.Advertised[0] = "write"
	if cp.InFlight.Calls[0].Name != "search" {
		t.Fatalf("freeze handed out the source batch: %q", cp.InFlight.Calls[0].Name)
	}
	if cp.Snapshot.Advertised[0] != "search" {
		t.Fatalf("freeze shared the advertised list: %v", cp.Snapshot.Advertised)
	}
}
