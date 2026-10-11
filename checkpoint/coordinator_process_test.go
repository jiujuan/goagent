package checkpoint_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

const snapshotConsistencyProcessHelper = "GOAGENT_SNAPSHOT_PROCESS_HELPER"

type snapshotConsistencyProcessReady struct {
	Claim        checkpoint.Claim `json:"claim"`
	CheckpointID string           `json:"checkpoint_id"`
	Revision     uint64           `json:"revision"`
}

type snapshotConsistencyProcess struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stderr  *bytes.Buffer
	cancel  context.CancelFunc
	wait    sync.Once
	waitErr error
}

func TestSnapshotConsistencyFileProcessHelper(t *testing.T) {
	if os.Getenv(snapshotConsistencyProcessHelper) != "1" {
		return
	}

	dir := os.Getenv("GOAGENT_SNAPSHOT_PROCESS_DIR")
	threadID := os.Getenv("GOAGENT_SNAPSHOT_PROCESS_THREAD")
	owner := os.Getenv("GOAGENT_SNAPSHOT_PROCESS_OWNER")
	lease, err := time.ParseDuration(os.Getenv("GOAGENT_SNAPSHOT_PROCESS_LEASE"))
	if err != nil {
		t.Fatal(err)
	}
	store := newFileStore(t, dir)
	begun, err := store.BeginRun(context.Background(), checkpoint.BeginRunRequest{
		ThreadID: threadID,
		Owner:    owner,
		Lease:    lease,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotConsistencyProcessCheckpoint(t, begun.Checkpoint)

	if err := json.NewEncoder(os.Stdout).Encode(snapshotConsistencyProcessReady{
		Claim:        begun.Claim,
		CheckpointID: begun.Checkpoint.ID,
		Revision:     begun.Checkpoint.Revision,
	}); err != nil {
		t.Fatal(err)
	}

	command, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if command != "release\n" {
		t.Fatalf("helper command = %q, want release", command)
	}
	if err := store.ReleaseClaim(context.Background(), begun.Claim); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotConsistencyFileCrossProcess(t *testing.T) {
	dir := t.TempDir()
	threadID := "cross-process"
	store := newFileStore(t, dir)
	if err := store.Save(context.Background(), snapshotConsistencyProcessCheckpoint(threadID)); err != nil {
		t.Fatal(err)
	}

	helper, ready := startSnapshotConsistencyProcess(t, dir, threadID, "child-owner", 5*time.Second)
	if ready.CheckpointID != "seed" || ready.Revision != 1 {
		t.Fatalf("helper loaded checkpoint = %s revision %d, want seed revision 1", ready.CheckpointID, ready.Revision)
	}

	competitor := newFileStore(t, dir)
	if _, err := competitor.BeginRun(context.Background(), checkpoint.BeginRunRequest{
		ThreadID: threadID,
		Owner:    "parent-conflict",
		Lease:    time.Second,
	}); !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("competing process error = %v, want ErrClaimConflict", err)
	}

	if _, err := fmt.Fprintln(helper.stdin, "release"); err != nil {
		t.Fatal(err)
	}
	if err := helper.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := helper.waitForExit(); err != nil {
		t.Fatalf("helper release failed: %v\nstderr:\n%s", err, helper.stderr.String())
	}

	winner, err := newFileStore(t, dir).BeginRun(context.Background(), checkpoint.BeginRunRequest{
		ThreadID: threadID,
		Owner:    "parent-winner",
		Lease:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if winner.Claim.FencingToken <= ready.Claim.FencingToken {
		t.Fatalf("winner fencing token = %d, want greater than %d", winner.Claim.FencingToken, ready.Claim.FencingToken)
	}
	if err := newFileStore(t, dir).ReleaseClaim(context.Background(), winner.Claim); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotConsistencyFileOwnerCrash(t *testing.T) {
	dir := t.TempDir()
	threadID := "owner-crash"
	store := newFileStore(t, dir)
	if err := store.Save(context.Background(), snapshotConsistencyProcessCheckpoint(threadID)); err != nil {
		t.Fatal(err)
	}

	helper, ready := startSnapshotConsistencyProcess(t, dir, threadID, "crashed-owner", 750*time.Millisecond)
	if _, err := newFileStore(t, dir).BeginRun(context.Background(), checkpoint.BeginRunRequest{
		ThreadID: threadID,
		Owner:    "early-owner",
		Lease:    time.Second,
	}); !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("pre-expiry takeover error = %v, want ErrClaimConflict", err)
	}

	if err := helper.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := helper.waitForExit(); err == nil {
		t.Fatal("killed helper exited successfully")
	}

	if remaining := time.Until(ready.Claim.ExpiresAt); remaining > 0 {
		timer := time.NewTimer(remaining + 10*time.Millisecond)
		defer timer.Stop()
		<-timer.C
	}

	winnerStore := newFileStore(t, dir)
	winner, err := winnerStore.BeginRun(context.Background(), checkpoint.BeginRunRequest{
		ThreadID: threadID,
		Owner:    "recovery-owner",
		Lease:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if winner.Claim.FencingToken <= ready.Claim.FencingToken {
		t.Fatalf("winner fencing token = %d, want greater than %d", winner.Claim.FencingToken, ready.Claim.FencingToken)
	}

	stale := snapshotConsistencyProcessCheckpoint(threadID)
	stale.ID = "stale-owner-commit"
	if _, err := winnerStore.Commit(context.Background(), checkpoint.CommitRequest{
		Checkpoint: stale,
		Claim:      &ready.Claim,
	}); !errors.Is(err, checkpoint.ErrStaleFencingToken) {
		t.Fatalf("stale owner Commit error = %v, want ErrStaleFencingToken", err)
	}
	if err := winnerStore.ReleaseClaim(context.Background(), winner.Claim); err != nil {
		t.Fatal(err)
	}
}

func startSnapshotConsistencyProcess(
	t *testing.T,
	dir, threadID, owner string,
	lease time.Duration,
) (*snapshotConsistencyProcess, snapshotConsistencyProcessReady) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestSnapshotConsistencyFileProcessHelper$")
	cmd.Env = append(os.Environ(),
		snapshotConsistencyProcessHelper+"=1",
		"GOAGENT_SNAPSHOT_PROCESS_DIR="+dir,
		"GOAGENT_SNAPSHOT_PROCESS_THREAD="+threadID,
		"GOAGENT_SNAPSHOT_PROCESS_OWNER="+owner,
		"GOAGENT_SNAPSHOT_PROCESS_LEASE="+lease.String(),
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		cancel()
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		cancel()
		t.Fatal(err)
	}

	process := &snapshotConsistencyProcess{
		cmd:    cmd,
		stdin:  stdin,
		stderr: &stderr,
		cancel: cancel,
	}
	t.Cleanup(process.cleanup)

	readyResult := make(chan struct {
		ready snapshotConsistencyProcessReady
		err   error
	}, 1)
	go func() {
		var ready snapshotConsistencyProcessReady
		err := json.NewDecoder(stdout).Decode(&ready)
		readyResult <- struct {
			ready snapshotConsistencyProcessReady
			err   error
		}{ready: ready, err: err}
	}()
	select {
	case result := <-readyResult:
		if result.err != nil {
			process.cleanup()
			t.Fatalf("decode helper readiness: %v\nstderr:\n%s", result.err, stderr.String())
		}
		return process, result.ready
	case <-time.After(5 * time.Second):
		process.cleanup()
		t.Fatalf("timed out waiting for helper readiness\nstderr:\n%s", stderr.String())
		return nil, snapshotConsistencyProcessReady{}
	}
}

func snapshotConsistencyProcessCheckpoint(threadID string) *checkpoint.Checkpoint {
	return &checkpoint.Checkpoint{
		ID:       "seed",
		ThreadID: threadID,
		State: core.State{
			Messages: []core.Message{{
				Role: core.RoleUser,
				Parts: []core.Part{
					core.Image{MIME: "image/png", Data: []byte("process-image")},
					core.ToolCall{ID: "call-1", Name: "inspect", Args: []byte(`{"value":"process"}`)},
				},
			}},
			KV: map[string]any{"nested": map[string]any{"value": "process"}},
		},
		FileSnapshot: map[string][]byte{"note.txt": []byte("process-file")},
	}
}

func assertSnapshotConsistencyProcessCheckpoint(t *testing.T, cp *checkpoint.Checkpoint) {
	t.Helper()
	if cp == nil || cp.ID != "seed" || cp.Revision != 1 {
		t.Fatalf("checkpoint = %+v, want seed revision 1", cp)
	}
	if got := cp.State.KV["nested"].(map[string]any)["value"]; got != "process" {
		t.Fatalf("nested state = %v, want process", got)
	}
	if got := string(cp.State.Messages[0].Parts[0].(core.Image).Data); got != "process-image" {
		t.Fatalf("image data = %q, want process-image", got)
	}
	if got := string(cp.State.Messages[0].Parts[1].(core.ToolCall).Args); got != `{"value":"process"}` {
		t.Fatalf("tool arguments = %q", got)
	}
	if got := string(cp.FileSnapshot["note.txt"]); got != "process-file" {
		t.Fatalf("file snapshot = %q, want process-file", got)
	}
}

func (p *snapshotConsistencyProcess) waitForExit() error {
	p.wait.Do(func() {
		p.waitErr = p.cmd.Wait()
		p.cancel()
	})
	return p.waitErr
}

func (p *snapshotConsistencyProcess) cleanup() {
	_ = p.stdin.Close()
	if p.cmd.Process != nil && p.cmd.ProcessState == nil {
		_ = p.cmd.Process.Kill()
	}
	_ = p.waitForExit()
}
