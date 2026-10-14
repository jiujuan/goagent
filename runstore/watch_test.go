package runstore

// SM-12 (ADR-0034 D7): the polling task watch. The poller is stepped through a wake channel
// rather than a timer, so a pass only happens when the test says so and a notification is
// never counted before the pass that could have produced it has finished.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
)

type runWatchHandle struct {
	p      *RunPoller
	wake   chan time.Time
	out    <-chan RunNotification
	cancel context.CancelFunc
}

func watchRuns(t *testing.T, store Store, buffer int, ids ...string) *runWatchHandle {
	t.Helper()
	wake := make(chan time.Time)
	p, err := NewRunPoller(store, WithRunWake(wake), WithRunNotificationBuffer(buffer))
	if err != nil {
		t.Fatalf("NewRunPoller: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	out, err := p.WatchRuns(ctx, RunWatchRequest{RunIDs: ids})
	if err != nil {
		cancel()
		t.Fatalf("WatchRuns: %v", err)
	}
	h := &runWatchHandle{p: p, wake: wake, out: out, cancel: cancel}
	t.Cleanup(h.cancel)
	return h
}

// tick runs exactly one poller pass and waits for it to finish, delivery included, so no
// assertion below can be racing with a pass still in flight.
func (h *runWatchHandle) tick(t *testing.T) {
	t.Helper()
	h.wait(t, h.p.Stats().Passes+1)
}

func (h *runWatchHandle) wait(t *testing.T, want int) {
	t.Helper()
	select {
	case h.wake <- time.Now():
	case <-time.After(2 * time.Second):
		t.Fatal("the poller stopped taking wakes")
	}
	deadline := time.After(2 * time.Second)
	for h.p.Stats().Passes < want {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatalf("the poller never finished the pass it was woken for: %d of %d", h.p.Stats().Passes, want)
		}
	}
}

func (h *runWatchHandle) collect(t *testing.T) []RunNotification {
	t.Helper()
	var got []RunNotification
	for {
		select {
		case note, open := <-h.out:
			if !open {
				t.Fatal("the notification channel closed while the watch was expected to run")
			}
			got = append(got, note)
		default:
			return got
		}
	}
}

func (h *runWatchHandle) pass(t *testing.T) []RunNotification {
	t.Helper()
	h.tick(t)
	return h.collect(t)
}

func noteVersions(notes []RunNotification) []uint64 {
	out := make([]uint64, 0, len(notes))
	for _, note := range notes {
		out = append(out, note.Summary.Version)
	}
	return out
}

// advance moves a record one step forward as its owner would, and returns the version the
// store accepted.
func advance(t *testing.T, s Store, runID string, change func(*RunRecord)) uint64 {
	t.Helper()
	record, err := s.Get(context.Background(), runID)
	if err != nil {
		t.Fatalf("get %q: %v", runID, err)
	}
	change(&record)
	updated, err := s.CompareAndSwap(context.Background(), runID, record.Version, record)
	if err != nil {
		t.Fatalf("cas %q: %v", runID, err)
	}
	return updated.Version
}

func TestWatchRunVersionChanges(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedRun(t, b.store, runSpec{id: "w-1", thread: "alpha", status: StatusQueued, at: runAt(1)})
			seedRun(t, b.store, runSpec{id: "w-2", thread: "beta", status: StatusQueued, at: runAt(2)})

			h := watchRuns(t, b.store, 4, "w-1", "w-2")
			if got := h.pass(t); len(got) != 0 {
				t.Fatalf("the baseline pass notified %v", noteVersions(got))
			}

			// A worker claims the run: identity, state and version all move.
			version := advance(t, b.store, "w-1", func(r *RunRecord) {
				r.Status = StatusRunning
				r.WorkerID = "worker-1"
				r.WorkerEpoch = 1
				r.WorkerLeaseUntil = runAt(3).Add(time.Minute)
			})
			got := h.pass(t)
			if len(got) != 1 || got[0].Summary.Version != version {
				t.Fatalf("claim notified %v, want the accepted version %d", noteVersions(got), version)
			}
			if got[0].RunID != "w-1" || got[0].Summary.Status != StatusRunning || got[0].Summary.WorkerID != "worker-1" {
				t.Errorf("notification = %+v", got[0].Summary)
			}
			if got[0].Coalesced != 0 {
				t.Errorf("a consumer that keeps up should not see merged hints: %+v", got[0])
			}

			// Another run moving is this watch's business too, and the order follows the
			// watched ids rather than whichever record changed.
			advance(t, b.store, "w-2", func(r *RunRecord) { r.Status = StatusRunning })

			// A heartbeat is a real change of the record: it is reported, because the hint
			// says "read again" and liveness is part of what a reader would want.
			version = advance(t, b.store, "w-1", func(r *RunRecord) {
				r.HeartbeatAt = runAt(4)
				r.WorkerLeaseUntil = runAt(4).Add(time.Minute)
			})
			got = h.pass(t)
			if len(got) != 2 || got[0].RunID != "w-1" || got[0].Summary.Version != version {
				t.Fatalf("heartbeat pass = %v / %v", got, noteVersions(got))
			}
			if got[1].RunID != "w-2" {
				t.Fatalf("watched order was not kept: %+v", got[1])
			}

			// Settling the run, and then retrying the same terminal write, is one notification
			// only: the retry did not move the version.
			terminal := func(r *RunRecord) {
				r.Status = StatusSucceeded
				r.FinishReason = FinishReasonCompleted
				r.Result = settledResult("the answer")
			}
			version = advance(t, b.store, "w-1", terminal)
			if got := h.pass(t); len(got) != 1 || got[0].Summary.Version != version {
				t.Fatalf("terminal write = %v", noteVersions(got))
			}
			if _, err := b.store.CompareAndSwap(context.Background(), "w-1", version, func() RunRecord {
				record, err := b.store.Get(context.Background(), "w-1")
				if err != nil {
					t.Fatalf("get: %v", err)
				}
				terminal(&record)
				return record
			}()); err != nil {
				t.Fatalf("idempotent terminal retry: %v", err)
			}
			if got := h.pass(t); len(got) != 0 {
				t.Fatalf("a settled record notified again: %v", noteVersions(got))
			}
		})
	}
}

func TestWatchRunAppearsAfterTheWatch(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			// A client that submitted work through a queue knows the RunID before the record
			// exists. Watching it early is the point; nothing is reported while there is
			// nothing to report.
			h := watchRuns(t, b.store, 2, "later")
			if got := h.pass(t); len(got) != 0 {
				t.Fatalf("a record that does not exist yet notified %v", noteVersions(got))
			}
			seedRun(t, b.store, runSpec{id: "later", thread: "alpha", status: StatusQueued, at: runAt(1)})
			got := h.pass(t)
			if len(got) != 1 || got[0].RunID != "later" || got[0].Summary.Version != 1 {
				t.Fatalf("the record appearing: %v", noteVersions(got))
			}
			if got[0].Summary.Status != StatusQueued {
				t.Errorf("notification = %+v", got[0].Summary)
			}
		})
	}
}

func TestWatchRunSlowConsumerMerges(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedRun(t, b.store, runSpec{id: "s-1", thread: "alpha", status: StatusQueued, at: runAt(1)})
			// One slot, and nobody reading.
			h := watchRuns(t, b.store, 1, "s-1")

			h.tick(t) // baseline
			h.tick(t) // nothing changed, nothing queued
			v2 := advance(t, b.store, "s-1", func(r *RunRecord) { r.Status = StatusRunning })
			h.tick(t) // delivered into the one slot
			v3 := advance(t, b.store, "s-1", func(r *RunRecord) { r.HeartbeatAt = runAt(2) })
			h.tick(t) // cannot be delivered: the consumer has not read
			v4 := advance(t, b.store, "s-1", func(r *RunRecord) {
				r.Status = StatusCancelling
				r.CancelRequestedAt = runAt(3)
			})
			h.tick(t) // replaces the waiting hint, which now stands for v3 as well

			got := h.collect(t)
			if len(got) != 1 || got[0].Summary.Version != v2 || got[0].Coalesced != 0 {
				t.Fatalf("buffered hint = %v", noteVersions(got))
			}
			h.tick(t) // the slot is free, so the newest state goes out
			late := h.collect(t)
			if len(late) != 1 {
				t.Fatalf("waiting hints delivered %v, want one merged notification", noteVersions(late))
			}
			if late[0].Summary.Version != v4 || late[0].Coalesced != 1 {
				t.Errorf("merged hint = %+v, want version %d standing in for %d", late[0], v4, v3)
			}

			// A consumer that catches up sees each later state on its own.
			v5 := advance(t, b.store, "s-1", func(r *RunRecord) { r.HeartbeatAt = runAt(5) })
			next := h.pass(t)
			if len(next) != 1 || next[0].Summary.Version != v5 || next[0].Coalesced != 0 {
				t.Fatalf("after catching up: %v", noteVersions(next))
			}
		})
	}
}

func TestWatchRunNotificationCarriesNoPayload(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedRun(t, b.store, runSpec{id: "p-1", thread: "alpha", status: StatusRunning, at: runAt(1)})
			h := watchRuns(t, b.store, 2, "p-1")
			h.pass(t) // baseline

			advance(t, b.store, "p-1", func(r *RunRecord) {
				r.Status = StatusSucceeded
				r.FinishReason = FinishReasonCompleted
				r.Result = settledResult(runPayloadMarker + " answer")
			})
			got := h.pass(t)
			if len(got) != 1 {
				t.Fatalf("the settling write notified %v", noteVersions(got))
			}
			encoded, err := json.Marshal(got[0])
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if strings.Contains(string(encoded), runPayloadMarker) {
				t.Fatalf("a notification carried a result body: %s", encoded)
			}
			if !got[0].Summary.HasResult {
				t.Error("the hint should say a result exists even though it does not carry it")
			}
		})
	}
}

// flakyRunStore answers reads until a test says it cannot.
type flakyRunStore struct {
	Store
	failing atomic.Bool
}

func (f *flakyRunStore) Get(ctx context.Context, runID string) (RunRecord, error) {
	if f.failing.Load() {
		return RunRecord{}, errors.New("runstore test: store unavailable")
	}
	return f.Store.Get(ctx, runID)
}

func TestWatchRunKeepsWatchingThroughOutage(t *testing.T) {
	base := NewMemory()
	store := &flakyRunStore{Store: base}
	seedRun(t, store, runSpec{id: "o-1", thread: "alpha", status: StatusQueued, at: runAt(1)})
	h := watchRuns(t, store, 2, "o-1")
	h.tick(t) // baseline

	store.failing.Store(true)
	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("a failed read invented a notification: %v", noteVersions(got))
	}
	if stats := h.p.Stats(); stats.Outages == 0 || stats.LastOutage == nil {
		t.Fatalf("an outage went unreported: %+v", stats)
	}

	// The record moved during the outage and the watch reports it once the store answers. The
	// write goes through the store behind the failing view, which only refuses reads.
	advance(t, base, "o-1", func(r *RunRecord) { r.Status = StatusRunning })
	store.failing.Store(false)
	got := h.pass(t)
	if len(got) != 1 || got[0].Summary.Status != StatusRunning {
		t.Fatalf("after the outage: %v", noteVersions(got))
	}
	if stats := h.p.Stats(); stats.Looks == 0 {
		t.Fatalf("the poller never reported a read: %+v", stats)
	}
}

func TestWatchRunEndsWithContext(t *testing.T) {
	for _, store := range runQueryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			seedRun(t, b.store, runSpec{id: "e-1", thread: "alpha", status: StatusQueued, at: runAt(1)})
			h := watchRuns(t, b.store, 2, "e-1")
			h.pass(t)
			if stats := h.p.Stats(); stats.Looks == 0 || stats.ActiveWatches != 1 {
				t.Fatalf("stats while watching = %+v", stats)
			}

			h.cancel()
			deadline := time.After(2 * time.Second)
			for {
				select {
				case _, open := <-h.out:
					if !open {
						goto drained
					}
				case <-deadline:
					t.Fatal("the notification channel stayed open after its context ended")
				}
			}
		drained:
			for h.p.Stats().ActiveWatches != 0 {
				select {
				case <-time.After(10 * time.Millisecond):
				case <-deadline:
					t.Fatal("the watch leaked: the poller still counts an active walk")
				}
			}
		})
	}
}

func TestWatchRunValidatesRequest(t *testing.T) {
	m := NewMemory()
	p, err := NewRunPoller(m)
	if err != nil {
		t.Fatalf("NewRunPoller: %v", err)
	}
	ctx := context.Background()

	if _, err := p.WatchRuns(ctx, RunWatchRequest{}); !errors.Is(err, ErrInvalidQuery) {
		t.Errorf("no runs: err = %v, want ErrInvalidQuery", err)
	}
	if _, err := p.WatchRuns(ctx, RunWatchRequest{RunIDs: []string{"ok", "not a run"}}); err == nil {
		t.Error("a run id this store cannot name was accepted")
	}
	if _, err := p.WatchRuns(nil, RunWatchRequest{RunIDs: []string{"ok"}}); err == nil {
		t.Error("a watch with no context was accepted")
	}
	if _, err := NewRunPoller(nil); err == nil {
		t.Error("a nil store was accepted")
	}
	if _, err := NewRunPoller(m, WithRunPollInterval(0)); err == nil {
		t.Error("a zero poll interval was accepted")
	}
	if _, err := NewRunPoller(m, WithRunNotificationBuffer(0)); err == nil {
		t.Error("a zero notification buffer was accepted")
	}
	// The default interval is a real wait, not a spin.
	if _, err := NewRunPoller(m); err != nil {
		t.Errorf("default poller: %v", err)
	}
}

// TestWatchHeartbeatMakesNoCheckpointNoise is the two-stores-one-directory case: a heartbeat
// is a task-record write, and it must never look like a new snapshot to a checkpoint watcher
// (ADR-0034 D2, D6, D7).
func TestWatchHeartbeatMakesNoCheckpointNoise(t *testing.T) {
	dir := t.TempDir()
	checkpoints, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatalf("checkpoint.NewFile: %v", err)
	}
	runs := newFileStore(t, dir)
	ctx := context.Background()

	if _, err := checkpoints.Commit(ctx, checkpoint.CommitRequest{Checkpoint: &checkpoint.Checkpoint{
		ID: "c1", ThreadID: "alpha", Step: 1, RunID: "r-1", CreatedAt: runAt(1),
	}}); err != nil {
		t.Fatalf("commit c1: %v", err)
	}
	seedRun(t, runs, runSpec{id: "r-1", thread: "alpha", status: StatusRunning, at: runAt(1)})
	advance(t, runs, "r-1", func(r *RunRecord) {
		r.WorkerID = "worker-1"
		r.WorkerEpoch = 1
	})

	// Both watchers are stepped by hand, so "no notification" means "no pass produced one".
	checkpointWake := make(chan time.Time)
	checkpointPoller, err := checkpoint.NewCheckpointPoller(checkpoints, checkpoint.WithCheckpointWake(checkpointWake))
	if err != nil {
		t.Fatalf("NewCheckpointPoller: %v", err)
	}
	snapshotCtx, cancelSnapshots := context.WithCancel(ctx)
	t.Cleanup(cancelSnapshots)
	snapshotEvents, err := checkpointPoller.WatchCheckpoints(snapshotCtx, checkpoint.CheckpointWatchRequest{ThreadIDs: []string{"alpha"}})
	if err != nil {
		t.Fatalf("WatchCheckpoints: %v", err)
	}
	steps := func(t *testing.T) {
		t.Helper()
		want := checkpointPoller.Stats().Passes + 1
		select {
		case checkpointWake <- time.Now():
		case <-time.After(2 * time.Second):
			t.Fatal("the checkpoint poller stopped taking wakes")
		}
		deadline := time.After(2 * time.Second)
		for checkpointPoller.Stats().Passes < want {
			select {
			case <-time.After(time.Millisecond):
			case <-deadline:
				t.Fatal("the checkpoint poller never finished the pass it was woken for")
			}
		}
	}
	take := func() []checkpoint.CheckpointNotification {
		var got []checkpoint.CheckpointNotification
		for {
			select {
			case note, open := <-snapshotEvents:
				if !open {
					t.Fatal("the snapshot channel closed early")
				}
				got = append(got, note)
			default:
				return got
			}
		}
	}

	steps(t) // baseline: the snapshot that is already the head
	if got := take(); len(got) != 0 {
		t.Fatalf("the baseline pass notified %v", got)
	}

	runWatch := watchRuns(t, runs, 8, "r-1")
	if got := runWatch.pass(t); len(got) != 0 {
		t.Fatalf("the run baseline notified %v", noteVersions(got))
	}

	for i := 1; i <= 3; i++ {
		advance(t, runs, "r-1", func(r *RunRecord) {
			r.HeartbeatAt = runAt(1 + i)
			r.WorkerLeaseUntil = runAt(1 + i).Add(time.Minute)
		})
		runWatch.tick(t)
		steps(t)
		if got := take(); len(got) != 0 {
			t.Fatalf("heartbeat %d produced a checkpoint notification: %v", i, got)
		}
	}
	// The same heartbeats are exactly what the task watch does report.
	if got := runWatch.collect(t); len(got) != 3 {
		t.Fatalf("the task watch reported %v of the three heartbeats", noteVersions(got))
	}

	// And a real snapshot still reaches the snapshot watcher, so the silence above is not a
	// watcher that cannot see anything.
	if _, err := checkpoints.Commit(ctx, checkpoint.CommitRequest{Checkpoint: &checkpoint.Checkpoint{
		ID: "c2", ThreadID: "alpha", ParentID: "c1", ParentRef: &checkpoint.Ref{ThreadID: "alpha", CheckpointID: "c1"},
		Step: 2, RunID: "r-1", CreatedAt: runAt(9),
	}}); err != nil {
		t.Fatalf("commit c2: %v", err)
	}
	steps(t)
	got := take()
	if len(got) != 1 || got[0].Metadata.Ref.CheckpointID != "c2" {
		t.Fatalf("after a real commit: %v", got)
	}
	if _, err := runs.Get(ctx, "r-1"); err != nil {
		t.Fatalf("the run record is still readable: %v", err)
	}
}
