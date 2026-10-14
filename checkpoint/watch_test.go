package checkpoint

// SM-12 (ADR-0034 D7): the polling checkpoint watch. These tests step the poller through a
// wake channel instead of a timer, so every pass is a test decision rather than a wait, and a
// notification is only ever counted after the pass that could have produced it has finished.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type watchHandle struct {
	p      *CheckpointPoller
	wake   chan time.Time
	out    <-chan CheckpointNotification
	cancel context.CancelFunc
}

// watchWith starts one poller over this backend's store and one walk over the given threads.
func watchWith(t *testing.T, b queryBackend, buffer int, threads ...string) *watchHandle {
	t.Helper()
	wake := make(chan time.Time)
	p, err := NewCheckpointPoller(b.durable,
		WithCheckpointWake(wake),
		WithCheckpointNotificationBuffer(buffer),
	)
	if err != nil {
		t.Fatalf("NewCheckpointPoller: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	out, err := p.WatchCheckpoints(ctx, CheckpointWatchRequest{ThreadIDs: threads})
	if err != nil {
		cancel()
		t.Fatalf("WatchCheckpoints: %v", err)
	}
	h := &watchHandle{p: p, wake: wake, out: out, cancel: cancel}
	t.Cleanup(h.cancel)
	return h
}

// tick runs exactly one poller pass and waits for it to finish, delivery included, so no
// assertion below can be racing with a pass still in flight.
func (h *watchHandle) tick(t *testing.T) {
	t.Helper()
	want := h.p.Stats().Passes + 1
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

func (h *watchHandle) collect(t *testing.T) []CheckpointNotification {
	t.Helper()
	var got []CheckpointNotification
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

func (h *watchHandle) pass(t *testing.T) []CheckpointNotification {
	t.Helper()
	h.tick(t)
	return h.collect(t)
}

func watchIDs(notes []CheckpointNotification) []string {
	out := make([]string, 0, len(notes))
	for _, note := range notes {
		out = append(out, note.Metadata.Ref.CheckpointID)
	}
	return out
}

func TestWatchNewRevisionsOnly(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)

			// The thread already has one snapshot when the watch starts, and a second one is
			// committed while nobody has looked yet.
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "w1", runID: "run-w1", at: queryAt(60 * time.Second)})
			h := watchWith(t, b, 4, "alpha")
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "w2", runID: "run-w2", at: queryAt(120 * time.Second)})

			// The first pass takes the baseline: the newest snapshot a watch has not seen yet
			// is not news to a client that reads current state when it reconnects.
			if got := h.pass(t); len(got) != 0 {
				t.Fatalf("the baseline pass notified %v", watchIDs(got))
			}

			// A commit that does not move this thread is not this watch's business.
			queryCommit(t, b, checkpointSpec{thread: "beta", id: "x1", runID: "run-x1", at: queryAt(120 * time.Second)})
			if got := h.pass(t); len(got) != 0 {
				t.Fatalf("another thread's commit notified %v", watchIDs(got))
			}

			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "w3", runID: "run-w3", at: queryAt(180 * time.Second)})
			got := h.pass(t)
			if len(got) != 1 || got[0].Metadata.Ref.CheckpointID != "w3" {
				t.Fatalf("commit on the watched thread notified %v, want only w3", watchIDs(got))
			}
			if got[0].ThreadID != "alpha" || got[0].Coalesced != 0 {
				t.Errorf("notification = %+v", got[0])
			}
			if got[0].Metadata.RunID != "run-w3" || got[0].Metadata.Revision == 0 {
				t.Errorf("notification carries no identity: %+v", got[0].Metadata)
			}

			// A second look at the same head revision is not a second notification: the walk
			// remembers what it already delivered by checkpoint and revision.
			if got := h.pass(t); len(got) != 0 {
				t.Fatalf("the same snapshot notified again: %v", watchIDs(got))
			}

			// A pause and a terminal record are commits like any other: the hint says look
			// again, and the summary carries what changed in shape.
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "w4", runID: "run-w4", paused: true, at: queryAt(240 * time.Second)})
			got = h.pass(t)
			if len(got) != 1 || got[0].Metadata.Ref.CheckpointID != "w4" || !got[0].Metadata.Paused {
				t.Fatalf("paused commit: %v", watchIDs(got))
			}
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "w5", runID: "run-w5", kind: CompletionCompleted, at: queryAt(300 * time.Second)})
			got = h.pass(t)
			if len(got) != 1 || got[0].Metadata.CompletionKind != CompletionCompleted {
				t.Fatalf("terminal commit: %v, %+v", watchIDs(got), got)
			}
		})
	}
}

func TestWatchFirstCommitOfAThread(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			// A thread with nothing committed is a watchable thread: its first snapshot is the
			// change, and the watch does not decide it is stale or finished.
			h := watchWith(t, b, 2, "later")
			if got := h.pass(t); len(got) != 0 {
				t.Fatalf("a thread with no snapshots notified %v", watchIDs(got))
			}
			queryCommit(t, b, checkpointSpec{thread: "later", id: "first", runID: "run-first", at: queryAt(60 * time.Second)})
			got := h.pass(t)
			if len(got) != 1 || got[0].Metadata.Ref.CheckpointID != "first" {
				t.Fatalf("first commit: %v", watchIDs(got))
			}
		})
	}
}

func TestWatchMergesSlowConsumer(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "m1", runID: "run-m1", at: queryAt(60 * time.Second)})
			// One slot and nobody reading.
			h := watchWith(t, b, 1, "alpha")

			h.tick(t) // baseline
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "m2", runID: "run-m2", at: queryAt(120 * time.Second)})
			h.tick(t) // delivered into the one slot
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "m3", runID: "run-m3", at: queryAt(180 * time.Second)})
			h.tick(t) // cannot be delivered
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "m4", runID: "run-m4", at: queryAt(240 * time.Second)})
			h.tick(t) // replaces the waiting hint, which now stands for m3 as well

			got := h.collect(t)
			if len(got) != 1 || got[0].Metadata.Ref.CheckpointID != "m2" || got[0].Coalesced != 0 {
				t.Fatalf("buffered hint = %v", watchIDs(got))
			}
			h.tick(t) // the slot is free, so the newest state goes out
			late := h.collect(t)
			if len(late) != 1 {
				t.Fatalf("waiting hints delivered %v, want one merged notification", watchIDs(late))
			}
			if late[0].Metadata.Ref.CheckpointID != "m4" || late[0].Coalesced != 1 {
				t.Errorf("merged hint = %+v, want m4 standing in for m3", late[0])
			}

			// A consumer that catches up gets each later move on its own.
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "m5", runID: "run-m5", at: queryAt(300 * time.Second)})
			next := h.pass(t)
			if len(next) != 1 || next[0].Metadata.Ref.CheckpointID != "m5" || next[0].Coalesced != 0 {
				t.Fatalf("after catching up: %v", watchIDs(next))
			}
		})
	}
}

// blindCheckpointer implements Checkpointer and nothing else, which is the case an optional
// view has to refuse out loud.
type blindCheckpointer struct {
	inner *Memory
}

func (b blindCheckpointer) Save(ctx context.Context, cp *Checkpoint) error {
	return b.inner.Save(ctx, cp)
}
func (b blindCheckpointer) Load(ctx context.Context, threadID, id string) (*Checkpoint, error) {
	return b.inner.Load(ctx, threadID, id)
}
func (b blindCheckpointer) Latest(ctx context.Context, threadID string) (*Checkpoint, error) {
	return b.inner.Latest(ctx, threadID)
}
func (b blindCheckpointer) History(ctx context.Context, threadID string) ([]*Checkpoint, error) {
	return b.inner.History(ctx, threadID)
}

var _ Checkpointer = blindCheckpointer{}

func TestWatchRequiresMetadataReader(t *testing.T) {
	if _, err := NewCheckpointPoller(blindCheckpointer{inner: NewMemory()}); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("a store with no metadata view: err = %v, want ErrCapabilityUnavailable", err)
	}
	if _, err := NewCheckpointPoller(nil); err == nil {
		t.Fatal("a nil store was accepted")
	}
	// Both durable stores answer the view, so a configured store is never refused.
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			if _, err := NewCheckpointPoller(b.durable); err != nil {
				t.Fatalf("NewCheckpointPoller(%s): %v", store.name, err)
			}
		})
	}
}

func TestWatchValidatesRequest(t *testing.T) {
	b := openMemoryQueryBackend(t)
	p, err := NewCheckpointPoller(b.durable)
	if err != nil {
		t.Fatalf("NewCheckpointPoller: %v", err)
	}
	ctx := context.Background()

	if _, err := p.WatchCheckpoints(ctx, CheckpointWatchRequest{}); !errors.Is(err, ErrInvalidQuery) {
		t.Errorf("no threads: err = %v, want ErrInvalidQuery", err)
	}
	if _, err := p.WatchCheckpoints(ctx, CheckpointWatchRequest{ThreadIDs: []string{"ok", "not/allowed"}}); err == nil {
		t.Error("a thread id this store cannot name was accepted")
	}
	if _, err := p.WatchCheckpoints(nil, CheckpointWatchRequest{ThreadIDs: []string{"ok"}}); err == nil {
		t.Error("a watch with no context was accepted")
	}

	// The knobs are checked where they are set, so a misconfigured poller never starts.
	if _, err := NewCheckpointPoller(b.durable, WithCheckpointPollInterval(0)); err == nil {
		t.Error("a zero poll interval was accepted")
	}
	if _, err := NewCheckpointPoller(b.durable, WithCheckpointNotificationBuffer(0)); err == nil {
		t.Error("a zero notification buffer was accepted")
	}
}

func TestWatchEndsWithContext(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "e1", runID: "run-e1", at: queryAt(60 * time.Second)})
			h := watchWith(t, b, 2, "alpha")
			h.pass(t) // baseline, and proof the poller is reading

			if stats := h.p.Stats(); stats.Looks == 0 || stats.ActiveWatches != 1 {
				t.Fatalf("stats while watching = %+v", stats)
			}
			h.cancel()

			// The channel closes, so a consumer's range loop ends instead of hanging, and the
			// walk itself goes away rather than keeping a goroutine and a timer.
			deadline := time.After(2 * time.Second)
			for {
				select {
				case _, open := <-h.out:
					if !open {
						goto closed
					}
					continue
				case <-deadline:
					t.Fatal("the notification channel stayed open after its context ended")
				}
			}
		closed:
			for {
				if h.p.Stats().ActiveWatches == 0 {
					break
				}
				select {
				case <-time.After(10 * time.Millisecond):
				case <-deadline:
					t.Fatal("the watch leaked: the poller still counts an active walk")
				}
			}
		})
	}
}

// flakyStore answers metadata reads until a test says it cannot.
type flakyStore struct {
	*Memory
	failing atomic.Bool
}

func (f *flakyStore) ReadSessionHead(ctx context.Context, threadID string) (CheckpointMetadata, bool, error) {
	if f.failing.Load() {
		return CheckpointMetadata{}, false, errors.New("checkpoint test: store unavailable")
	}
	return f.Memory.ReadSessionHead(ctx, threadID)
}

func TestWatchKeepsWatchingThroughOutage(t *testing.T) {
	m := NewMemory()
	store := &flakyStore{Memory: m}
	if _, err := m.Commit(context.Background(), CommitRequest{Checkpoint: &Checkpoint{
		ID: "o1", ThreadID: "alpha", Step: 1, RunID: "run-o1", CreatedAt: queryAt(60 * time.Second),
	}}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	wake := make(chan time.Time)
	p, err := NewCheckpointPoller(store, WithCheckpointWake(wake))
	if err != nil {
		t.Fatalf("NewCheckpointPoller: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	out, err := p.WatchCheckpoints(ctx, CheckpointWatchRequest{ThreadIDs: []string{"alpha"}})
	if err != nil {
		t.Fatalf("WatchCheckpoints: %v", err)
	}
	step := func() {
		want := p.Stats().Passes + 1
		select {
		case wake <- time.Now():
		case <-time.After(2 * time.Second):
			t.Fatal("the poller stopped taking wakes")
		}
		deadline := time.After(2 * time.Second)
		for p.Stats().Passes < want {
			select {
			case <-time.After(time.Millisecond):
			case <-deadline:
				t.Fatal("the poller never finished the pass it was woken for")
			}
		}
	}

	step() // baseline
	store.failing.Store(true)
	step()
	if stats := p.Stats(); stats.Outages == 0 || stats.LastOutage == nil {
		t.Fatalf("an outage went unreported: %+v", stats)
	}
	select {
	case note := <-out:
		t.Fatalf("a failed read produced a notification: %+v", note)
	default:
	}

	// The state moved during the outage, and the watch reports it as soon as the store answers:
	// a hint is a re-read prompt, not a stream that has to be caught up.
	if _, err := m.Commit(context.Background(), CommitRequest{Checkpoint: &Checkpoint{
		ID: "o2", ThreadID: "alpha", ParentID: "o1", Step: 2, RunID: "run-o2", CreatedAt: queryAt(120 * time.Second),
	}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	store.failing.Store(false)
	step()
	select {
	case note := <-out:
		if note.Metadata.Ref.CheckpointID != "o2" {
			t.Fatalf("after the outage: %+v", note)
		}
	default:
		t.Fatal("the watch never reported the state that arrived during the outage")
	}
}
