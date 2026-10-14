package watch

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

// The loop is shared by two stores, so its own tests drive it with a hand-stepped ticker: no
// test here waits on real time, and none can pass by accident while the loop is still
// scheduling.

type fakeTicker struct {
	ticks   chan time.Time
	stopped int
}

func (f *fakeTicker) Chan() <-chan time.Time { return f.ticks }
func (f *fakeTicker) Stop()                  { f.stopped++ }

func (f *fakeTicker) step(t *testing.T) {
	t.Helper()
	select {
	case f.ticks <- time.Unix(0, 0):
	case <-time.After(2 * time.Second):
		t.Fatal("the loop stopped taking ticks")
	}
}

// harness runs one loop with a test ticker and a non-blocking deliver, which is how both
// stores use it: they own the channel, and a full channel makes deliver refuse.
type harness[T any] struct {
	ticker *fakeTicker
	passes chan struct{}
	out    chan Notification[T]
	done   chan struct{}
	cancel context.CancelFunc
}

func start[T any](t *testing.T, l *Loop[T], buffer int) *harness[T] {
	t.Helper()
	f := &fakeTicker{ticks: make(chan time.Time)}
	l.NewTicker = func(time.Duration) Ticker { return f }
	if l.Interval == 0 {
		l.Interval = time.Minute
	}
	passSignal := make(chan struct{}, 64)
	previous := l.OnPass
	l.OnPass = func() {
		if previous != nil {
			previous()
		}
		select {
		case passSignal <- struct{}{}:
		default:
		}
	}
	out := make(chan Notification[T], buffer)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(out)
		l.Run(ctx, func(note Notification[T]) bool {
			select {
			case out <- note:
				return true
			default:
				return false
			}
		})
	}()
	h := &harness[T]{ticker: f, passes: passSignal, out: out, done: done, cancel: cancel}
	t.Cleanup(func() { cancel(); <-done })
	return h
}

// tick runs exactly one loop pass and waits for that pass to finish, delivery included, so no
// assertion below can be racing with a pass still in flight.
func (h *harness[T]) tick(t *testing.T) {
	t.Helper()
	h.ticker.step(t)
	select {
	case <-h.passes:
	case <-time.After(2 * time.Second):
		t.Fatal("the loop never finished the pass it was woken for")
	}
}

// pass runs one pass and returns everything that became readable.
func (h *harness[T]) pass(t *testing.T) []Notification[T] {
	t.Helper()
	h.tick(t)
	return h.collect(t)
}

func (h *harness[T]) collect(t *testing.T) []Notification[T] {
	t.Helper()
	var got []Notification[T]
	for {
		select {
		case note, open := <-h.out:
			if !open {
				t.Fatal("the channel closed while the watch was still expected to run")
			}
			got = append(got, note)
		default:
			return got
		}
	}
}

// state is the version source the loop reads, one version per key.
type state struct {
	versions map[string]uint64
	hidden   map[string]bool
	failing  map[string]bool
}

func newState() *state {
	return &state{versions: map[string]uint64{}, hidden: map[string]bool{}, failing: map[string]bool{}}
}

func (s *state) set(key string, version uint64) { s.versions[key] = version }

func (s *state) reader() Read[string] {
	return func(_ context.Context, key string) (Reading[string], error) {
		if s.failing[key] {
			return Reading[string]{}, errors.New("watch test: store unavailable for " + key)
		}
		if s.hidden[key] {
			return Reading[string]{Found: false}, nil
		}
		version := s.versions[key]
		return Reading[string]{Value: strconv.FormatUint(version, 10), Version: version, Found: true}, nil
	}
}

func TestWatchBaselineIsSilentAndLaterChangesNotify(t *testing.T) {
	s := newState()
	s.set("a", 1)
	var reads []string
	loop := &Loop[string]{
		Keys: []string{"a"},
		Read: func(ctx context.Context, key string) (Reading[string], error) {
			reads = append(reads, key)
			return s.reader()(ctx, key)
		},
	}
	h := start(t, loop, DefaultBuffer)

	// The state at start is the baseline: a client that re-reads current state first is not
	// told about it again.
	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("the baseline pass delivered %v", keysOf(got))
	}
	if len(reads) == 0 {
		t.Fatal("the baseline pass read nothing")
	}
	for _, key := range reads {
		if key != "a" {
			t.Fatalf("the pass read %q, want only the watched key", key)
		}
	}

	s.set("a", 2)
	got := h.pass(t)
	if len(got) != 1 || got[0].Version != 2 || got[0].Value != "2" || got[0].Key != "a" {
		t.Fatalf("change delivered %v", got)
	}

	// The same version again — an idempotent retry that changed nothing — is not a new state.
	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("a repeated version notified: %v", keysOf(got))
	}

	// A reading that went backwards is neither a new state nor a moved marker.
	s.set("a", 6)
	if got := h.pass(t); len(got) != 1 || got[0].Version != 6 {
		t.Fatalf("a newer version: %v", got)
	}
	s.set("a", 4)
	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("a backwards reading notified: %v", keysOf(got))
	}
	// The marker did not move down to 4, so 6 again is still the version already delivered.
	s.set("a", 6)
	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("the delivered version came back and notified again: %v", keysOf(got))
	}
	s.set("a", 7)
	if got := h.pass(t); len(got) != 1 || got[0].Version != 7 {
		t.Fatalf("the next real change: %v", got)
	}
}

func TestWatchKeysThatAppearAndDisappear(t *testing.T) {
	s := newState()
	s.hidden["later"] = true
	h := start(t, (&Loop[string]{Keys: []string{"later"}, Read: s.reader()}), DefaultBuffer)

	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("a key with no state yet delivered %v", keysOf(got))
	}
	// The record was created: appearing is a change, even though this is the first version of
	// it this watch has seen.
	s.hidden["later"] = false
	s.set("later", 1)
	if got := h.pass(t); len(got) != 1 || got[0].Version != 1 {
		t.Fatalf("appearing key: %v", got)
	}
	// A key that stops answering is not a state to report, and coming back at the version the
	// watch already saw must not notify twice.
	s.hidden["later"] = true
	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("a key that went unread delivered %v", keysOf(got))
	}
	s.hidden["later"] = false
	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("the same version came back and notified: %v", got)
	}
	s.set("later", 2)
	if got := h.pass(t); len(got) != 1 || got[0].Version != 2 {
		t.Fatalf("a newer version after a gap: %v", got)
	}
}

func TestWatchMergesNotificationsForASlowConsumer(t *testing.T) {
	s := newState()
	s.set("a", 1)
	// One slot, and nobody reading.
	h := start(t, (&Loop[string]{Keys: []string{"a"}, Read: s.reader()}), 1)

	h.tick(t) // baseline
	s.set("a", 2)
	h.tick(t) // delivered into the one buffer slot
	s.set("a", 3)
	h.tick(t) // cannot be delivered: the consumer has not read
	s.set("a", 4)
	h.tick(t) // replaces the waiting one, which now stands for two changes

	// What the consumer finds waiting is the state that was delivered while it kept up, plus
	// the newest one, which stands in for everything older that never made it out.
	got := h.collect(t)
	if len(got) != 1 || got[0].Version != 2 || got[0].Coalesced != 0 {
		t.Fatalf("buffered notification = %v, want version 2 with nothing merged into it", got)
	}
	h.tick(t) // the slot is free now, so the waiting state goes out
	late := h.collect(t)
	if len(late) != 1 {
		t.Fatalf("the waiting state delivered %v, want exactly one notification", late)
	}
	if late[0].Version != 4 || late[0].Coalesced != 1 {
		t.Errorf("merged notification = %+v, want version 4 standing in for version 3 as well", late[0])
	}

	// Once the consumer catches up, the next real change is reported on its own.
	s.set("a", 5)
	next := h.pass(t)
	if len(next) != 1 || next[0].Version != 5 || next[0].Coalesced != 0 {
		t.Fatalf("after catching up: %v", next)
	}
}

func TestWatchKeepsWatchingThroughReadErrors(t *testing.T) {
	s := newState()
	s.set("a", 1)
	var errs []error
	loop := &Loop[string]{Keys: []string{"a"}, Read: s.reader(), OnError: func(err error) { errs = append(errs, err) }}
	h := start(t, loop, DefaultBuffer)

	h.tick(t) // baseline
	s.failing["a"] = true
	if got := h.pass(t); len(got) != 0 {
		t.Fatalf("a failed read invented a notification: %v", got)
	}
	if len(errs) == 0 {
		t.Fatal("OnError never saw the outage")
	}
	for _, err := range errs {
		if err == nil {
			t.Fatal("OnError was called with nil")
		}
	}
	// The state moved on during the outage; the watch reports it when the store answers again.
	s.failing["a"] = false
	s.set("a", 7)
	if got := h.pass(t); len(got) != 1 || got[0].Version != 7 {
		t.Fatalf("after the outage: %v", got)
	}

	// A key whose very first read failed has no baseline, so its first answer is a change.
	s2 := newState()
	s2.set("a", 8)
	s2.failing["a"] = true
	h2 := start(t, (&Loop[string]{Keys: []string{"a"}, Read: s2.reader(), OnError: func(error) {}}), DefaultBuffer)
	if got := h2.pass(t); len(got) != 0 {
		t.Fatalf("a failing first read delivered %v", got)
	}
	s2.failing["a"] = false
	if got := h2.pass(t); len(got) != 1 || got[0].Version != 8 {
		t.Fatalf("a key whose first pass failed is still watched: %v", got)
	}
}

func TestWatchFlushOrderFollowsKeys(t *testing.T) {
	s := newState()
	s.set("a", 1)
	s.set("b", 1)
	h := start(t, (&Loop[string]{Keys: []string{"b", "a"}, Read: s.reader()}), 4)

	h.tick(t) // baseline
	s.set("a", 10)
	s.set("b", 10)
	got := h.pass(t)
	if len(got) != 2 || got[0].Key != "b" || got[1].Key != "a" {
		t.Fatalf("delivered %v, want the watched order rather than the map order", keysOf(got))
	}
}

func TestWatchContextEndReturnsAndStopsTheTicker(t *testing.T) {
	s := newState()
	s.set("a", 1)
	h := start(t, (&Loop[string]{Keys: []string{"a"}, Read: s.reader()}), DefaultBuffer)

	h.pass(t)
	// Ending the context is the only thing left to notice: nothing more is sent to the ticker,
	// so the loop has to leave on its own and the channel it delivered into gets closed.
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the loop outlived its context")
	}
	for range h.out {
	}
	if h.ticker.stopped == 0 {
		t.Fatal("the ticker was never stopped, so the loop would keep a timer running")
	}

	// Leaving while reads fail is the same exit.
	s.failing["a"] = true
	h2 := start(t, (&Loop[string]{Keys: []string{"a"}, Read: s.reader(), OnError: func(error) {}}), DefaultBuffer)
	h2.pass(t)
	h2.cancel()
	select {
	case <-h2.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the loop outlived its context while reads were failing")
	}
	for range h2.out {
	}
}

func TestLoopUsesItsDefaultsWhenNotConfigured(t *testing.T) {
	s := newState()
	loop := (&Loop[string]{Keys: []string{"a"}, Read: s.reader()})

	// An unset interval takes the default rather than spinning, and a caller that injected
	// nothing gets the real timer.
	ticker := loop.newTicker()
	if ticker == nil {
		t.Fatal("no ticker")
	}
	defer ticker.Stop()
	select {
	case <-ticker.Chan():
		t.Fatal("the default ticker fired immediately")
	case <-time.After(20 * time.Millisecond):
	}
	if DefaultInterval <= 0 || DefaultBuffer <= 0 {
		t.Fatalf("DefaultInterval = %v, DefaultBuffer = %d", DefaultInterval, DefaultBuffer)
	}
}

func keysOf[T any](notes []Notification[T]) []string {
	out := make([]string, 0, len(notes))
	for _, note := range notes {
		out = append(out, note.Key)
	}
	return out
}
