package runstore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestRunFileRetrySyncFailure(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "terminal"}[terminal], func(t *testing.T) {
			store := newFileStore(t, t.TempDir())
			ctx := context.Background()
			rec := recordIn("retry-sync", StatusQueued, testBase())
			if terminal {
				rec = mustCreate(t, store, rec)
				var err error
				rec, err = cas(t, store, rec.RunID, rec.Version, func(r *RunRecord) { r.Status = StatusRunning })
				if err != nil {
					t.Fatal(err)
				}
			}
			boom := errors.New("sync failed")
			calls := 0
			store.ops.sync = func(*os.File) error { calls++; return boom }
			var err error
			if terminal {
				rec.Status = StatusSucceeded
				_, err = store.CompareAndSwap(ctx, rec.RunID, rec.Version, rec)
			} else {
				_, err = store.Create(ctx, rec)
			}
			if !errors.Is(err, boom) {
				t.Fatalf("first write: %v", err)
			}
			retry := func() error {
				if terminal {
					latest := getRecord(t, store, rec.RunID)
					_, err := store.CompareAndSwap(ctx, latest.RunID, latest.Version, latest)
					return err
				}
				_, err := store.Create(ctx, rec)
				return err
			}
			if err := retry(); !errors.Is(err, boom) {
				t.Fatalf("retry must confirm durability: %v", err)
			}
			if calls != 2 {
				t.Fatalf("Sync calls=%d", calls)
			}
			before := readJournal(t, store.dir, rec.RunID)
			store.ops = defaultFileOps()
			if err := retry(); err != nil {
				t.Fatal(err)
			}
			if after := readJournal(t, store.dir, rec.RunID); after != before {
				t.Fatal("retry appended another event")
			}
		})
	}
}

func TestRunFileActiveLockDoesNotExpire(t *testing.T) {
	store := newFileStore(t, t.TempDir())
	release, err := store.acquireProcessLock(context.Background(), "held")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	path, _ := store.lockPath("held")
	old := time.Now().Add(-3 * time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	other, err := store.acquireProcessLock(ctx, "held")
	if err == nil {
		other()
		t.Fatal("active lock stolen based on age")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestRunFileSameInstanceWaitCancellation(t *testing.T) {
	store := newFileStore(t, t.TempDir())
	entered, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- store.withRun(context.Background(), "held", func(*RunRecord) error { close(entered); <-release; return nil })
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := store.Get(ctx, "held"); done <- err }()
	select {
	case err := <-done:
		close(release)
		<-first
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		close(release)
		<-first
		<-done
		t.Fatal("local lock ignored cancellation")
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := store.Create(cancelled, recordIn("cancelled", StatusQueued, testBase())); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled create: %v", err)
	}
}

func TestRunFileReleaseDoesNotRemoveNextOwner(t *testing.T) {
	store := newFileStore(t, t.TempDir())
	first, err := store.acquireProcessLock(context.Background(), "owners")
	if err != nil {
		t.Fatal(err)
	}
	first()
	second, err := store.acquireProcessLock(context.Background(), "owners")
	if err != nil {
		t.Fatal(err)
	}
	defer second()
	first() // Releasing an old handle again cannot release or unlink the new lock.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	third, err := store.acquireProcessLock(ctx, "owners")
	if err == nil {
		third()
		t.Fatal("old release removed current owner's lock")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
