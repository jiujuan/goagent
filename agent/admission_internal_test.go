package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
)

type gateAcquireResult struct {
	release func()
	err     error
}

func gateAcquireAsync(g *threadGate, ctx context.Context, threadID string, policy AdmissionPolicy, maxWaiters int) <-chan gateAcquireResult {
	ch := make(chan gateAcquireResult, 1)
	go func() {
		rel, err := g.acquire(ctx, threadID, policy, maxWaiters)
		ch <- gateAcquireResult{release: rel, err: err}
	}()
	return ch
}

func gateAcquireNow(t *testing.T, g *threadGate, threadID string, policy AdmissionPolicy) func() {
	t.Helper()
	rel, err := g.acquire(context.Background(), threadID, policy, defaultAdmissionWaiters)
	if err != nil {
		t.Fatalf("acquire %q: %v", threadID, err)
	}
	return rel
}

func gateRecv(t *testing.T, ch <-chan gateAcquireResult) gateAcquireResult {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("waiter returned error: %v", r.err)
		}
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not granted")
		return gateAcquireResult{}
	}
}

func gateRecvErr(t *testing.T, ch <-chan gateAcquireResult, want error) {
	t.Helper()
	select {
	case r := <-ch:
		if r.release != nil {
			r.release()
			t.Fatal("canceled waiter received a release function")
		}
		if !errors.Is(r.err, want) {
			t.Fatalf("waiter error = %v, want %v", r.err, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled waiter did not return")
	}
}

func gateQueueLen(g *threadGate, threadID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.waiters[threadID])
}

func gateBusy(g *threadGate, threadID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.busy[threadID]
}

func waitGateQueued(t *testing.T, g *threadGate, threadID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := gateQueueLen(g, threadID); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiters on %q = %d, want %d", threadID, gateQueueLen(g, threadID), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestThreadGateFIFO(t *testing.T) {
	g := &threadGate{}
	relA := gateAcquireNow(t, g, "thread", AdmissionQueue)

	bCh := gateAcquireAsync(g, context.Background(), "thread", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "thread", 1)
	cCh := gateAcquireAsync(g, context.Background(), "thread", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "thread", 2)

	relA()
	b := gateRecv(t, bCh)
	if !gateBusy(g, "thread") {
		t.Fatal("thread reported idle while B holds the slot")
	}
	if got := gateQueueLen(g, "thread"); got != 1 {
		t.Fatalf("queued waiters after granting B = %d, want 1 (C)", got)
	}

	b.release()
	c := gateRecv(t, cCh)
	c.release()
	if gateBusy(g, "thread") {
		t.Fatal("thread still busy after every holder released")
	}
	if got := gateQueueLen(g, "thread"); got != 0 {
		t.Fatalf("queued waiters = %d, want 0", got)
	}
}

func TestThreadGateHandoff(t *testing.T) {
	g := &threadGate{}
	relA := gateAcquireNow(t, g, "thread", AdmissionQueue)
	bCh := gateAcquireAsync(g, context.Background(), "thread", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "thread", 1)

	relA()
	b := gateRecv(t, bCh)
	if !gateBusy(g, "thread") {
		t.Fatal("handoff freed the thread before B held the slot")
	}
	if _, err := g.acquire(context.Background(), "thread", AdmissionFail, defaultAdmissionWaiters); !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("newcomer during handoff error = %v, want ErrClaimConflict", err)
	}

	b.release()
	relC := gateAcquireNow(t, g, "thread", AdmissionFail)
	relC()
}

func TestThreadGateWaiterCancelBeforeGrant(t *testing.T) {
	g := &threadGate{}
	relA := gateAcquireNow(t, g, "thread", AdmissionQueue)

	bCtx, cancelB := context.WithCancel(context.Background())
	bCh := gateAcquireAsync(g, bCtx, "thread", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "thread", 1)
	cCh := gateAcquireAsync(g, context.Background(), "thread", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "thread", 2)

	cancelB()
	gateRecvErr(t, bCh, context.Canceled)
	if got := gateQueueLen(g, "thread"); got != 1 {
		t.Fatalf("queued waiters after B canceled = %d, want 1 (C)", got)
	}

	relA()
	c := gateRecv(t, cCh)
	c.release()
	if gateBusy(g, "thread") {
		t.Fatal("thread still busy after C released")
	}
}

func TestThreadGateWaiterCancelAfterGrant(t *testing.T) {
	g := &threadGate{}
	relA := gateAcquireNow(t, g, "thread", AdmissionQueue)

	bCtx, cancelB := context.WithCancel(context.Background())
	bCh := gateAcquireAsync(g, bCtx, "thread", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "thread", 1)
	cCh := gateAcquireAsync(g, context.Background(), "thread", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "thread", 2)

	// Cancel before granting, so whichever side of B's select wins (ready or
	// done) it must pass the slot to C instead of holding it.
	cancelB()
	relA()
	gateRecvErr(t, bCh, context.Canceled)

	if _, err := g.acquire(context.Background(), "thread", AdmissionFail, defaultAdmissionWaiters); !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("canceled waiter leaked its slot: new acquire error = %v", err)
	}
	c := gateRecv(t, cCh)
	c.release()
	if gateBusy(g, "thread") {
		t.Fatal("thread still busy after C released")
	}
}

func TestThreadGateQueueFull(t *testing.T) {
	g := &threadGate{}
	relA := gateAcquireNow(t, g, "thread", AdmissionQueue)
	bCh := gateAcquireAsync(g, context.Background(), "thread", AdmissionQueue, 1)
	waitGateQueued(t, g, "thread", 1)

	if _, err := g.acquire(context.Background(), "thread", AdmissionQueue, 1); !errors.Is(err, ErrAdmissionQueueFull) {
		t.Fatalf("second waiter error = %v, want ErrAdmissionQueueFull", err)
	}

	relA()
	b := gateRecv(t, bCh)
	b.release()
	if gateBusy(g, "thread") {
		t.Fatal("thread still busy after B released")
	}
}

func TestThreadGateDifferentThreads(t *testing.T) {
	g := &threadGate{}
	relA := gateAcquireNow(t, g, "t1", AdmissionFail)
	bCh := gateAcquireAsync(g, context.Background(), "t1", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "t1", 1)

	relB := gateAcquireNow(t, g, "t2", AdmissionFail)

	relA()
	b := gateRecv(t, bCh)
	b.release()
	relB()
	if gateBusy(g, "t1") || gateBusy(g, "t2") {
		t.Fatal("a thread stayed busy after its runs released")
	}
}

func TestThreadGateReleaseIdempotent(t *testing.T) {
	g := &threadGate{}
	relA := gateAcquireNow(t, g, "thread", AdmissionQueue)
	bCh := gateAcquireAsync(g, context.Background(), "thread", AdmissionQueue, defaultAdmissionWaiters)
	waitGateQueued(t, g, "thread", 1)

	relA()
	relA()
	b := gateRecv(t, bCh)
	if _, err := g.acquire(context.Background(), "thread", AdmissionFail, defaultAdmissionWaiters); !errors.Is(err, checkpoint.ErrClaimConflict) {
		t.Fatalf("double release freed B's slot: new acquire error = %v", err)
	}

	b.release()
	c := gateAcquireNow(t, g, "thread", AdmissionFail)
	c()
}
