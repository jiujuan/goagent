package bus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/jiujuan/goagent/core"
)

func onlySubscriber(b *Bus, topic Topic) *subscriber {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs[topic] {
		return s
	}
	panic("missing subscriber")
}

func TestPublishContextCancelledWhenFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(WithBufferSize(1))
		ch, cancel := b.Subscribe("run", Lossless)
		defer cancel()
		b.Publish("run", core.RunStarted{})
		ctx, stop := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- b.PublishContext(ctx, "run", core.TaskStarted{}) }()
		synctest.Wait()
		stop()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("publish error = %v", err)
		}
		if len(ch) != 1 {
			t.Fatalf("buffer = %d", len(ch))
		}
	})
}

func TestUnsubscribeUnblocksRegisteredPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(WithBufferSize(1))
		ch, cancel := b.Subscribe("run", Lossless)
		b.Publish("run", core.RunStarted{})
		s := onlySubscriber(b, "run")
		if !s.beginSend() {
			t.Fatal("could not register send")
		}
		cancelled := make(chan struct{})
		go func() { cancel(); close(cancelled) }()
		synctest.Wait()
		select {
		case <-cancelled:
			t.Fatal("cancel returned with registered sender")
		default:
		}
		s.endSend()
		<-cancelled
		for range ch {
		}
		cancel()
	})
}

func TestSlowTopicDoesNotBlockOtherTopic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(WithBufferSize(1))
		_, cancel := b.Subscribe("slow", Lossless)
		b.Publish("slow", core.RunStarted{})
		sent := make(chan struct{})
		go func() { b.Publish("slow", core.TaskStarted{}); close(sent) }()
		synctest.Wait()
		_, otherCancel := b.Subscribe("other", Lossless)
		if n := b.Subscribers("other"); n != 1 {
			t.Fatalf("subscribers = %d", n)
		}
		otherCancel()
		cancel()
		<-sent
	})
}

func TestFinishTopicOrderedTail(t *testing.T) {
	for _, mode := range []DeliveryMode{Lossless, Lossy} {
		synctest.Test(t, func(t *testing.T) {
			b := New(WithBufferSize(1))
			ch, cancel := b.Subscribe("run", mode)
			defer cancel()
			b.Publish("run", core.RunStarted{})
			b.FinishTopic("run", core.TaskDone{}, core.RunDone{})
			b.FinishTopic("run", core.RunFailed{})
			if b.Subscribers("run") != 0 {
				t.Fatal("sealed subscribers remain indexed")
			}
			if _, ok := (<-ch).(core.RunStarted); !ok {
				t.Fatal("missing buffered event")
			}
			if _, ok := (<-ch).(core.TaskDone); !ok {
				t.Fatal("missing root tail")
			}
			if _, ok := (<-ch).(core.RunDone); !ok {
				t.Fatal("missing run tail")
			}
			if _, ok := <-ch; ok {
				t.Fatal("tail channel still open")
			}
			fresh, stop := b.Subscribe("run", mode)
			defer stop()
			select {
			case <-fresh:
				t.Fatal("generic bus must not replay tail")
			default:
			}
		})
	}
}

func TestFinishTopicCancelAndRegisteredSend(t *testing.T) {
	for _, sealFirst := range []bool{true, false} {
		synctest.Test(t, func(t *testing.T) {
			b := New(WithBufferSize(1))
			ch, cancel := b.Subscribe("run", Lossless)
			b.Publish("run", core.RunStarted{})
			s := onlySubscriber(b, "run")
			if !s.beginSend() {
				t.Fatal("registration failed")
			}
			if sealFirst {
				b.FinishTopic("run", core.TaskDone{}, core.RunDone{})
			}
			exited := make(chan struct{})
			go func() { cancel(); close(exited) }()
			synctest.Wait()
			if !sealFirst {
				b.FinishTopic("run", core.RunDone{})
			}
			select {
			case <-exited:
				t.Fatal("returned before registered send ended")
			default:
			}
			s.endSend()
			<-exited
			cancel()
			for range ch {
			}
			if s.tailDone != nil {
				<-s.tailDone
			}
		})
	}
}

func TestFinishTopicUnsubscribeBlockedTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(WithBufferSize(1))
		_, cancel := b.Subscribe("run", Lossless)
		s := onlySubscriber(b, "run")
		b.Publish("run", core.RunStarted{})
		b.FinishTopic("run", core.RunDone{})
		synctest.Wait()
		cancel()
		<-s.tailDone
		<-s.closed
	})
}

func TestFinishTopicPayloadIsolation(t *testing.T) {
	b := New(WithBufferSize(3))
	a, stopA := b.Subscribe("run", Lossless)
	defer stopA()
	c, stopC := b.Subscribe("run", Lossless)
	defer stopC()
	message := core.Message{Parts: []core.Part{core.ToolResult{Content: []core.Part{core.Image{Data: []byte{1}}}}}}
	pending := []core.ApprovalRequest{{Args: []byte{2}}}
	b.FinishTopic("run", core.RunDone{Result: core.Result{Message: message}}, core.Interrupted{Pending: pending})
	x := (<-a).(core.RunDone)
	y := (<-c).(core.RunDone)
	x.Result.Message.Parts[0].(core.ToolResult).Content[0].(core.Image).Data[0] = 9
	if y.Result.Message.Parts[0].(core.ToolResult).Content[0].(core.Image).Data[0] != 1 {
		t.Fatal("tail aliases another observer")
	}
	if message.Parts[0].(core.ToolResult).Content[0].(core.Image).Data[0] != 1 {
		t.Fatal("tail aliases source")
	}
	pa := (<-a).(core.Interrupted)
	pc := (<-c).(core.Interrupted)
	pa.Pending[0].Args[0] = 9
	if pc.Pending[0].Args[0] != 2 || pending[0].Args[0] != 2 {
		t.Fatal("pending aliases")
	}
}

func TestFinishTopicPublishCancelRace(t *testing.T) {
	for range 50 {
		b := New(WithBufferSize(1))
		ch, cancel := b.Subscribe("run", Lossless)
		b.Publish("run", core.RunStarted{})
		var wg sync.WaitGroup
		wg.Go(func() { _ = b.PublishContext(context.Background(), "run", core.TurnStarted{}) })
		wg.Go(func() { b.FinishTopic("run", core.TaskDone{}, core.RunDone{}) })
		wg.Go(cancel)
		wg.Go(cancel)
		wg.Wait()
		for range ch {
		}
	}
}

func TestFinishTopicDetachedCancelWinsBeforeSeal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(WithBufferSize(1))
		ch, cancel := b.Subscribe("run", Lossless)
		s := onlySubscriber(b, "run")
		if !s.beginSend() {
			t.Fatal("registration failed")
		}
		// Freeze FinishTopic immediately after it detaches the index, retaining s.
		b.mu.Lock()
		detached := b.subs["run"]
		delete(b.subs, "run")
		b.mu.Unlock()
		stopped := make(chan struct{})
		go func() { cancel(); close(stopped) }()
		synctest.Wait()
		for sub := range detached {
			sub.seal([]core.Event{core.RunDone{}})
		}
		if s.tailDone != nil {
			t.Fatal("seal installed finisher after cancellation won")
		}
		s.endSend()
		<-stopped
		if _, ok := <-ch; ok {
			t.Fatal("cancel did not close channel")
		}
	})
}

func TestPublishContextCanceledKeepsReadyDiagnostics(t *testing.T) {
	for _, mode := range []DeliveryMode{Lossless, Lossy} {
		b := New(WithBufferSize(1))
		ch, stop := b.Subscribe("run", mode)
		defer stop()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := b.PublishContext(ctx, "run", core.TaskDone{Status: "canceled"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
		select {
		case ev := <-ch:
			if ev.(core.TaskDone).Status != "canceled" {
				t.Fatal(ev)
			}
		default:
			t.Fatal("ready cancellation diagnostic lost")
		}
		b.Publish("run", core.RunStarted{})
		if err := b.PublishContext(ctx, "run", core.TaskDone{}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if len(ch) != 1 {
			t.Fatal("canceled send blocked or changed full buffer")
		}
	}
}
