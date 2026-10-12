package queue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/queue"
)

func TestMemBusPubSub(t *testing.T) {
	b := queue.NewMemBus()
	ch, cancel := b.Subscribe("job-1")
	defer cancel()

	b.Publish("job-1", core.RunStarted{RunID: "r1"})
	b.Publish("job-1", core.RunDone{Result: core.Result{Message: core.AssistantText("ok")}})
	b.Publish("other", core.RunStarted{RunID: "nope"}) // different key, not delivered

	if _, ok := (<-ch).(core.RunStarted); !ok {
		t.Fatal("expected RunStarted")
	}
	done, ok := (<-ch).(core.RunDone)
	if !ok || done.Result.Message.Text() != "ok" {
		t.Fatalf("expected RunDone(ok), got %#v", done)
	}
}

func TestBridgeForwardsEvents(t *testing.T) {
	b := queue.NewMemBus()
	out, cancel := b.Subscribe("k")
	defer cancel()

	src := make(chan core.Event, 4)
	src <- core.PlanNodeStarted{NodeID: "n1"}
	src <- core.PlanNodeDone{NodeID: "n1", Status: "done"}
	close(src)

	go queue.Bridge(b, "k", src)

	select {
	case ev := <-out:
		if s, ok := ev.(core.PlanNodeStarted); !ok || s.NodeID != "n1" {
			t.Fatalf("bridged event = %#v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bridge did not forward")
	}
}

func TestNewBusInProcess(t *testing.T) {
	b, err := queue.NewBus() // no WithRedis -> MemBus
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := b.(*queue.MemBus); !ok {
		t.Fatalf("NewBus() = %T, want *MemBus", b)
	}
}

// memBusBuffer 是 Subscribe 给每个订阅者的通道容量（见 queue/bus.go），
// 决定了慢订阅者能吸收多少事件、从哪一条开始丢弃。
const memBusBuffer = 64

// runWithDeadline 等一组 goroutine 结束，超时或 panic 都判失败：
// 竞争若复发，表现为 send on closed channel panic，或发布方等不到锁而卡住。
func runWithDeadline(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s：并发发布与取消未能结束", what)
	}
}

// TestMemBusConcurrentPublishAndCancel 专门覆盖发布与订阅取消的交错：
// 取消会关闭订阅通道，发布方必须与之互斥，否则就是向已关闭的通道发送。
// 两个键各由多个 goroutine 反复发布/订阅/取消，取消时机完全由竞争决定，
// 因此这条用例需要 -race 执行（go test -race ./queue）。
func TestMemBusConcurrentPublishAndCancel(t *testing.T) {
	b := queue.NewMemBus()
	const rounds = 300

	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			key := fmt.Sprintf("job-%d", p%2)
			for i := 0; i < rounds; i++ {
				b.Publish(key, core.RunStarted{RunID: fmt.Sprintf("run-%d-%d", p, i)})
			}
		}(p)
	}
	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			key := fmt.Sprintf("job-%d", s%2)
			for i := 0; i < rounds; i++ {
				ch, cancel := b.Subscribe(key)
				// 读走一条，让发布方既走"送达"也走"丢弃"分支。
				select {
				case <-ch:
				case <-time.After(time.Millisecond):
				}
				cancel()
			}
		}(s)
	}
	runWithDeadline(t, &wg, "发布/取消交错")
}

// TestBridgeCancelWhileForwardingNotStarted 是 worker 里的真实形状：Bridge 还停在
// 源通道上（一条都没转发），持有订阅的一方就取消了。取消关闭通道时发布方不得发送。
func TestBridgeCancelWhileForwardingNotStarted(t *testing.T) {
	b := queue.NewMemBus()
	src := make(chan core.Event)
	out, cancel := b.Subscribe("job-live")

	done := make(chan struct{})
	go func() {
		queue.Bridge(b, "job-live", src)
		close(done)
	}()

	cancel() // Bridge 此时正阻塞在 src 上，尚未 Publish
	src <- core.RunStarted{RunID: "after-cancel"}
	close(src)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Bridge 未能随源通道关闭而结束")
	}
	select {
	case ev, ok := <-out:
		if ok {
			t.Fatalf("取消后仍收到事件 %#v", ev)
		}
		// 通道已关闭时接收会立即返回，所以这里必须真正取出：
		// ok=false 只说明订阅通道被取消关闭，与发布方的发送通道无关。
	default:
	}
}

// TestMemBusFullBufferDropsWithoutBlocking：订阅者不读取时发布方必须继续返回，
// 溢出部分丢弃，缓冲区恰好收满。
func TestMemBusFullBufferDropsWithoutBlocking(t *testing.T) {
	b := queue.NewMemBus()
	ch, cancel := b.Subscribe("slow")
	defer cancel()

	const published = memBusBuffer + 50
	publishedDone := make(chan struct{})
	go func() {
		defer close(publishedDone)
		for i := 0; i < published; i++ {
			b.Publish("slow", core.RunStarted{RunID: fmt.Sprintf("r%d", i)})
		}
	}()
	select {
	case <-publishedDone:
	case <-time.After(5 * time.Second):
		t.Fatal("发布被慢订阅者阻塞")
	}

	var got int
	for {
		select {
		case <-ch:
			got++
		default:
			if got != memBusBuffer {
				t.Fatalf("收到 %d 个事件，期望缓冲区容量 %d", got, memBusBuffer)
			}
			return
		}
	}
}

// TestMemBusPublishDoesNotStallBehindSlowSubscriber：一个不读取的订阅者不能把
// 另一个正常读取的订阅者的投递拖住（锁在整个非阻塞发送期间仍然可用）。
func TestMemBusPublishDoesNotStallBehindSlowSubscriber(t *testing.T) {
	b := queue.NewMemBus()
	stalled, stalledCancel := b.Subscribe("k")
	defer stalledCancel()
	fresh, cancel := b.Subscribe("k")
	defer cancel()

	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	for i := 0; i < 2*memBusBuffer; i++ {
		b.Publish("k", core.RunStarted{RunID: fmt.Sprintf("r%d", i)})
		select {
		case <-fresh:
		case <-ctx.Done():
			t.Fatalf("第 %d 次发布后活跃订阅者取不到事件（缓冲区 %d）", i, memBusBuffer)
		}
	}

	// 不读取的那一路只吃满缓冲区，其余丢弃；它从未让发布方停下来。
	var absorbed int
	for {
		select {
		case <-stalled:
			absorbed++
		default:
			if absorbed != memBusBuffer {
				t.Fatalf("慢订阅者收到 %d 个事件，期望缓冲区容量 %d", absorbed, memBusBuffer)
			}
			return
		}
	}
}
