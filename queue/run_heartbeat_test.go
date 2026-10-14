package queue

// SM-09（ADR-0034 D6）：任务心跳与租约。心跳只写任务记录的活性字段，不推进 checkpoint head、
// 不续 checkpoint 认领；记录的归属一旦不在本次投递上，旧 epoch 不得再发布终态。

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/runstore"
)

var heartbeatTestStart = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// takeoverStore 假装恢复工具把这条记录交给了另一个 worker：读到的所有权被换掉，之后的写入
// 一律按丢失所有权拒绝——真实存储里领取后的归属不会自己变，所以这一层只在测试里存在。
type takeoverStore struct {
	runstore.Store

	take    atomic.Bool
	newID   string
	mu      sync.Mutex
	writes  int
	rejects int
}

func (s *takeoverStore) Get(ctx context.Context, runID string) (runstore.RunRecord, error) {
	rec, err := s.Store.Get(ctx, runID)
	if err != nil || !s.take.Load() {
		return rec, err
	}
	rec.WorkerID = s.newID
	rec.WorkerEpoch = s.currentEpoch(runID) + 1
	return rec, nil
}

// currentEpoch reads the real record's epoch so the forged one is strictly newer.
func (s *takeoverStore) currentEpoch(runID string) uint64 {
	rec, err := s.Store.Get(context.Background(), runID)
	if err != nil {
		return 0
	}
	return rec.WorkerEpoch
}

func (s *takeoverStore) CompareAndSwap(ctx context.Context, runID string, expected uint64, next runstore.RunRecord) (runstore.RunRecord, error) {
	if s.take.Load() {
		s.mu.Lock()
		s.rejects++
		s.mu.Unlock()
		return runstore.RunRecord{}, runstore.ErrOwnershipLost
	}
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.Store.CompareAndSwap(ctx, runID, expected, next)
}

func (s *takeoverStore) counts() (writes, rejects int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes, s.rejects
}

// waitForHeartbeat 等记录上出现第 want 次心跳（领取占一个版本，每次心跳再加一个）。
func waitForHeartbeat(t *testing.T, store runstore.Store, runID string, want int) time.Time {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := store.Get(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Version >= uint64(want+2) && !rec.HeartbeatAt.IsZero() {
			return rec.HeartbeatAt
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("运行 %s 没等到第 %d 次心跳", runID, want)
	return time.Time{}
}

func headCheckpointOrNil(t *testing.T, store checkpoint.Checkpointer, threadID string) *checkpoint.Checkpoint {
	t.Helper()
	head, err := store.Latest(context.Background(), threadID)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

// checkpointDiffers 比较两个 head 是否同一份，允许两边都为空（还没提交过）。
func checkpointDiffers(a, b *checkpoint.Checkpoint) bool {
	if a == nil || b == nil {
		return a != b
	}
	return a.ID != b.ID || a.Revision != b.Revision
}

// TestHeartbeatDoesNotAdvanceHead 分两步：先用可注入的时钟跨过好几个租期逐步心跳，确认真
// 实写入只有任务记录的活性字段；再让一次真实投递在阻塞的工具上跑过多次心跳，确认 checkpoint
// 历史一步没动，并且投递结束后循环不再写记录。
func TestHeartbeatDoesNotAdvanceHead(t *testing.T) {
	ctx := context.Background()

	t.Run("stepped across leases", func(t *testing.T) {
		cps := newSettleStore()
		durable := runstore.NewMemory()
		w := managedWorker(t, managedAgent(t, cps, "不该被心跳推进", nil), durable, "worker-A",
			WithWorkerLease(30*time.Second),
			WithHeartbeatInterval(5*time.Second),
			WithCancelPollInterval(time.Second))

		// 假时钟：跨租期不需要真的等。
		now := heartbeatTestStart
		w.now = func() time.Time { return now }

		created := managedCreate(t, durable, managedRecord("run-hb-step", "thread-hb-step", "turn-1"))
		claimed, err := w.claim(ctx, created)
		if err != nil {
			t.Fatalf("领取失败: %v", err)
		}
		watch := w.newTaskWatch(claimed)
		noop := func() {}

		// 20 步 × 500 毫秒 = 10 秒 ⇒ 恰好两次心跳（第 5 秒、第 10 秒）。
		for i := 0; i < 20; i++ {
			now = now.Add(500 * time.Millisecond)
			watch.tick(ctx, noop)
		}

		rec := managedGet(t, durable, created.RunID)
		if rec.Status != runstore.StatusRunning || rec.FinishReason != "" || rec.Result != nil {
			t.Fatalf("心跳改动了运行状态: %s/%q", rec.Status, rec.FinishReason)
		}
		if !rec.HeartbeatAt.Equal(now) {
			t.Fatalf("HeartbeatAt = %v, 期望 %v", rec.HeartbeatAt, now)
		}
		if want := now.Add(30 * time.Second); !rec.WorkerLeaseUntil.Equal(want) {
			t.Fatalf("WorkerLeaseUntil = %v, 期望 %v", rec.WorkerLeaseUntil, want)
		}
		if rec.Version != claimed.Version+2 {
			t.Fatalf("Version = %d, 期望两次心跳后 %d", rec.Version, claimed.Version+2)
		}
		if !rec.CancelRequestedAt.IsZero() || rec.WorkerID != "worker-A" || rec.WorkerEpoch != 1 {
			t.Fatalf("心跳动了别的事实: %+v", rec)
		}
		if heartbeats, failures, lastErr := watch.stats(); heartbeats != 2 || failures != 0 || lastErr != nil {
			t.Fatalf("心跳统计 = %d/%d/%v, 期望 2/0/<nil>", heartbeats, failures, lastErr)
		}
		if lost := watch.ownershipLost(); lost != nil {
			t.Fatalf("所有权没变却报丢失: %v", lost)
		}

		// 任务心跳一步都不会写到 checkpoint 上。
		if count := historyCount(t, cps, created.ThreadID); count != 0 {
			t.Fatalf("心跳写出了 %d 条 checkpoint", count)
		}
		if head, err := cps.Latest(ctx, created.ThreadID); err != nil || head != nil {
			t.Fatalf("心跳推进了线程 head: %+v err=%v", head, err)
		}
		if commits := cps.settleCommits(); len(commits) != 0 {
			t.Fatalf("心跳提交了 %d 次终态", len(commits))
		}

		// 间隔未到就绝不多写一次。
		before := managedGet(t, durable, created.RunID)
		now = now.Add(400 * time.Millisecond)
		watch.tick(ctx, noop)
		if after := managedGet(t, durable, created.RunID); after.Version != before.Version {
			t.Fatalf("还没到心跳间隔就多写了一次: %d → %d", before.Version, after.Version)
		}

		// 记录被恢复工具代笔终结后，循环要停下执行，也不再写心跳。
		terminated := managedCAS(t, durable, managedGet(t, durable, created.RunID), func(r *runstore.RunRecord) {
			r.Status = runstore.StatusFailed
			r.FinishReason = runstore.FinishReasonFailed
			r.Error = &runstore.RunError{Code: runstore.ErrCodeExecutionFailed, Message: "恢复工具判定"}
		})
		var cancelled atomic.Bool
		now = now.Add(5 * time.Second)
		watch.tick(ctx, func() { cancelled.Store(true) })
		if !cancelled.Load() {
			t.Fatal("记录已终结却没有停止执行")
		}
		if after := managedGet(t, durable, created.RunID); after.Version != terminated.Version {
			t.Fatalf("终结之后还在写心跳: %d → %d", terminated.Version, after.Version)
		}
	})

	t.Run("running delivery", func(t *testing.T) {
		cps := checkpoint.NewMemory()
		durable := runstore.NewMemory()
		started := make(chan struct{})
		release := make(chan struct{})
		w := managedWorker(t, managedBlockingAgent(t, cps, started, release), durable, "worker-A",
			WithWorkerLease(60*time.Millisecond),
			WithHeartbeatInterval(20*time.Millisecond),
			WithCancelPollInterval(2*time.Millisecond))

		created := managedCreate(t, durable, managedRecord("run-hb-live", "thread-hb-live", "turn-1"))
		type result struct {
			out ManagedRunOutcome
			err error
		}
		done := make(chan result, 1)
		go func() {
			out, err := w.Execute(ctx, created.RunID, "问题")
			done <- result{out, err}
		}()

		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("工具没有开始")
		}

		waitForHeartbeat(t, durable, created.RunID, 2)
		headDuring := headCheckpointOrNil(t, cps, created.ThreadID)
		histDuring := historyCount(t, cps, created.ThreadID)

		// 再来两次心跳：head 与历史条数一步都不许动（阻塞在工具里时本来就没有新提交）。
		waitForHeartbeat(t, durable, created.RunID, 4)
		headMore := headCheckpointOrNil(t, cps, created.ThreadID)
		if checkpointDiffers(headDuring, headMore) {
			t.Fatalf("心跳把 head 推到了 %+v（之前 %+v）", headMore, headDuring)
		}
		if histMore := historyCount(t, cps, created.ThreadID); histMore != histDuring {
			t.Fatalf("心跳把 checkpoint 历史从 %d 条推到 %d 条", histDuring, histMore)
		}
		hbAt := managedGet(t, durable, created.RunID).HeartbeatAt

		close(release)
		var res result
		select {
		case res = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("worker 没有收尾")
		}
		if res.err != nil {
			t.Fatalf("心跳不该让执行失败: %v", res.err)
		}
		if res.out.Record.Status != runstore.StatusSucceeded || res.out.RecordError != nil {
			t.Fatalf("终态 = %s recordErr=%v", res.out.Record.Status, res.out.RecordError)
		}
		// 心跳写的活性字段不会被终态抹掉。
		final := managedGet(t, durable, created.RunID)
		if final.HeartbeatAt.Before(hbAt) {
			t.Fatalf("终态发布把心跳退回了: %v → %v", hbAt, final.HeartbeatAt)
		}

		// 投递结束后循环退出：再等几个心跳间隔，版本不再动。
		settled := final.Version
		time.Sleep(80 * time.Millisecond)
		if later := managedGet(t, durable, created.RunID); later.Version != settled {
			t.Fatalf("循环没退出：%d → %d", settled, later.Version)
		}
	})
}

// TestWorkerEpochLossStopsPublishing：记录的归属被换走后，旧 worker 停止执行、停止发布，
// 任务终态与 checkpoint 终态都不由它写。
func TestWorkerEpochLossStopsPublishing(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	durable := runstore.NewMemory()
	store := &takeoverStore{Store: durable, newID: "worker-recovered"}

	started := make(chan struct{})
	release := make(chan struct{})
	w := managedWorker(t, managedBlockingAgent(t, cps, started, release), store, "worker-A",
		WithWorkerLease(10*time.Second),
		WithHeartbeatInterval(10*time.Millisecond),
		WithCancelPollInterval(2*time.Millisecond))

	created := managedCreate(t, store, managedRecord("run-epoch-loss", "thread-epoch-loss", "turn-1"))
	done := make(chan ManagedRunOutcome, 1)
	go func() {
		out, err := w.Execute(ctx, created.RunID, "问题")
		if err == nil {
			t.Errorf("丢失所有权必须返回错误")
		}
		done <- out
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("工具没有开始")
	}
	// 先让几次心跳证明这条循环活着，再把记录交给别人。
	waitForHeartbeat(t, durable, created.RunID, 3)
	store.take.Store(true)
	owned := managedGet(t, durable, created.RunID)
	writesAtFlip, _ := store.counts()
	if owned.WorkerID != "worker-A" || owned.WorkerEpoch != 1 || owned.Status != runstore.StatusRunning {
		t.Fatalf("领取信息不对: %+v", owned)
	}

	var out ManagedRunOutcome
	select {
	case out = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("丢所有权后执行没有停下")
	}

	if !out.Executed {
		t.Fatalf("确实执行过: %+v", out)
	}
	if out.RecordError == nil || !errors.Is(out.RecordError, runstore.ErrOwnershipLost) {
		t.Fatalf("RecordError = %v, 期望 ErrOwnershipLost", out.RecordError)
	}
	if !strings.Contains(out.RecordError.Error(), "worker-recovered") {
		t.Fatalf("丢失错误没说明记录现在的归属: %v", out.RecordError)
	}
	if out.Record.WorkerID != "worker-recovered" {
		t.Fatalf("应报回存储里真实的归属: %q", out.Record.WorkerID)
	}
	// 读到归属已换就该收手：连一次写入尝试都不该发生。
	writes, rejects := store.counts()
	if writes != writesAtFlip || rejects != 0 {
		t.Fatalf("丢了所有权还试图写记录：写入 %d→%d，被拒 %d 次", writesAtFlip, writes, rejects)
	}

	// 旧 epoch 一次都没写进记录：还停在让出之后的样子。
	stored := managedGet(t, durable, created.RunID)
	if stored.Status != runstore.StatusRunning || stored.WorkerID != "worker-A" ||
		stored.WorkerEpoch != 1 || stored.Version != owned.Version {
		t.Fatalf("旧 worker 改写了已让出的记录: %+v", stored)
	}
	// checkpoint 终态不由它补：取消让运行自己收尾，但不许出现 Completion 记录。
	if count := terminalCount(t, cps, created.ThreadID); count != 0 {
		t.Fatalf("丢所有权后仍写出 %d 条 checkpoint 终态", count)
	}
	// 循环随投递一起退出：不再有心跳进来。
	time.Sleep(50 * time.Millisecond)
	if later := managedGet(t, durable, created.RunID); later.Version != stored.Version {
		t.Fatalf("循环没退出: %d → %d", stored.Version, later.Version)
	}
}

// outageStore 让任务存储读写临时全失败，用来区分"抖一下"和"整个租期都联系不上"。
type outageStore struct {
	runstore.Store
	down   atomic.Bool
	errMsg string
}

func (s *outageStore) Get(ctx context.Context, runID string) (runstore.RunRecord, error) {
	if s.down.Load() {
		return runstore.RunRecord{}, errors.New(s.errMsg)
	}
	return s.Store.Get(ctx, runID)
}

func (s *outageStore) CompareAndSwap(ctx context.Context, runID string, expected uint64, next runstore.RunRecord) (runstore.RunRecord, error) {
	if s.down.Load() {
		return runstore.RunRecord{}, errors.New(s.errMsg)
	}
	return s.Store.CompareAndSwap(ctx, runID, expected, next)
}

// TestWorkerLeaseLapseStopsPublishing：存储抖动一次都不许改状态；联系不上超过一整个租期，
// 这条投递就停止发布，之后存储恢复也不再补写。
func TestWorkerLeaseLapseStopsPublishing(t *testing.T) {
	ctx := context.Background()
	cps := checkpoint.NewMemory()
	durable := runstore.NewMemory()
	outage := &outageStore{Store: durable, errMsg: "任务存储不可用"}

	w := managedWorker(t, managedAgent(t, cps, "回答", nil), outage, "worker-A",
		WithWorkerLease(time.Second),
		WithHeartbeatInterval(200*time.Millisecond),
		WithCancelPollInterval(100*time.Millisecond))
	now := heartbeatTestStart
	w.now = func() time.Time { return now }

	created := managedCreate(t, durable, managedRecord("run-lease-lapse", "thread-lease-lapse", "turn-1"))
	claimed, err := w.claim(ctx, created)
	if err != nil {
		t.Fatalf("领取失败: %v", err)
	}
	watch := w.newTaskWatch(claimed)
	noop := func() {}

	// 抖动 300 毫秒（不足一个租期）：错误留着，但不认输。
	outage.down.Store(true)
	for i := 0; i < 3; i++ {
		now = now.Add(100 * time.Millisecond)
		watch.tick(ctx, noop)
	}
	if lost := watch.ownershipLost(); lost != nil {
		t.Fatalf("不足一个租期的抖动就认输了: %v", lost)
	}
	if _, failures, lastErr := watch.stats(); failures < 3 || lastErr == nil {
		t.Fatalf("瞬时写失败没被保留: failures=%d err=%v", failures, lastErr)
	}
	if after := managedGet(t, durable, created.RunID); after.Version != claimed.Version || after.Status != runstore.StatusRunning {
		t.Fatalf("抖动期间记录被动过: %+v", after)
	}

	// 恢复后心跳继续。
	outage.down.Store(false)
	now = now.Add(200 * time.Millisecond)
	watch.tick(ctx, noop)
	recovered := managedGet(t, durable, created.RunID)
	if recovered.Version != claimed.Version+1 || !recovered.HeartbeatAt.Equal(now) {
		t.Fatalf("恢复后没补上心跳: %+v, 期望心跳 %v", recovered, now)
	}
	if heartbeats, _, _ := watch.stats(); heartbeats != 1 {
		t.Fatalf("心跳次数 = %d, 期望 1", heartbeats)
	}

	// 超过一整个租期联系不上：停止发布，并且这段时间一次写都没成。
	outage.down.Store(true)
	for i := 0; i < 20; i++ {
		now = now.Add(100 * time.Millisecond)
		watch.tick(ctx, noop)
	}
	lost := watch.ownershipLost()
	if lost == nil {
		t.Fatal("联系不上超过租期应停止发布")
	}
	if !strings.Contains(lost.Error(), "任务存储不可用") {
		t.Fatalf("丢失错误没带上存储失败的原因: %v", lost)
	}
	if after := managedGet(t, durable, created.RunID); after.Version != recovered.Version {
		t.Fatalf("故障期间记录被推进了: %d → %d", recovered.Version, after.Version)
	}

	// 认输之后存储恢复也不再补写：这条投递不再相信自己拥有记录。
	outage.down.Store(false)
	for i := 0; i < 5; i++ {
		now = now.Add(200 * time.Millisecond)
		watch.tick(ctx, noop)
	}
	if after := managedGet(t, durable, created.RunID); after.Version != recovered.Version {
		t.Fatalf("认输后还在写心跳: %d → %d", recovered.Version, after.Version)
	}
	if count := terminalCount(t, cps, created.ThreadID); count != 0 {
		t.Fatalf("心跳循环写出了 %d 条 checkpoint 终态", count)
	}
}

// TestRunHeartbeatConstruction 检查心跳间隔的默认值与启动校验。
func TestRunHeartbeatConstruction(t *testing.T) {
	cps := checkpoint.NewMemory()
	store := runstore.NewMemory()
	a := managedAgent(t, cps, "回答", nil)

	w := managedWorker(t, a, store, "w")
	if w.heartbeat != defaultHeartbeatInterval || w.cancelPoll != defaultCancelPollInterval || w.lease != managedRunLease {
		t.Fatalf("默认间隔 = 心跳 %v / 取消轮询 %v / 租期 %v", w.heartbeat, w.cancelPoll, w.lease)
	}
	if w.lease < time.Duration(minHeartbeatsPerLease)*w.heartbeat {
		t.Fatalf("默认租期 %v 覆盖不了 %d 次心跳 %v", w.lease, minHeartbeatsPerLease, w.heartbeat)
	}

	for name, err := range map[string]error{
		"zero heartbeat": func() error {
			_, e := NewManagedRunWorker(a, store, "w", WithHeartbeatInterval(0))
			return e
		}(),
		"negative heartbeat": func() error {
			_, e := NewManagedRunWorker(a, store, "w", WithHeartbeatInterval(-time.Second))
			return e
		}(),
		"lease shorter than 3 heartbeats": func() error {
			_, e := NewManagedRunWorker(a, store, "w",
				WithHeartbeatInterval(time.Second), WithWorkerLease(2999*time.Millisecond))
			return e
		}(),
	} {
		if err == nil {
			t.Fatalf("%s 应构造失败", name)
		}
	}

	// 刚好覆盖三次心跳的租期要通过。
	if _, err := NewManagedRunWorker(a, store, "w",
		WithHeartbeatInterval(time.Second), WithWorkerLease(3*time.Second)); err != nil {
		t.Fatalf("三次心跳的租期应通过: %v", err)
	}
}
