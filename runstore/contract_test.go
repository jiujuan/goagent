package runstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// The contract tests run against every implementation, so the Memory and the durable File
// store are held to one lifecycle rather than two hand-written expectations.
type storeFactory struct {
	name string
	make func(t *testing.T) Store
}

func storeFactories() []storeFactory {
	return []storeFactory{
		{"memory", func(t *testing.T) Store { return NewMemory() }},
		{"file", func(t *testing.T) Store { return newFileStore(t, t.TempDir()) }},
	}
}

// stubClock pins the store clock for one test. The clock is a package injection point,
// so these tests must not run in parallel.
func stubClock(t *testing.T, base time.Time, step time.Duration) func() int {
	t.Helper()
	_previous := storageClock
	calls := 0
	storageClock = func() time.Time {
		calls++
		return base.Add(time.Duration(calls) * step)
	}
	t.Cleanup(func() { storageClock = _previous })
	return func() int { return calls }
}

func testBase() time.Time { return time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC) }

func settledResult(text string) *core.Result {
	return &core.Result{Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{core.Text{Text: text}}}}
}

// recordIn returns the canonical record for a status: the same status always produces the
// same content, so a terminal retry built here is genuinely identical to the stored one.
func recordIn(runID string, status Status, when time.Time) RunRecord {
	r := RunRecord{
		RunID:    runID,
		ThreadID: "thread-" + runID,
		TurnID:   "turn-" + runID,
		Status:   status,
	}
	switch status {
	case StatusCancelling, StatusCancelled:
		r.CancelRequestedAt = when
	case StatusFailed:
		r.Error = &RunError{Code: ErrCodeExecutionFailed, Message: "tool crashed"}
	case StatusSucceeded:
		r.FinishReason = FinishReasonCompleted
		r.Result = settledResult("settled answer")
		r.LatestCheckpointRef = &checkpoint.Ref{ThreadID: r.ThreadID, CheckpointID: "cp-settled"}
		r.CheckpointRevision = 7
	case StatusPaused:
		r.FinishReason = FinishReasonPaused
		r.LatestCheckpointRef = &checkpoint.Ref{ThreadID: r.ThreadID, CheckpointID: "cp-pause"}
		r.CheckpointRevision = 3
	}
	return r
}

func mustCreate(t *testing.T, s Store, r RunRecord) RunRecord {
	t.Helper()
	created, err := s.Create(context.Background(), r)
	if err != nil {
		t.Fatalf("Create(%q) 失败: %v", r.RunID, err)
	}
	if created.Version != 1 {
		t.Fatalf("Create(%q) 的 Version = %d, 期望 1", r.RunID, created.Version)
	}
	return created
}

func getRecord(t *testing.T, s Store, runID string) RunRecord {
	t.Helper()
	record, err := s.Get(context.Background(), runID)
	if err != nil {
		t.Fatalf("Get(%q) 失败: %v", runID, err)
	}
	return record
}

// cas writes the whole record back after changing it: CompareAndSwap replaces a record,
// so a caller restates every field it wants kept. This is how the workers in later tasks
// will write, and it is what makes "the store never trusts a caller's shape" testable.
func cas(t *testing.T, s Store, runID string, expectedVersion uint64, mutate func(*RunRecord)) (RunRecord, error) {
	t.Helper()
	next := getRecord(t, s, runID)
	if mutate != nil {
		mutate(&next)
	}
	return s.CompareAndSwap(context.Background(), runID, expectedVersion, next)
}

// TestRunTransitions 穷举 ADR-0034 D4 的状态边：允许的边写入成功并推进 Version，
// 其余一律拒绝；终态记录只接受同内容重试。
func TestRunTransitions(t *testing.T) {
	allStatuses := []Status{
		StatusQueued, StatusRunning, StatusCancelling,
		StatusPaused, StatusSucceeded, StatusFailed, StatusCancelled,
	}
	// 独立重述 D4 允许的边，而不是复用实现里的表，否则测试只是在自我确认。
	allowed := map[Status][]Status{
		StatusQueued: {
			// 入队重试保持 queued；投递确定失败时 queued 直接落 failed。
			StatusQueued, StatusRunning, StatusCancelled, StatusFailed,
		},
		StatusRunning:    {StatusRunning, StatusPaused, StatusSucceeded, StatusFailed, StatusCancelling},
		StatusCancelling: {StatusCancelling, StatusCancelled, StatusPaused, StatusSucceeded, StatusFailed},
		// 终态只结束本次 Run，Resume 另建新记录。
		StatusPaused: nil, StatusSucceeded: nil, StatusFailed: nil, StatusCancelled: nil,
	}
	isAllowed := func(from, to Status) bool {
		for _, s := range allowed[from] {
			if s == to {
				return true
			}
		}
		return false
	}

	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			for _, from := range allStatuses {
				for _, to := range allStatuses {
					runID := "edge-" + string(from) + "-" + string(to)
					ctx := context.Background()
					store := f.make(t)
					created := mustCreate(t, store, recordIn(runID, from, testBase()))

					next := recordIn(runID, to, testBase().Add(30*time.Minute))
					updated, err := store.CompareAndSwap(ctx, runID, created.Version, next)
					switch {
					case from.Terminal() && from == to:
						// 同内容重试：幂等返回原记录，Version 不动。
						if err != nil {
							t.Fatalf("%s→%s 同内容重试应幂等: %v", from, to, err)
						}
						if updated.Version != created.Version {
							t.Fatalf("%s→%s 重试推进了 Version: %d → %d", from, to, created.Version, updated.Version)
						}
					case from.Terminal():
						if !errors.Is(err, ErrInvalidTransition) {
							t.Fatalf("%s→%s 终态改写应 ErrInvalidTransition, got %v", from, to, err)
						}
					case isAllowed(from, to):
						if err != nil {
							t.Fatalf("%s→%s 应允许: %v", from, to, err)
						}
						if updated.Version != created.Version+1 {
							t.Fatalf("%s→%s Version = %d, 期望 %d", from, to, updated.Version, created.Version+1)
						}
						stored := getRecord(t, store, runID)
						if stored.Status != to {
							t.Fatalf("%s→%s 落库状态 = %q", from, to, stored.Status)
						}
						if !stored.CreatedAt.Equal(created.CreatedAt) {
							t.Fatalf("%s→%s 改动了创建时间: %v → %v", from, to, created.CreatedAt, stored.CreatedAt)
						}
					default:
						if !errors.Is(err, ErrInvalidTransition) {
							t.Fatalf("%s→%s 应拒绝为 ErrInvalidTransition, got %v", from, to, err)
						}
						if stored := getRecord(t, store, runID); stored.Status != from {
							t.Fatalf("%s→%s 被拒绝却改成了 %q", from, to, stored.Status)
						}
					}
				}
			}
		})
	}
}

// TestRunStatusTableIsClosed 钉住状态表本身：七个状态都有行，终态行没有出路，
// 非终态才有自环。存储层的终态闸门之外，这张表是第二道保险。
func TestRunStatusTableIsClosed(t *testing.T) {
	all := []Status{
		StatusQueued, StatusRunning, StatusCancelling,
		StatusPaused, StatusSucceeded, StatusFailed, StatusCancelled,
	}
	for _, s := range all {
		row, ok := allowedTransitions[s]
		if !ok {
			t.Fatalf("状态 %q 在状态表里没有行", s)
		}
		if s.Terminal() {
			if len(row) != 0 {
				t.Fatalf("终态 %q 还有出路: %v", s, row)
			}
			continue
		}
		if !row[s] {
			t.Fatalf("非终态 %q 少了自环（心跳与入队重放都要用）", s)
		}
		for target := range row {
			if !validStatus(target) {
				t.Fatalf("%q 的出路里有未知状态 %q", s, target)
			}
		}
	}
	for _, s := range []Status{Status("exploded"), Status("")} {
		if validStatus(s) {
			t.Fatalf("%q 被当成了合法状态", s)
		}
	}
	if len(allowedTransitions) != len(all) {
		t.Fatalf("状态表有 %d 行，与已知状态数 %d 不符", len(allowedTransitions), len(all))
	}
}

// TestRunTerminalRetryWithDifferentOutcome 证明终态幂等只认同一结局，换个答案就是改写。
func TestRunTerminalRetryWithDifferentOutcome(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			ctx := context.Background()
			store := f.make(t)
			created := mustCreate(t, store, recordIn("done", StatusSucceeded, testBase()))

			rewrite := recordIn("done", StatusSucceeded, testBase())
			rewrite.Result = settledResult("另一个答案")
			if _, err := store.CompareAndSwap(ctx, "done", created.Version, rewrite); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("换结果的终态重试 = %v, want ErrInvalidTransition", err)
			}

			// 迟到取消也不能改写已冻结的成功。
			late := recordIn("done", StatusCancelled, testBase())
			if _, err := store.CompareAndSwap(ctx, "done", created.Version, late); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("终态上的迟到取消 = %v, want ErrInvalidTransition", err)
			}
			if stored := getRecord(t, store, "done"); stored.Result.Message.Parts[0].(core.Text).Text != "settled answer" {
				t.Fatalf("终态结果被改写: %+v", stored.Result)
			}
		})
	}
}

// TestRunIdentityIsImmutable 证明 CAS 不能换掉记录的身份与创建时间。
func TestRunIdentityIsImmutable(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*RunRecord)
	}{
		{"thread", func(r *RunRecord) { r.ThreadID = "other-thread" }},
		{"turn", func(r *RunRecord) { r.TurnID = "other-turn" }},
		{"job", func(r *RunRecord) { r.JobID = "job-late" }},
		{"resume_of", func(r *RunRecord) { r.ResumeOfRunID = "run-late" }},
		{"created_at", func(r *RunRecord) { r.CreatedAt = testBase().Add(24 * time.Hour) }},
	}
	for _, f := range storeFactories() {
		f := f
		for _, m := range mutations {
			t.Run(f.name+"/"+m.name, func(t *testing.T) {
				stubClock(t, testBase(), time.Hour)
				ctx := context.Background()
				store := f.make(t)
				base := recordIn("ident", StatusQueued, testBase())
				base.JobID = "job-1"
				base.ResumeOfRunID = "run-0"
				created := mustCreate(t, store, base)

				// 除被改坏的这一处，记录其余身份字段原样带过去。
				next := recordIn("ident", StatusRunning, testBase())
				next.JobID, next.ResumeOfRunID = "job-1", "run-0"
				m.mutate(&next)
				if _, err := store.CompareAndSwap(ctx, "ident", created.Version, next); !errors.Is(err, ErrConflict) {
					t.Fatalf("改写 %s 应 ErrConflict, got %v", m.name, err)
				}
				if stored := getRecord(t, store, "ident"); stored.Status != StatusQueued || stored.Version != 1 {
					t.Fatalf("被拒绝的改写仍落库了: %+v", stored)
				}
			})
		}
	}
}

// TestRunCancelRaceKeepsFrozenOutcome 复现取消与完成的竞争：取消先落库后完成仍可胜出，
// 完成冻结之后的取消只能被拒绝。
func TestRunCancelRaceKeepsFrozenOutcome(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			store := f.make(t)
			created := mustCreate(t, store, recordIn("race", StatusRunning, testBase()))

			cancelAt := testBase().Add(time.Hour)
			cancelling, err := cas(t, store, "race", created.Version, func(r *RunRecord) {
				r.Status = StatusCancelling
				r.CancelRequestedAt = cancelAt
			})
			if err != nil {
				t.Fatalf("running→cancelling 应允许: %v", err)
			}
			if cancelling.WorkerID != "" {
				t.Fatalf("取消请求不该带来 owner: %+v", cancelling)
			}

			// 工作已经把结局算完：cancelling→succeeded 保留取消时间并冻结成功。
			done, err := cas(t, store, "race", cancelling.Version, func(r *RunRecord) {
				r.Status = StatusSucceeded
				r.FinishReason = FinishReasonCompleted
				r.Result = settledResult("已经产出")
			})
			if err != nil {
				t.Fatalf("cancelling→succeeded 应允许: %v", err)
			}
			if !done.CancelRequestedAt.Equal(cancelAt) {
				t.Fatalf("冻结成功时丢了取消时间: %v", done.CancelRequestedAt)
			}

			// 迟到的取消请求不能再把它改成 cancelled。
			if _, err := cas(t, store, "race", done.Version, func(r *RunRecord) {
				r.Status = StatusCancelled
				r.CancelRequestedAt = cancelAt.Add(time.Minute)
			}); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("迟到取消应 ErrInvalidTransition, got %v", err)
			}
			stored := getRecord(t, store, "race")
			if stored.Status != StatusSucceeded || !stored.CancelRequestedAt.Equal(cancelAt) {
				t.Fatalf("冻结结局被迟到取消改动: %+v", stored)
			}
		})
	}
}

// TestRunStoreCASConflict 覆盖 CAS 的版本与存在性契约。
func TestRunStoreCASConflict(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			ctx := context.Background()
			store := f.make(t)
			created := mustCreate(t, store, recordIn("cas", StatusQueued, testBase()))
			toRunning := recordIn("cas", StatusRunning, testBase())

			// 未知 RunID：CAS 不创建。
			if _, err := store.CompareAndSwap(ctx, "missing", 1, recordIn("missing", StatusRunning, testBase())); !errors.Is(err, ErrNotFound) {
				t.Fatalf("未知 RunID = %v, want ErrNotFound", err)
			}
			// expectedVersion 为 0：版本从 1 起，没读过记录的调用方同样只能拿到冲突。
			if _, err := store.CompareAndSwap(ctx, "cas", 0, toRunning); !errors.Is(err, ErrConflict) {
				t.Fatalf("版本 0 = %v, want ErrConflict", err)
			}
			// 版本比存储新也不行：冲突只按相等判断。
			if _, err := store.CompareAndSwap(ctx, "cas", created.Version+5, toRunning); !errors.Is(err, ErrConflict) {
				t.Fatalf("超前版本 = %v, want ErrConflict", err)
			}
			// CAS 里的 RunID 与目标不一致。
			mismatched := toRunning
			mismatched.RunID = "other"
			if _, err := store.CompareAndSwap(ctx, "cas", created.Version, mismatched); !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("RunID 不一致 = %v, want ErrInvalidRecord", err)
			}
			// 空 RunID 不能创建。
			if _, err := store.Create(ctx, RunRecord{ThreadID: "t", Status: StatusQueued}); !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("空 RunID Create = %v, want ErrInvalidRecord", err)
			}
			// 抢在他人之后再用旧版本写一次：内容合法，只有版本不对，所以必须冲突。
			running, err := cas(t, store, "cas", created.Version, func(r *RunRecord) { r.Status = StatusRunning })
			if err != nil {
				t.Fatalf("queued→running 应允许: %v", err)
			}
			if _, err := store.CompareAndSwap(ctx, "cas", created.Version, toRunning); !errors.Is(err, ErrConflict) {
				t.Fatalf("旧版本重放 = %v, want ErrConflict", err)
			}
			// 状态机拒绝的写不改变 Version。
			if _, err := cas(t, store, "cas", running.Version, func(r *RunRecord) { r.Status = StatusQueued }); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("running→queued 应 ErrInvalidTransition, got %v", err)
			}
			if again := getRecord(t, store, "cas"); again.Version != running.Version {
				t.Fatalf("被拒绝的写推进了 Version: %d → %d", running.Version, again.Version)
			}
		})
	}
}

// TestRunStoreCreateIdempotent 证明同 ID 同内容重放不会重复记录，也不会推进版本。
func TestRunStoreCreateIdempotent(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			ctx := context.Background()
			store := f.make(t)
			first := recordIn("dup", StatusQueued, testBase())
			first.JobID = "job-1"
			created := mustCreate(t, store, first)

			// 重放：内容相同，时间戳由存储决定，所以第二次连 CreatedAt 都不带。
			replay := first
			replay.Version = created.Version
			replay.CreatedAt = created.CreatedAt
			again, err := store.Create(ctx, replay)
			if err != nil {
				t.Fatalf("同内容重放应幂等: %v", err)
			}
			if again.Version != 1 || !again.CreatedAt.Equal(created.CreatedAt) || !again.UpdatedAt.Equal(created.UpdatedAt) {
				t.Fatalf("重放改了存储事实: %+v vs %+v", again, created)
			}

			// 异内容冲突。
			for name, mutate := range map[string]func(*RunRecord){
				"thread": func(r *RunRecord) { r.ThreadID = "other" },
				"status": func(r *RunRecord) { r.Status = StatusRunning },
				"job":    func(r *RunRecord) { r.JobID = "job-2" },
			} {
				other := first
				mutate(&other)
				if _, err := store.Create(ctx, other); !errors.Is(err, ErrConflict) {
					t.Fatalf("改 %s 的重放应 ErrConflict, got %v", name, err)
				}
			}

			// 已被 worker 推进过之后，队列重放的 queued 不再是同内容。
			if _, err := cas(t, store, "dup", 1, func(r *RunRecord) { r.Status = StatusRunning }); err != nil {
				t.Fatalf("queued→running 应允许: %v", err)
			}
			if _, err := store.Create(ctx, first); !errors.Is(err, ErrConflict) {
				t.Fatalf("运行中记录上的 queued 重放应 ErrConflict, got %v", err)
			}
		})
	}
}

// TestRunStoreVersionsAndClock 钉住 Version 从 1 起、UpdatedAt 来自存储时钟、
// CreatedAt 全程不变，并且它与 checkpoint.Revision 各自独立。
func TestRunStoreVersionsAndClock(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			ticks := stubClock(t, testBase(), time.Hour)
			store := f.make(t)
			base := recordIn("clock", StatusQueued, testBase())
			created := mustCreate(t, store, base)
			if got := ticks(); got != 1 {
				t.Fatalf("Create 应只取一次时钟, 取了 %d 次", got)
			}
			wantCreated := testBase().Add(time.Hour)
			if !created.CreatedAt.Equal(wantCreated) || !created.UpdatedAt.Equal(wantCreated) {
				t.Fatalf("Create 时间戳 = created %v updated %v, 期望都是 %v", created.CreatedAt, created.UpdatedAt, wantCreated)
			}

			running, err := cas(t, store, "clock", created.Version, func(r *RunRecord) { r.Status = StatusRunning })
			if err != nil {
				t.Fatalf("queued→running 应允许: %v", err)
			}
			if running.Version != 2 {
				t.Fatalf("Version = %d, 期望 2", running.Version)
			}
			if !running.CreatedAt.Equal(created.CreatedAt) {
				t.Fatalf("UpdatedAt 推进却动了 CreatedAt: %v → %v", created.CreatedAt, running.CreatedAt)
			}
			if !running.UpdatedAt.After(running.CreatedAt) {
				t.Fatalf("UpdatedAt 应该来自更晚的时钟: %v", running.UpdatedAt)
			}

			// 调用方自带的 checkpoint 边界只是记录里的引用，Version 不受它影响。
			next, err := cas(t, store, "clock", running.Version, func(r *RunRecord) {
				r.LatestCheckpointRef = &checkpoint.Ref{ThreadID: "thread-clock", CheckpointID: "cp-1"}
				r.CheckpointRevision = 42
			})
			if err != nil {
				t.Fatalf("自环心跳应允许: %v", err)
			}
			if next.Version != 3 || next.CheckpointRevision != 42 {
				t.Fatalf("记录版本 %d 与 checkpoint revision %d 混了: %+v", next.Version, next.CheckpointRevision, next)
			}
		})
	}
}

// TestRunStoreCopyIsolation 证明出入口都不共享内存：调用方改进来的记录、拿到手的记录、
// 还有 Create 返回值，都碰不到存储里的那一份。
func TestRunStoreCopyIsolation(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			store := f.make(t)

			payload := []byte{1, 2, 3}
			input := RunRecord{
				RunID:               "iso",
				ThreadID:            "thread-iso",
				Status:              StatusQueued,
				JobID:               "job-iso",
				LatestCheckpointRef: &checkpoint.Ref{ThreadID: "thread-iso", CheckpointID: "cp-1"},
				Result: &core.Result{Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
					core.Image{Data: payload},
				}}},
				Error:       &RunError{Code: ErrCodeEnqueueFailed},
				RecordError: &RunError{Code: ErrCodeDeadlineExceeded},
			}
			created := mustCreate(t, store, input)

			// 调用方留着原对象继续改。
			payload[0] = 9
			*input.LatestCheckpointRef = checkpoint.Ref{ThreadID: "x", CheckpointID: "y"}
			input.Error.Code = "mutated"
			input.RecordError.Code = "mutated"
			input.Result.Message.Parts[0].(core.Image).Data[1] = 9

			roundtripStore(t, store, "iso")

			// 出口对象同样改不动存储。
			exported := getRecord(t, store, "iso")
			exported.Result.Message.Parts[0].(core.Image).Data[2] = 7
			*exported.LatestCheckpointRef = checkpoint.Ref{ThreadID: "z", CheckpointID: "w"}
			exported.Error.Code = "mutated"
			exported.RecordError.Code = "mutated"
			exported.JobID = "mutated"
			roundtripStore(t, store, "iso")

			// 写入接口返回的记录也是副本，顺着它的指针改不动存储。
			created.Error.Code = "mutated"
			created.RecordError.Code = "mutated"
			*created.LatestCheckpointRef = checkpoint.Ref{ThreadID: "c", CheckpointID: "d"}
			created.Result.Message.Parts[0].(core.Image).Data[0] = 5
			created.JobID = "mutated"
			roundtripStore(t, store, "iso")
		})
	}
}

// TestRunCASReturnsCopies 证明 CompareAndSwap 返回的记录也是副本：顺着它的指针改，
// 改不动存储里那一份。
func TestRunCASReturnsCopies(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			store := f.make(t)
			created := mustCreate(t, store, recordIn("wcopy", StatusQueued, testBase()))

			// 存储里先放一条带指针的记录，再取回 CAS 的返回对象并改它。
			withRefs, err := cas(t, store, "wcopy", created.Version, func(r *RunRecord) {
				r.Status = StatusRunning
				r.LatestCheckpointRef = &checkpoint.Ref{ThreadID: "thread-wcopy", CheckpointID: "cp-live"}
				r.CompletionRef = &checkpoint.Ref{ThreadID: "thread-wcopy", CheckpointID: "cp-done"}
				r.RecordError = &RunError{Code: ErrCodeDispatchUnknown}
			})
			if err != nil {
				t.Fatalf("queued→running 应允许: %v", err)
			}
			withRefs.LatestCheckpointRef.CheckpointID = "mutated"
			withRefs.CompletionRef.CheckpointID = "mutated"
			withRefs.RecordError.Code = "mutated"
			stored := getRecord(t, store, "wcopy")
			if stored.LatestCheckpointRef.CheckpointID != "cp-live" ||
				stored.CompletionRef.CheckpointID != "cp-done" ||
				stored.RecordError.Code != ErrCodeDispatchUnknown {
				t.Fatalf("CAS 返回值与存储共享了指针: %+v", stored)
			}
		})
	}
}

func roundtripStore(t *testing.T, store Store, runID string) {
	t.Helper()
	record := getRecord(t, store, runID)
	if record.Result.Message.Parts[0].(core.Image).Data[0] != 1 ||
		record.Result.Message.Parts[0].(core.Image).Data[1] != 2 ||
		record.Result.Message.Parts[0].(core.Image).Data[2] != 3 {
		t.Fatalf("结果消息与存储共享了内存: %v", record.Result.Message.Parts[0].(core.Image).Data)
	}
	if id := record.LatestCheckpointRef.CheckpointID; id != "cp-1" {
		t.Fatalf("checkpoint 引用被外部改写: %q", id)
	}
	if record.Error.Code != ErrCodeEnqueueFailed || record.RecordError.Code != ErrCodeDeadlineExceeded {
		t.Fatalf("错误对象被外部改写: %+v %+v", record.Error, record.RecordError)
	}
	if record.JobID != "job-iso" {
		t.Fatalf("身份字段被外部改写: %q", record.JobID)
	}
}

// TestRunWorkerEpoch 覆盖 worker 所有权的校验：声明必须成对、只能一次、心跳不清取消请求。
func TestRunWorkerEpoch(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			store := f.make(t)
			created := mustCreate(t, store, recordIn("worker", StatusQueued, testBase()))

			// 声明所有权必须 worker+epoch 成对。
			if _, err := cas(t, store, "worker", created.Version, func(r *RunRecord) {
				r.Status = StatusRunning
				r.WorkerID = "w1"
			}); !errors.Is(err, ErrOwnershipLost) {
				t.Fatalf("只给 worker = %v, want ErrOwnershipLost", err)
			}

			claimed, err := cas(t, store, "worker", created.Version, func(r *RunRecord) {
				r.Status = StatusRunning
				r.WorkerID = "w1"
				r.WorkerEpoch = 1
				r.WorkerLeaseUntil = testBase().Add(30 * time.Minute)
			})
			if err != nil {
				t.Fatalf("首次领取应成功: %v", err)
			}
			if claimed.WorkerID != "w1" || claimed.WorkerEpoch != 1 {
				t.Fatalf("领取没落到存储: %+v", claimed)
			}

			// 旧 epoch、别的 worker、只改名不改 epoch 都写不进去。
			for name, attempt := range map[string]func(*RunRecord){
				"stale epoch":  func(r *RunRecord) { r.WorkerEpoch = 9 },
				"other worker": func(r *RunRecord) { r.WorkerID = "w2" },
				"worker only": func(r *RunRecord) {
					r.WorkerID = "w2"
					r.WorkerEpoch = 0
				},
			} {
				if _, err := cas(t, store, "worker", claimed.Version, attempt); !errors.Is(err, ErrOwnershipLost) {
					t.Fatalf("%s 应 ErrOwnershipLost, got %v", name, err)
				}
			}

			// 心跳由 owner 发起，状态不变、版本前进、取消请求仍在。
			cancelAt := testBase().Add(2 * time.Hour)
			cancelling, err := cas(t, store, "worker", claimed.Version, func(r *RunRecord) {
				r.Status = StatusCancelling
				r.CancelRequestedAt = cancelAt
			})
			if err != nil {
				t.Fatalf("running→cancelling 应允许: %v", err)
			}
			beat, err := cas(t, store, "worker", cancelling.Version, func(r *RunRecord) {
				r.HeartbeatAt = testBase().Add(3 * time.Hour)
				// 心跳重新构造记录时把取消时间写丢了。
				r.CancelRequestedAt = time.Time{}
			})
			if err != nil {
				t.Fatalf("owner 心跳应允许: %v", err)
			}
			if !beat.CancelRequestedAt.Equal(cancelAt) {
				t.Fatalf("心跳清掉了取消请求: %v", beat.CancelRequestedAt)
			}

			// 取消请求不能倒退。
			if _, err := cas(t, store, "worker", beat.Version, func(r *RunRecord) {
				r.CancelRequestedAt = cancelAt.Add(-time.Minute)
			}); !errors.Is(err, ErrConflict) {
				t.Fatalf("取消时间倒退应 ErrConflict, got %v", err)
			}
			// 非 owner 的写（比如协调器）保留原 owner。
			reconciled, err := cas(t, store, "worker", beat.Version, func(r *RunRecord) {
				r.WorkerID = ""
				r.WorkerEpoch = 0
			})
			if err != nil {
				t.Fatalf("非 owner 的同状态写应允许: %v", err)
			}
			if reconciled.WorkerID != "w1" || reconciled.WorkerEpoch != 1 {
				t.Fatalf("非 owner 的写丢了所有权: %+v", reconciled)
			}
			// 心跳不能倒退。
			if _, err := cas(t, store, "worker", reconciled.Version, func(r *RunRecord) {
				r.HeartbeatAt = testBase().Add(1 * time.Hour)
			}); !errors.Is(err, ErrConflict) {
				t.Fatalf("过期心跳应 ErrConflict, got %v", err)
			}
		})
	}
}

// TestRunConcurrentSameVersionOnlyOneWins 证明同一版本并发写只有一个成功。
func TestRunConcurrentSameVersionOnlyOneWins(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			ctx := context.Background()
			store := f.make(t)
			created := mustCreate(t, store, recordIn("contend", StatusQueued, testBase()))

			const racers = 8
			start := make(chan struct{})
			var wg sync.WaitGroup
			results := make([]error, racers)
			for i := 0; i < racers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					// 每个竞争者都提交同样合法的领取：内容不出错，输赢只由版本决定。
					attempt := created
					attempt.Status = StatusRunning
					attempt.WorkerID = "w1"
					attempt.WorkerEpoch = 1
					<-start
					_, err := store.CompareAndSwap(ctx, "contend", created.Version, attempt)
					results[i] = err
				}(i)
			}
			close(start)
			wg.Wait()

			winners := 0
			for _, err := range results {
				if err == nil {
					winners++
				} else if !errors.Is(err, ErrConflict) {
					t.Fatalf("竞争失败方应只报版本冲突: %v", err)
				}
			}
			if winners != 1 {
				t.Fatalf("同版本 %d 个写成功了 %d 个, 期望 1", racers, winners)
			}
			stored := getRecord(t, store, "contend")
			if stored.Version != 2 || stored.Status != StatusRunning {
				t.Fatalf("竞争后的存储状态不对: %+v", stored)
			}
			if stored.WorkerEpoch == 0 {
				t.Fatalf("赢家没写下所有权: %+v", stored)
			}
		})
	}
}

// TestRunCASRestatesWholeRecord 钉住 CAS 的"整条重写"契约：调用方漏掉的普通字段会被丢掉，
// 而存储自己维护的取消请求、所有权与创建时间不会因此丢失。
func TestRunCASRestatesWholeRecord(t *testing.T) {
	for _, f := range storeFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			stubClock(t, testBase(), time.Hour)
			ctx := context.Background()
			store := f.make(t)
			created := mustCreate(t, store, recordIn("restate", StatusQueued, testBase()))

			full, err := cas(t, store, "restate", created.Version, func(r *RunRecord) {
				r.Status = StatusRunning
				r.WorkerID = "w1"
				r.WorkerEpoch = 1
				r.HeartbeatAt = testBase().Add(time.Hour)
				r.WorkerLeaseUntil = testBase().Add(2 * time.Hour)
			})
			if err != nil {
				t.Fatalf("queued→running 应允许: %v", err)
			}

			// 只改状态，其余照抄读到的记录。
			again, err := store.CompareAndSwap(ctx, "restate", full.Version, full)
			if err != nil {
				t.Fatalf("原样重写应允许: %v", err)
			}
			if again.HeartbeatAt != full.HeartbeatAt || again.WorkerLeaseUntil != full.WorkerLeaseUntil {
				t.Fatalf("原样重写丢了活性字段: %+v", again)
			}

			// 漏掉租约就真的没了，但 owner、心跳与创建时间由存储守住。
			bare := full
			bare.WorkerLeaseUntil = time.Time{}
			dropped, err := store.CompareAndSwap(ctx, "restate", again.Version, bare)
			if err != nil {
				t.Fatalf("漏字段的重写应允许: %v", err)
			}
			if !dropped.WorkerLeaseUntil.IsZero() {
				t.Fatalf("调用方漏掉的租约被偷偷保留了: %+v", dropped)
			}
			if dropped.WorkerID != "w1" || dropped.WorkerEpoch != 1 {
				t.Fatalf("漏字段的重写丢了 owner: %+v", dropped)
			}
			if dropped.HeartbeatAt != full.HeartbeatAt {
				t.Fatalf("心跳不该被重写丢掉: %+v", dropped)
			}
			if !dropped.CreatedAt.Equal(created.CreatedAt) {
				t.Fatalf("漏字段的重写丢了创建时间: %v", dropped.CreatedAt)
			}
		})
	}
}

// TestRunStoreShapeErrors 覆盖结构不合法的写：状态未知、失败无错误码、取消无请求时间。
func TestRunStoreShapeErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*RunRecord)
	}{
		{"未知状态", func(r *RunRecord) { r.Status = Status("exploded") }},
		{"失败无错误", func(r *RunRecord) { r.Status = StatusFailed; r.Error = nil }},
		{"失败无码值", func(r *RunRecord) { r.Status = StatusFailed; r.Error = &RunError{Message: "no code"} }},
		{"取消无请求时间", func(r *RunRecord) { r.Status = StatusCancelled; r.CancelRequestedAt = time.Time{} }},
		{"cancelling 无请求时间", func(r *RunRecord) { r.Status = StatusCancelling; r.CancelRequestedAt = time.Time{} }},
	}
	for _, f := range storeFactories() {
		f := f
		for _, c := range cases {
			c := c
			t.Run(f.name+"/"+c.name, func(t *testing.T) {
				stubClock(t, testBase(), time.Hour)
				store := f.make(t)

				bad := recordIn("shape", StatusQueued, testBase())
				c.mutate(&bad)
				if _, err := store.Create(context.Background(), bad); !errors.Is(err, ErrInvalidRecord) {
					t.Fatalf("Create %s 应 ErrInvalidRecord, got %v", c.name, err)
				}

				created := mustCreate(t, store, recordIn("shape", StatusRunning, testBase()))
				if _, err := cas(t, store, "shape", created.Version, c.mutate); !errors.Is(err, ErrInvalidRecord) {
					t.Fatalf("CAS %s 应 ErrInvalidRecord, got %v", c.name, err)
				}
			})
		}
	}
}

// TestRunCreateContent covers fields beyond identity and status, including terminal records.
func TestRunCreateContent(t *testing.T) {
	for _, f := range storeFactories() {
		t.Run(f.name, func(t *testing.T) {
			for _, status := range []Status{StatusQueued, StatusSucceeded} {
				t.Run(string(status), func(t *testing.T) {
					store := f.make(t)
					original := recordIn("content", status, testBase())
					saved := mustCreate(t, store, original)
					mutations := map[string]func(*RunRecord){
						"worker":       func(r *RunRecord) { r.WorkerID = "other" },
						"epoch":        func(r *RunRecord) { r.WorkerEpoch = 2 },
						"cancel":       func(r *RunRecord) { r.CancelRequestedAt = testBase() },
						"heartbeat":    func(r *RunRecord) { r.HeartbeatAt = testBase() },
						"lease":        func(r *RunRecord) { r.WorkerLeaseUntil = testBase() },
						"checkpoint":   func(r *RunRecord) { r.LatestCheckpointRef = &checkpoint.Ref{ThreadID: "other", CheckpointID: "cp"} },
						"revision":     func(r *RunRecord) { r.CheckpointRevision++ },
						"completion":   func(r *RunRecord) { r.CompletionRef = &checkpoint.Ref{ThreadID: r.ThreadID, CheckpointID: "done"} },
						"result":       func(r *RunRecord) { r.Result = settledResult("different") },
						"error":        func(r *RunRecord) { r.Error = &RunError{Code: ErrCodeDispatchUnknown} },
						"record error": func(r *RunRecord) { r.RecordError = &RunError{Code: "record_failed"} },
						"finish":       func(r *RunRecord) { r.FinishReason = FinishReasonFailed },
						"created":      func(r *RunRecord) { r.CreatedAt = saved.CreatedAt.Add(time.Hour) },
					}
					for name, mutate := range mutations {
						t.Run(name, func(t *testing.T) {
							next := saved
							mutate(&next)
							if _, err := store.Create(context.Background(), next); !errors.Is(err, ErrConflict) {
								t.Fatalf("different content: got %v, want ErrConflict", err)
							}
							if got := getRecord(t, store, saved.RunID); got.Version != saved.Version {
								t.Fatal("conflict changed version")
							}
						})
					}
					replay := original
					replay.Version = 999
					replay.UpdatedAt = testBase().Add(time.Hour)
					if _, err := store.Create(context.Background(), replay); err != nil {
						t.Fatalf("original replay: %v", err)
					}
				})
			}
		})
	}
}

func TestRunOwnerlessMutations(t *testing.T) {
	for _, f := range storeFactories() {
		t.Run(f.name, func(t *testing.T) {
			store := f.make(t)
			initial := recordIn("fenced", StatusRunning, testBase())
			initial.WorkerID, initial.WorkerEpoch = "worker", 1
			saved := mustCreate(t, store, initial)
			mutations := map[string]func(*RunRecord){
				"heartbeat":  func(r *RunRecord) { r.HeartbeatAt = testBase() },
				"lease":      func(r *RunRecord) { r.WorkerLeaseUntil = testBase() },
				"result":     func(r *RunRecord) { r.Result = settledResult("unauthorized") },
				"checkpoint": func(r *RunRecord) { r.CheckpointRevision++ },
				"succeeded":  func(r *RunRecord) { r.Status = StatusSucceeded; r.Result = settledResult("done") },
				"failed":     func(r *RunRecord) { r.Status = StatusFailed; r.Error = &RunError{Code: ErrCodeExecutionFailed} },
				"paused":     func(r *RunRecord) { r.Status = StatusPaused },
				"cancel with payload": func(r *RunRecord) {
					r.Status = StatusCancelling
					r.CancelRequestedAt = testBase()
					r.Result = settledResult("injected")
				},
			}
			for name, mutate := range mutations {
				t.Run(name, func(t *testing.T) {
					store := f.make(t)
					saved := mustCreate(t, store, initial)
					next := saved
					next.WorkerID, next.WorkerEpoch = "", 0
					mutate(&next)
					if _, err := store.CompareAndSwap(context.Background(), next.RunID, saved.Version, next); !errors.Is(err, ErrOwnershipLost) {
						t.Fatalf("ownerless mutation: got %v, want ErrOwnershipLost", err)
					}
					if got := getRecord(t, store, saved.RunID); !reflect.DeepEqual(got, saved) {
						t.Fatal("rejected write changed record")
					}
				})
			}
			if got := getRecord(t, store, saved.RunID); got.Version != saved.Version {
				t.Fatal("rejected writes changed record")
			}
			cancel := saved
			cancel.WorkerID, cancel.WorkerEpoch = "", 0
			cancel.Status, cancel.CancelRequestedAt = StatusCancelling, testBase()
			requested, err := store.CompareAndSwap(context.Background(), saved.RunID, saved.Version, cancel)
			if err != nil {
				t.Fatalf("coordinator cancellation: %v", err)
			}
			if requested.WorkerID != saved.WorkerID || requested.WorkerEpoch != saved.WorkerEpoch {
				t.Fatal("cancellation changed ownership")
			}
			retry := requested
			retry.WorkerID, retry.WorkerEpoch = "", 0
			requested, err = store.CompareAndSwap(context.Background(), retry.RunID, retry.Version, retry)
			if err != nil {
				t.Fatalf("repeat cancellation: %v", err)
			}
			requested.HeartbeatAt = testBase().Add(time.Minute)
			beat, err := store.CompareAndSwap(context.Background(), requested.RunID, requested.Version, requested)
			if err != nil {
				t.Fatalf("owner heartbeat: %v", err)
			}
			beat.Status, beat.FinishReason, beat.Result = StatusSucceeded, FinishReasonCompleted, settledResult("owner result")
			done, err := store.CompareAndSwap(context.Background(), beat.RunID, beat.Version, beat)
			if err != nil {
				t.Fatalf("owner completion after cancellation: %v", err)
			}
			if !done.CancelRequestedAt.Equal(cancel.CancelRequestedAt) {
				t.Fatal("completion erased cancellation")
			}
		})
	}
}
