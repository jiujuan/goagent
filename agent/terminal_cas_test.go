package agent_test

// BC-03（ADR-0033 D4，V5）：终态记录的 CAS 语义——旧运行不得污染新 head，
// 活跃 claim 必须拦下绕过它的写入，重复调用只确认同一条记录。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func recordAgent(t *testing.T, store checkpoint.Checkpointer, reply string) *agent.Agent {
	t.Helper()
	return terminalAgent(t, store, mock.New("m", func(*llm.Request) *llm.Response { return mock.Text(reply) }))
}

// TestRecordCompletionStaleRunRejected：A 结束、B 运行完成之后，A 才记录终态时
// 必须因 revision 冲突被拒，B 的 head 与历史不受影响。
func TestRecordCompletionStaleRunRejected(t *testing.T) {
	ctx := context.Background()
	for name, newBackends := range newTerminalBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackends()
			a := recordAgent(t, b.store, "第一次")
			runA := a.Stream(ctx, "go", agent.OnThread("stale-"+name))
			if _, err := runA.Wait(); err != nil {
				t.Fatal(err)
			}

			second := recordAgent(t, b.reopen(), "第二次")
			runB := second.Stream(ctx, "go", agent.OnThread("stale-"+name))
			if _, err := runB.Wait(); err != nil {
				t.Fatal(err)
			}
			if err := second.RecordCompletion(ctx, runB); err != nil {
				t.Fatalf("记录 B 的终态: %v", err)
			}
			before := historyCount(t, b.reopen(), "stale-"+name)
			headBefore := headCheckpoint(t, b.reopen(), "stale-"+name)

			if err := a.RecordCompletion(ctx, runA); !errors.Is(err, checkpoint.ErrRevisionConflict) {
				t.Fatalf("A 的旧结果应被 CAS 拒绝，实际: %v", err)
			}

			after := headCheckpoint(t, b.reopen(), "stale-"+name)
			if after.ID != headBefore.ID || after.Revision != headBefore.Revision {
				t.Fatalf("被拒的记录改动了 head: %s/%d -> %s/%d", headBefore.ID, headBefore.Revision, after.ID, after.Revision)
			}
			if after.Completion == nil || after.Completion.RunID != runB.ID {
				t.Fatalf("B 的终态被污染: %+v", after.Completion)
			}
			if got := historyCount(t, b.reopen(), "stale-"+name); got != before {
				t.Fatalf("被拒的记录追加了历史: %d -> %d", before, got)
			}
		})
	}
}

// TestRecordCompletionCASAfterLoad：闸门卡在 A 的提交上（此时它已经读过边界），
// 放行之前让 B 提交；A 恢复后仍被拒绝——说明校验发生在提交时，不只是读取时。
func TestRecordCompletionCASAfterLoad(t *testing.T) {
	ctx := context.Background()
	for name, newBackends := range newTerminalBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackends()
			base, ok := b.store.(checkpoint.ThreadCoordinator)
			if !ok {
				t.Fatalf("后端 %s 不支持协调", name)
			}
			store := &terminalStore{
				ThreadCoordinator: base,
				gate: func(req checkpoint.CommitRequest) bool {
					return req.Checkpoint != nil && req.Checkpoint.Completion != nil
				},
				entered: make(chan struct{}),
				release: make(chan struct{}),
			}
			a := recordAgent(t, store, "A 的结果")
			runA := a.Stream(ctx, "go", agent.OnThread("cas-"+name))
			if _, err := runA.Wait(); err != nil {
				t.Fatal(err)
			}

			recorded := make(chan error, 1)
			go func() { recorded <- a.RecordCompletion(ctx, runA) }()

			select {
			case <-store.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("A 的终态提交没有卡在闸门上")
			}

			// A 已经读过边界但提交被卡住；此时让 B 完整跑完并记录终态。
			second := recordAgent(t, b.reopen(), "B 的结果")
			runB := second.Stream(ctx, "go", agent.OnThread("cas-"+name))
			if _, err := runB.Wait(); err != nil {
				t.Fatal(err)
			}

			close(store.release)
			if err := <-recorded; !errors.Is(err, checkpoint.ErrRevisionConflict) {
				t.Fatalf("A 的迟到的记录应被拒绝，实际: %v", err)
			}

			head := headCheckpoint(t, b.reopen(), "cas-"+name)
			if head.Completion != nil && head.Completion.RunID == runA.ID {
				t.Fatal("A 的旧结果写进了 head")
			}
		})
	}
}

// TestRecordCompletionClaimConflict：边界没变但别的 owner 持有活跃 claim 时，
// 记录必须被 ErrClaimRequired 拦下，且请求不携带旧 claim、inbox ack 或工具执行。
func TestRecordCompletionClaimConflict(t *testing.T) {
	ctx := context.Background()
	for name, newBackends := range newTerminalBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackends()
			base, ok := b.store.(checkpoint.ThreadCoordinator)
			if !ok {
				t.Fatalf("后端 %s 不支持协调", name)
			}
			store := &terminalStore{ThreadCoordinator: base}

			a := recordAgent(t, store, "被 claim 拦住的结果")
			runA := a.Stream(ctx, "go", agent.OnThread("claim-"+name))
			if _, err := runA.Wait(); err != nil {
				t.Fatal(err)
			}
			boundaryRevision := headCheckpoint(t, b.reopen(), "claim-"+name).Revision

			begun, err := base.BeginRun(ctx, checkpoint.BeginRunRequest{
				ThreadID: "claim-" + name,
				Owner:    "另一个 worker",
				Lease:    checkpoint.DefaultLease,
			})
			if err != nil {
				t.Fatalf("取得 claim: %v", err)
			}

			if err := a.RecordCompletion(ctx, runA); !errors.Is(err, checkpoint.ErrClaimRequired) {
				t.Fatalf("活跃 claim 应拦住记录，实际: %v", err)
			}
			commits := store.terminalCommits()
			if len(commits) != 1 {
				t.Fatalf("应只有一次终态提交尝试，实际 %d", len(commits))
			}
			req := commits[0]
			if req.Claim != nil {
				t.Fatalf("记录请求携带了 claim: %+v", req.Claim)
			}
			if len(req.AckMessageIDs) != 0 || len(req.ToolExecutions) != 0 {
				t.Fatalf("记录请求夹带了 ack 或工具执行: acks=%v tools=%v", req.AckMessageIDs, req.ToolExecutions)
			}
			if req.ExpectedRevision == nil || *req.ExpectedRevision != boundaryRevision {
				t.Fatalf("记录请求的期望 revision = %v，期望 %d", req.ExpectedRevision, boundaryRevision)
			}
			if head := headCheckpoint(t, b.reopen(), "claim-"+name); head.Completion != nil {
				t.Fatalf("被拒绝的记录写入了终态: %+v", head.Completion)
			}

			// 释放 claim 后同一次记录应当成功。
			if err := base.ReleaseClaim(ctx, begun.Claim); err != nil {
				t.Fatalf("释放 claim: %v", err)
			}
			if err := a.RecordCompletion(ctx, runA); err != nil {
				t.Fatalf("claim 释放后记录失败: %v", err)
			}
			head := headCheckpoint(t, b.reopen(), "claim-"+name)
			if head.Completion == nil || head.Completion.RunID != runA.ID {
				t.Fatalf("终态未按预期写入: %+v", head.Completion)
			}
		})
	}
}

// TestRecordCompletionIdempotent：并发与重复调用只追加一条终态；"已落盘但回执失败"
// 之后重试只确认旧提交；B 前进之后 A 的重试不改变 B 的 head。
func TestRecordCompletionIdempotent(t *testing.T) {
	ctx := context.Background()

	t.Run("并发与重复调用只追加一条", func(t *testing.T) {
		for name, newBackends := range newTerminalBackends(t) {
			t.Run(name, func(t *testing.T) {
				b := newBackends()
				a := recordAgent(t, b.store, "只记录一次")
				run := a.Stream(ctx, "go", agent.OnThread("idem-"+name))
				if _, err := run.Wait(); err != nil {
					t.Fatal(err)
				}
				before := historyCount(t, b.reopen(), "idem-"+name)

				var wg sync.WaitGroup
				errs := make(chan error, 6)
				for i := 0; i < 6; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						errs <- a.RecordCompletion(ctx, run)
					}()
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatalf("并发记录失败: %v", err)
					}
				}
				if err := a.RecordCompletion(ctx, run); err != nil {
					t.Fatalf("重复调用失败: %v", err)
				}

				after := historyCount(t, b.reopen(), "idem-"+name)
				if after != before+1 {
					t.Fatalf("历史应只多一条终态记录: %d -> %d", before, after)
				}
				terminal := 0
				hist, err := b.reopen().History(ctx, "idem-"+name)
				if err != nil {
					t.Fatal(err)
				}
				for _, cp := range hist {
					if cp.Completion != nil {
						terminal++
						if cp.Completion.RunID != run.ID {
							t.Fatalf("终态属于别的运行: %+v", cp.Completion)
						}
					}
				}
				if terminal != 1 {
					t.Fatalf("出现 %d 条终态记录", terminal)
				}
			})
		}
	})

	t.Run("回执丢失后重试", func(t *testing.T) {
		for name, newBackends := range newTerminalBackends(t) {
			t.Run(name, func(t *testing.T) {
				b := newBackends()
				base, ok := b.store.(checkpoint.ThreadCoordinator)
				if !ok {
					t.Fatalf("后端 %s 不支持协调", name)
				}
				var once sync.Once
				store := &terminalStore{
					ThreadCoordinator: base,
					afterCommit: func(req checkpoint.CommitRequest, receipt checkpoint.CommitReceipt, err error) (checkpoint.CommitReceipt, error) {
						if err == nil && req.Checkpoint != nil && req.Checkpoint.Completion != nil {
							// 第一次：数据已落盘，但回执丢失。
							once.Do(func() { err = errors.New("回执丢失") })
						}
						return receipt, err
					},
				}
				a := recordAgent(t, store, "回执丢失的结果")
				run := a.Stream(ctx, "go", agent.OnThread("ack-"+name))
				if _, err := run.Wait(); err != nil {
					t.Fatal(err)
				}
				before := historyCount(t, b.reopen(), "ack-"+name)

				if err := a.RecordCompletion(ctx, run); err == nil {
					t.Fatal("回执丢失应上报错误")
				}
				if err := a.RecordCompletion(ctx, run); err != nil {
					t.Fatalf("重试应确认已落盘的同一记录: %v", err)
				}
				if after := historyCount(t, b.reopen(), "ack-"+name); after != before+1 {
					t.Fatalf("重试追加了重复记录: %d -> %d", before, after)
				}
				head := headCheckpoint(t, b.reopen(), "ack-"+name)
				if head.Completion == nil || head.Completion.RunID != run.ID {
					t.Fatalf("终态不符: %+v", head.Completion)
				}
			})
		}
	})

	t.Run("B 前进后重试 A 不改 head", func(t *testing.T) {
		for name, newBackends := range newTerminalBackends(t) {
			t.Run(name, func(t *testing.T) {
				b := newBackends()
				a := recordAgent(t, b.store, "A 的结果")
				runA := a.Stream(ctx, "go", agent.OnThread("retry-"+name))
				if _, err := runA.Wait(); err != nil {
					t.Fatal(err)
				}
				if err := a.RecordCompletion(ctx, runA); err != nil {
					t.Fatal(err)
				}

				second := recordAgent(t, b.reopen(), "B 的结果")
				runB := second.Stream(ctx, "go", agent.OnThread("retry-"+name))
				if _, err := runB.Wait(); err != nil {
					t.Fatal(err)
				}
				if err := second.RecordCompletion(ctx, runB); err != nil {
					t.Fatal(err)
				}
				before := historyCount(t, b.reopen(), "retry-"+name)
				headBefore := headCheckpoint(t, b.reopen(), "retry-"+name)

				// 同记录重试只确认旧提交：返回成功、head 不动。
				if err := a.RecordCompletion(ctx, runA); err != nil {
					t.Fatalf("A 的重试应确认旧提交，实际: %v", err)
				}
				after := headCheckpoint(t, b.reopen(), "retry-"+name)
				if after.ID != headBefore.ID || after.Revision != headBefore.Revision {
					t.Fatalf("A 的重试移动了 B 的 head: %s/%d -> %s/%d", headBefore.ID, headBefore.Revision, after.ID, after.Revision)
				}
				if got := historyCount(t, b.reopen(), "retry-"+name); got != before {
					t.Fatalf("A 的重试追加了历史: %d -> %d", before, got)
				}
				if after.Completion == nil || after.Completion.RunID != runB.ID {
					t.Fatalf("B 的终态被改写: %+v", after.Completion)
				}
			})
		}
	})
}
