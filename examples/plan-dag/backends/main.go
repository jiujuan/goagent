// Command plan-dag-backends is a focused tutorial on the execution plan's
// pluggable concurrency backend. The same DAG is run twice — once on each
// backend — to show they are behaviorally identical (same step order, same
// parallelism, same final result), differing only in the engine that bounds
// concurrency.
//
//	并发底座两选一（plan.Config.Backend）:
//	  • BackendGoroutines（默认）—— 每个就绪步骤一个 goroutine，用信号量限并发。
//	    手写协调（errgroup 式），零额外设施，适合绝大多数场景。
//	  • BackendQueue —— 把步骤执行投递到 queue.Worker 有界池。当你想让计划步骤
//	    和其它后台任务共享同一套 worker 基础设施，或基于 queue 的 Queue/Bus 扩展时选它。
//
// 两者的 OnError / Retry / Timeout 语义完全一致；步骤结果不经（有损的）Bus 传回，
// 调度器直接收集，所以持久化与恢复都不受后端选择影响。
//
// 计划形状（菱形 DAG，三个抓取步并行，再汇总）:
//
//	┌────────────┐
//	│  fetch_a   │──┐
//	└────────────┘  │
//	┌────────────┐  │   三步无依赖 → 并行
//	│  fetch_b   │──┤
//	└────────────┘  │
//	┌────────────┐  │
//	│  fetch_c   │──┘
//	     │  │  │
//	     ▼  ▼  ▼
//	┌────────────────┐
//	│     merge      │   依赖三者 → 等全部完成
//	└────────────────┘
//
// 每个抓取步故意 sleep 80ms：串行需 ~240ms，并行只需 ~80ms。示例打印各后端的
// 墙钟耗时，直观证明两种底座都真正并行。
//
//	go run ./examples/plan-dag/backends
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/plan"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
)

// buildPlan returns a fresh diamond DAG. Each step is a FuncExecutor that reads
// its upstreams' outputs from session state and sleeps to simulate I/O, so the
// scheduler's parallelism is visible in wall-clock time.
func buildPlan() *plan.Plan {
	fetch := func(id, payload string) *plan.Step {
		return &plan.Step{
			ID: id, Name: id,
			Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
				time.Sleep(80 * time.Millisecond) // 模拟一次外部抓取
				return &plan.StepResult{StepID: id, Output: payload}, nil
			}),
		}
	}
	return &plan.Plan{
		ID:   "fanin",
		Goal: "并行抓取三路数据后汇总",
		Steps: []*plan.Step{
			fetch("fetch_a", "天气=晴"),
			fetch("fetch_b", "人数=10"),
			fetch("fetch_c", "预算=8000"),
			{
				ID: "merge", Name: "merge", DependsOn: []string{"fetch_a", "fetch_b", "fetch_c"},
				Exec: plan.FuncExecutor(func(sc *plan.StepContext) (*plan.StepResult, error) {
					a, _ := sc.State.Get(plan.StepResultKey("fetch_a"))
					b, _ := sc.State.Get(plan.StepResultKey("fetch_b"))
					c, _ := sc.State.Get(plan.StepResultKey("fetch_c"))
					return &plan.StepResult{
						StepID: "merge",
						Output: fmt.Sprintf("汇总【%v | %v | %v】", a, b, c),
					}, nil
				}),
			},
		},
	}
}

// runOnBackend executes the plan once on the given backend and returns the
// wall-clock elapsed time. Step transitions and the final summary are printed.
func runOnBackend(name string, backend plan.Backend) time.Duration {
	planAgent := plan.New(plan.Config{
		Name:    "fanin-planner",
		Plan:    buildPlan(),
		MaxConc: 4, // 足够让三个抓取步同时跑
		Backend: backend,
	})
	// 每个后端用独立 session，互不干扰。
	r := runner.New(runner.Config{AppName: "plan-dag-backends", Root: planAgent, Store: session.InMemory()})

	fmt.Printf("\n──── 后端: %s ────\n", name)
	start := time.Now()
	for ev, err := range r.Run(context.Background(), "user", "s-"+name, core.UserText("go")) {
		if err != nil {
			log.Fatal(err)
		}
		if ev == nil || ev.Partial {
			continue
		}
		switch {
		case ev.Progress != nil && ev.Progress.Kind == "plan_step":
			fmt.Printf("  · 步骤 %-10s → %s\n", ev.Progress.JobID, ev.Progress.Status)
		case ev.Message != nil && ev.Author == planAgent.Name():
			fmt.Println("  " + ev.Message.Text())
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("  ⏱ 墙钟耗时: %v\n", elapsed.Round(time.Millisecond))
	return elapsed
}

func main() {
	fmt.Println("=== 同一个 DAG，两种并发底座，行为一致 ===")

	g := runOnBackend("BackendGoroutines (默认/errgroup 式)", plan.BackendGoroutines)
	q := runOnBackend("BackendQueue (queue.Worker 池)", plan.BackendQueue)

	fmt.Printf("\n结论：两种底座产出相同，且均真正并行（≈80ms ≪ 串行 240ms）。\n")
	fmt.Printf("       goroutines=%v  queue=%v\n", g.Round(time.Millisecond), q.Round(time.Millisecond))
	fmt.Println("       选择建议：默认用 BackendGoroutines；需共享 worker 池/基于 queue 扩展时用 BackendQueue。")
}
