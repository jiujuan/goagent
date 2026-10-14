// Command managed-run 演示受管任务（ADR-0034 D4、D5）：提交先落任务记录再投递；worker 领取后
// 用派发到的身份执行；提交方"断线"之后，任何只拿着 RunID 的新客户端都能接着跟——读状态、等待
// 终态、请求取消、显式修复。示例默认单进程离线跑完这几个角色（mock 模型 + 文件存储 + 进程内队列），
// 并演示投递结果分不出来时记录保持 queued + dispatch_unknown。
//
//	go run ./examples/managed-run                       // 离线全流程
//	go run ./examples/managed-run -dir ./tmp-managed    // 保留目录，便于换个进程再连
//	go run ./examples/managed-run -role status -dir ./tmp-managed -run run-demo-1  // 重启的客户端
//
// 真正跨进程的 worker 需要可持久化的投递通道：加 -redis redis://host:6379/0 后，终端 A 跑
// -role worker，终端 B 跑 -role submit，两边共享同一个 -dir。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/queue"
	"github.com/jiujuan/goagent/runstore"
	"github.com/jiujuan/goagent/tool"
)

// 两个 agent key：一个直接答，一个先调一个会停住的工具，用来演示跨客户端取消。
const (
	quickKey = "report-writer"
	slowKey  = "report-holder"
)

func main() {
	role := flag.String("role", "all", "角色：all|submit|worker|status|cancel")
	dir := flag.String("dir", "", "共享目录（任务记录 + 检查点）；留空则用临时目录并在结束时删除")
	runID := flag.String("run", "", "要跟随的 RunID（status/cancel 角色需要）")
	redisURL := flag.String("redis", "", "可选 Redis 地址；给了才跨进程投递，留空用进程内队列")
	workerID := flag.String("worker", "worker-1", "本进程在任务记录上留下的 worker 名字")
	flag.Parse()

	target := *dir
	if target == "" {
		temp, err := os.MkdirTemp("", "goagent-managed-run-*")
		if err != nil {
			log.Fatal(err)
		}
		target = temp
		defer os.RemoveAll(temp)
		fmt.Printf("📁 共享目录: %s（临时，结束时删除）\n", target)
	} else {
		fmt.Printf("📁 共享目录: %s（保留）\n", target)
	}

	ctx := context.Background()
	switch *role {
	case "all":
		runAll(ctx, target)
	case "submit":
		submit(ctx, target, *redisURL, *runID)
	case "worker":
		worker(ctx, target, *redisURL, *workerID)
	case "status":
		statusRole(ctx, target, needRun(*runID))
	case "cancel":
		cancelRole(ctx, target, needRun(*runID))
	default:
		log.Fatalf("unknown role %q", *role)
	}
}

func needRun(value string) string {
	if value == "" {
		log.Fatal("这个角色需要 -run <RunID>")
	}
	return value
}

// runAll 把提交、worker、查询、取消、修复串成一个可离线跑完的故事。每个"已经落盘"的判断都由
// 新开的存储句柄读回，而不是复用写入时那个句柄里的内存状态。
func runAll(ctx context.Context, dir string) {
	q := queue.NewMemQueue(8)
	store := openRunStore(dir)

	sub, err := queue.EnqueueManagedAgent(ctx, q, store, queue.ManagedRunRequest{
		AgentKey: quickKey,
		RunID:    "run-demo-1", ThreadID: "thread-demo", TurnID: "turn-demo-1", JobID: "job-demo-1",
		Input: "生成本季度摘要",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n📤 提交 %s：记录先落盘，作业随后投递\n", sub.RunID)
	printRecord("提交后", mustGetRun(ctx, store, sub.RunID))

	// worker 角色只认 Type/Payload：模型、工具、检查点存储都由 AgentResolver 按 AgentKey 重建。
	handler, err := queue.ManagedRunHandler(store, "worker-1", resolveAgent(dir),
		queue.WithCancelPollInterval(10*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	pool := queue.NewPool(q, 2).WithRegistry(queue.Registry{queue.ManagedRunJobType: handler})
	go pool.Run(ctx)

	client := newClient(ctx, dir)
	settled := waitForRun(ctx, client, sub.RunID)
	fmt.Printf("\n✅ 只凭 RunID 等待：worker 执行完并发布终态后返回\n")
	printRecord("结束后", settled)

	// 重启的客户端：新句柄、新 RunClient，内存里什么都没带。
	restarted := newClient(ctx, dir)
	again, err := restarted.GetRun(ctx, sub.RunID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n♻️ 换个客户端读到同一条记录：v%d %s，结果=%q\n",
		again.Version, again.Status, again.Result.Message.Text())

	// 第二次运行让 worker 停在工具里；取消由客户端写进记录，worker 自己轮询到并收尾。
	sub2, err := queue.EnqueueManagedAgent(ctx, q, store, queue.ManagedRunRequest{
		AgentKey: slowKey,
		RunID:    "run-demo-2", ThreadID: "thread-demo", TurnID: "turn-demo-2", JobID: "job-demo-2",
		Input: "会停下来的那一次",
	})
	if err != nil {
		log.Fatal(err)
	}
	running, err := waitStatus(ctx, store, sub2.RunID, runstore.StatusRunning)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n🙋 记录此刻属于 %q（epoch %d），另一个客户端请求取消\n", running.WorkerID, running.WorkerEpoch)
	requested, err := client.CancelRun(ctx, sub2.RunID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   请求写入后 status=%s，时间 %s\n", requested.Status, requested.CancelRequestedAt.Format("15:04:05.000"))
	printRecord("取消收尾后", waitForRun(ctx, client, sub2.RunID))

	// 投递结果分不出来：错误不证明队列拒收，记录就保持 queued，只挂一个可重试的未知错误。
	unknownSub, dispatchErr := queue.EnqueueManagedAgent(ctx, opaqueFailureQueue{}, store, queue.ManagedRunRequest{
		AgentKey: quickKey,
		RunID:    "run-demo-3", ThreadID: "thread-demo", TurnID: "turn-demo-3", JobID: "job-demo-3",
		Input: "也许没送出去的那一次",
	})
	fmt.Printf("\n❓ 投递调用报错（%v）\n", dispatchErr)
	printRecord("未知投递后", mustGetRun(ctx, store, unknownSub.RunID))

	// 修复只按磁盘上的证据走：这条运行从没执行过，没有终态快照，所以不许编一个结局。
	reconciler, err := queue.NewRunReconciler(openRunStore(dir), openSnapshotStore(dir))
	if err != nil {
		log.Fatal(err)
	}
	report, err := reconciler.ReconcileRun(ctx, unknownSub.RunID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("🧭 显式修复这条：repaired=%v reason=%s\n", report.Repaired, report.Reason)

	// 同一份作业再投一次：这次交给真队列，worker 领取执行；重复投递由领取的那次 CAS 挡住。
	if _, err := queue.RetryDispatch(ctx, q, store, unknownSub); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("🔁 RetryDispatch 之后：\n")
	printRecord("重投结束", waitForRun(ctx, client, unknownSub.RunID))

	// 列表只给状态投影，不带结果正文。
	page, err := openRunFile(dir).ListRuns(ctx, runstore.RunListOptions{ThreadID: "thread-demo", Limit: 10})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n📇 ListRuns：线程 %q 有 %d 条状态投影\n", "thread-demo", len(page.Items))
	for _, item := range page.Items {
		fmt.Printf("   %s %-10s v%d worker=%q 有结果=%v 运行错误=%s 记账错误=%s\n",
			item.RunID, item.Status, item.Version, item.WorkerID, item.HasResult,
			orDash(item.ErrorCode), orDash(item.RecordErrorCode))
	}
	fmt.Printf("   摘要里没有 Result 正文，也没有错误详情文本\n")
}

// --- 分角色入口 ---------------------------------------------------------------

func submit(ctx context.Context, dir, redisURL, runID string) {
	q, _, err := newQueue(redisURL)
	if err != nil {
		log.Fatal(err)
	}
	if redisURL == "" {
		fmt.Println("⚠️ 没给 -redis：进程内队列只在本进程有效，别的进程取不到这份作业")
	}
	if runID == "" {
		runID = fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	sub, err := queue.EnqueueManagedAgent(ctx, q, openRunStore(dir), queue.ManagedRunRequest{
		AgentKey: quickKey, RunID: runID, ThreadID: "thread-cli", Input: "跨进程提交",
	})
	if err != nil {
		log.Printf("投递这一步报错（RunID 仍然可用，记录保持 queued）: %v", err)
	}
	fmt.Printf("📤 已提交 RunID=%s JobID=%s；换进程用 -role status -dir %s -run %s 就能接上\n",
		sub.RunID, sub.JobID, dir, sub.RunID)
}

func worker(ctx context.Context, dir, redisURL, workerID string) {
	_, consumer, err := newQueue(redisURL)
	if err != nil {
		log.Fatal(err)
	}
	handler, err := queue.ManagedRunHandler(openRunStore(dir), workerID, resolveAgent(dir),
		queue.WithCancelPollInterval(50*time.Millisecond), queue.WithHeartbeatInterval(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	pool := queue.NewPool(consumer, 2).WithRegistry(queue.Registry{queue.ManagedRunJobType: handler})
	fmt.Printf("👷 worker %s 开始取作业（Ctrl-C 结束）\n", workerID)
	pool.Run(ctx)
}

func statusRole(ctx context.Context, dir, runID string) {
	record, err := newClient(ctx, dir).GetRun(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("🔎 只凭 RunID 读到：\n")
	printRecord(runID, record)
}

func cancelRole(ctx context.Context, dir, runID string) {
	record, err := newClient(ctx, dir).CancelRun(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("🙋 取消请求写入后：\n")
	printRecord(runID, record)
}

// --- 存储与 agent 装配 --------------------------------------------------------

func openRunStore(dir string) runstore.Store {
	store, err := runstore.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}
	return store
}

func openRunFile(dir string) *runstore.File {
	store, err := runstore.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}
	return store
}

func openSnapshotStore(dir string) checkpoint.Checkpointer {
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}
	return store
}

func newQueue(redisURL string) (queue.Queue, queue.Consumer, error) {
	if redisURL == "" {
		q := queue.NewMemQueue(16)
		return q, q, nil
	}
	return queue.New(queue.WithRedis(redisURL))
}

// resolveAgent 是应用侧的 AgentResolver：作业里只有 AgentKey，模型、工具、检查点存储都在这里
// 重建。作业载荷不带闭包、选项或存储句柄，所以带外选择（模型凭证、外部文件后端、审批中间件）
// 属于这里返回的 agent，而不是提交请求：外部托管的文件后端要每个进程自己重新绑定
// （vfs.NewDirStore 配 agent.WithRunFiles，作为运行级选项传给 StreamManaged/Send），它既不随
// 作业搬运，也不随快照回滚。
func resolveAgent(dir string) queue.AgentResolver {
	return func(_ context.Context, key string) (*agent.Agent, error) {
		switch key {
		case quickKey, slowKey:
		default:
			return nil, fmt.Errorf("examples/managed-run: 不认识 agent key %q", key)
		}
		store, err := checkpoint.NewFile(dir)
		if err != nil {
			return nil, err
		}
		model := quickModel()
		if key == slowKey {
			model = holdFirstModel()
		}
		a, err := agent.New(
			agent.WithModel(model),
			agent.WithCheckpointer(store),
			agent.WithTools(holdTool()),
			agent.WithToolTimeout(60*time.Second),
		)
		if err != nil {
			return nil, err
		}
		return a, nil
	}
}

func quickModel() llm.Model {
	return mock.New("quick", func(*llm.Request) *llm.Response {
		return mock.Text("本季度摘要已生成。")
	})
}

// holdFirstModel 第一次要工具，工具返回后收尾：配合下面那个不停到 context 取消的工具，
// 这次运行会停在"工具还在跑"的那一刻。
func holdFirstModel() llm.Model {
	return mock.New("slow", func(req *llm.Request) *llm.Response {
		if _, called := mock.LastToolResult(req); !called {
			return mock.CallTool("c1", "hold", `{}`)
		}
		return mock.Text("工具之后的收尾。")
	})
}

func holdTool() tool.Tool {
	return tool.New("hold", "停到执行 context 取消为止", func(tctx *tool.Context, _ struct{}) (string, error) {
		<-tctx.Done()
		return "", tctx.Err()
	})
}

// opaqueFailureQueue 演示"投递结果未知"：错误不包装 queue.ErrDispatchRefused，所以提交方
// 不能断定作业没进去，记录必须保持 queued 而不是被写成失败。
type opaqueFailureQueue struct{}

func (opaqueFailureQueue) Enqueue(context.Context, queue.Job) error {
	return errors.New("examples/managed-run: 投递调用超时，队列是否收到未知")
}

// --- 小工具 -------------------------------------------------------------------

func newClient(ctx context.Context, dir string) *queue.RunClient {
	client, err := queue.NewRunClient(openRunStore(dir),
		queue.WithRunPollInterval(5*time.Millisecond), queue.WithRunPollMax(50*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	return client
}

// waitForRun 等到这条运行进入终态。30 秒只是"示例不该无限挂住"的上限，判断依据始终是记录本身。
func waitForRun(ctx context.Context, client *queue.RunClient, runID string) runstore.RunRecord {
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	record, err := client.WaitRun(waitCtx, runID)
	if err != nil {
		log.Fatalf("等 %s 到终态: %v", runID, err)
	}
	return record
}

func waitStatus(ctx context.Context, store runstore.Store, runID string, want runstore.Status) (runstore.RunRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		record, err := store.Get(ctx, runID)
		if err != nil {
			return record, err
		}
		if record.Status == want {
			return record, nil
		}
		select {
		case <-ctx.Done():
			return record, fmt.Errorf("examples/managed-run: 等 %s 变成 %s 超时: %w", runID, want, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func mustGetRun(ctx context.Context, store runstore.Store, runID string) runstore.RunRecord {
	record, err := store.Get(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}
	return record
}

func printRecord(label string, record runstore.RunRecord) {
	fmt.Printf("   %s: v%d status=%s finish=%s worker=%q epoch=%d\n",
		label, record.Version, record.Status, record.FinishReason, record.WorkerID, record.WorkerEpoch)
	fmt.Printf("        心跳=%s 取消请求=%s 结果=%s 运行错误=%s 记账错误=%s\n",
		stampOrDash(record.HeartbeatAt), stampOrDash(record.CancelRequestedAt),
		textOf(record.Result), codeOf(record.Error), codeOf(record.RecordError))
}

func textOf(result *core.Result) string {
	if result == nil {
		return "—"
	}
	return fmt.Sprintf("%q", result.Message.Text())
}

func codeOf(runErr *runstore.RunError) string {
	if runErr == nil {
		return "—"
	}
	return runErr.Code
}

func orDash(value string) string {
	if value == "" {
		return "—"
	}
	return value
}

func stampOrDash(value time.Time) string {
	if value.IsZero() {
		return "—"
	}
	return value.Format("15:04:05")
}
