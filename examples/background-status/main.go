// Command background-status 演示后台任务的两个不同终点：Handle.Wait 说"运行结束了"，
// Handle.WaitRecorded 说"这次运行的终态已经写进存储"。示例配置 JSONL 文件存储、
// middleware.RunBudget 与非零 agent.WithToolTimeout，先起 worker 再入队，随后取消提交方
// 的 context，用 WaitRecorded 等到落盘，再用 Agent.RunStatus 从磁盘读回状态与文件。
// 用 mock provider，全程离线。
//
//	go run ./examples/background-status
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/queue"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/vfs"
)

const threadID = "quarterly-report"

func main() {
	dir, err := os.MkdirTemp("", "goagent-background-status-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fmt.Printf("📁 检查点目录: %s（示例结束时删除）\n\n", dir)

	store, err := checkpoint.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}

	a, err := agent.New(
		agent.WithModel(reportModel()),
		agent.WithCheckpointer(store),
		agent.WithTools(noteTool(), peekTool()),
		// 整次运行的挂钟上限。它到点后让运行"正常收尾"（撤下工具、要一句最终回答），
		// 不是在途调用的硬期限：一个不看 context 的工具，这一轮还是得等它。
		agent.WithMiddleware(middleware.RunBudget(middleware.RunBudgetOptions{
			MaxDuration: 500 * time.Millisecond,
			MaxTurns:    8,
			OnExceed: func(r middleware.BudgetReport) {
				fmt.Printf("  [budget] 资源 %v 越过上限，改为正常收尾\n", r.Exceeded)
			},
		})),
		// 单个工具调用的上限（每次调用，不是一批，也不是一整轮）。真正能打断
		// "不看 context 的工具"的是它，不是上面的 MaxDuration。
		agent.WithToolTimeout(3*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	// 队列 + worker：WaitRecorded 等的是 worker 的收尾，所以必须先有 worker 在跑。
	q := queue.NewMemQueue(8)
	pool := queue.NewPool(q, 1)
	ctx := context.Background()
	go pool.Run(ctx)

	submitCtx, cancelSubmit := context.WithCancel(ctx)
	h, err := queue.EnqueueAgent(submitCtx, q, a, "生成本季度报告",
		agent.OnThread(threadID), agent.WithRunFiles(vfs.NewInState()))
	if err != nil {
		cancelSubmit()
		log.Fatal(err)
	}
	// 提交方在这里"断线"：入队成功后运行归队列所有，取消只结束提交这一侧。
	// 要中止执行得显式 h.Cancel()——Handle 没有别的隐式中止路径。
	cancelSubmit()
	fmt.Printf("📤 已入队 %s，提交方 context 立刻取消（运行不受影响）\n", h.ID)

	res, err := h.WaitRecorded()
	if err != nil {
		reportFailure(err)
		q.Close()
		os.Exit(1)
	}
	q.Close()
	fmt.Printf("✅ 运行结束且终态记录已确认: %s\n\n", res.Message.Text())

	// 查询只是读存储里的事实，不写、不取 claim、不续租。
	status, err := a.RunStatus(ctx, threadID)
	if err != nil {
		log.Fatal(err)
	}
	if status.Completion == nil {
		// 暂停的运行没有终态记录；WaitRecorded 对它返回 nil 错误。
		log.Fatalf("head %s 上没有终态记录（paused=%v）", status.Latest.ID, status.Paused)
	}
	fmt.Printf("🔎 RunStatus: exists=%v paused=%v revision=%d step=%d kind=%s run=%s\n",
		status.Exists, status.Paused, status.Revision, status.Step,
		status.Completion.Kind, status.Completion.RunID)
	fmt.Printf("   记录的结果: %s\n", status.Completion.Result.Message.Text())
	fmt.Printf("   快照里的文件: report.txt = %q\n\n", string(status.Latest.FileSnapshot["report.txt"]))

	// 换一个 store 实例、换一个 Agent：终态与文件在磁盘上，不依赖写入它的那个进程。
	reopened, err := checkpoint.NewFile(dir)
	if err != nil {
		log.Fatal(err)
	}
	reader, err := agent.New(
		agent.WithModel(peekModel()),
		agent.WithCheckpointer(reopened),
		agent.WithTools(peekTool()),
	)
	if err != nil {
		log.Fatal(err)
	}
	answer, err := reader.Run(ctx, "报告文件里写了什么？", agent.OnThread(threadID))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("♻️ 同线程新运行从终态快照恢复文件: %s\n\n", answer)

	after, err := reader.RunStatus(ctx, threadID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("📌 新运行把 head 从 revision %d 推到 %d，此刻 head 上还带着终态记录吗: %v\n"+
		"   RunStatus 读到的只是查询那一刻的存储事实，回答不了\"现在有没有运行在跑\"。\n",
		status.Revision, after.Revision, after.Completion != nil)
}

// reportModel 先写一份文件（于是有一个中间提交边界），再给最终回答。
// 预算越过后工具被撤下，同一个分支自然走文本收尾。
func reportModel() llm.Model {
	return mock.New("m", func(req *llm.Request) *llm.Response {
		last, hasLast := mock.LastToolResult(req)
		wrote := hasLast && last.Name == "note"
		if len(req.Tools) > 0 && !wrote {
			return mock.CallTool("c1", "note", `{"text":"队列不再随提交方取消。"}`)
		}
		return mock.Text("本季度报告已生成并归档。")
	})
}

// noteTool 睡 800ms，让 RunBudget 的 500ms 上限一定会越过；它在 3 秒的工具上限之内
// 返回，所以这一轮工具上限没有触发——那条线管的是不会自己返回的工具。
func noteTool() tool.Tool {
	return tool.New("note", "写报告文件", func(tctx *tool.Context, a struct {
		Text string `json:"text"`
	}) (string, error) {
		time.Sleep(800 * time.Millisecond)
		if err := tctx.State.Files.Write("report.txt", []byte(a.Text)); err != nil {
			return "", err
		}
		return "已写入 report.txt", nil
	})
}

func peekTool() tool.Tool {
	return tool.New("peek", "读报告文件", func(tctx *tool.Context, a struct {
		Path string `json:"path"`
	}) (string, error) {
		b, err := tctx.State.Files.Read(a.Path)
		if err != nil {
			return "文件不在快照里", nil
		}
		return string(b), nil
	})
}

func peekModel() llm.Model {
	return mock.New("peek", func(req *llm.Request) *llm.Response {
		if last, ok := mock.LastToolResult(req); ok && last.Name == "peek" && len(last.Content) > 0 {
			text, _ := last.Content[0].(core.Text)
			return mock.Text("读回: " + text.Text)
		}
		return mock.CallTool("p1", "peek", `{"path":"report.txt"}`)
	})
}

// reportFailure 把 WaitRecorded 的合并错误拆开说：运行错误与记录错误各是什么。
func reportFailure(err error) {
	fmt.Printf("❌ 收尾返回错误: %v\n", err)
	switch {
	case errors.Is(err, checkpoint.ErrRevisionConflict):
		fmt.Println("   记录被 revision 校验拒绝：head 已被别的运行推进。终态绑定的是\n" +
			"   本次运行的提交边界，不能读新 head 再改写旧终态——先确认这次运行的结果对不对，\n" +
			"   再决定要不要由那一轮自己记录。")
	case errors.Is(err, checkpoint.ErrClaimConflict):
		fmt.Println("   线程正被别的持有者占用（租约未过期），本次记录没有落盘。")
	case errors.Is(err, checkpoint.ErrStaleFencingToken):
		fmt.Println("   本次运行的 writer 租约已过期，存储拒绝了旧 fence 的写入。")
	case errors.Is(err, checkpoint.ErrDurableUnavailable):
		fmt.Println("   当前 store 没实现 checkpoint.Durable，框架无法原子追加终态；\n" +
			"   换用 Memory/File 或自己实现 Durable，不要退回普通 Save。")
	case errors.Is(err, agent.ErrNoCommittedBoundary):
		fmt.Println("   这次运行没有自己的成功提交边界（准备阶段就失败，或自定义 Runnable\n" +
			"   没有落盘），所以没有终态可记。运行结果本身仍然可用。")
	default:
		fmt.Println("   运行错误与记录错误都在上面；Wait 已经能给出的运行结果不受影响。")
	}
}
