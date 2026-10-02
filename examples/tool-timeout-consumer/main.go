// Command tool-timeout-consumer 从"读事件流的人"这个视角演示怎么把工具超时讲清楚。
//
// 上限到点之后，循环做两件事：取消工具收到的 context，并且不再等这条调用——它替工具
// 写一句话，然后继续往下跑。于是事件流上会出现两种只在设了上限时才见到的情况：一句
// 并不出自工具的文案，以及 ToolDone 之后还在来的 ToolUpdate。这个示例分三个场景演示：
// 工具自报中断、循环代答超时（同批次另一个调用还在跑，所以迟到上报照样收得到）、
// 运行本身被取消。
//
//	go run ./examples/tool-timeout-consumer
//
// 无 API key 依赖：全程使用 llm/mock。
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

const (
	bound  = 400 * time.Millisecond // A、B 场景的单次调用上限
	step   = 150 * time.Millisecond // 工具每步的模拟耗时
	quiet  = 1 * time.Second        // 每个场景结束后再等一会，让被放弃的调用跑完
	cancel = 400 * time.Millisecond // C 场景：run 在何时被取消
)

func main() {
	fmt.Printf("单次调用上限：%v，工具每步 %v\n\n", bound, step)

	demo{
		title: "== A. 工具会看 context：它自己报告做到哪一步 ==",
		model: echoModel("fetch_rows"),
		tools: []tool.Tool{fetchRows()},
		bound: bound,
	}.run()

	demo{
		title: "== B. 工具不看 context：循环替它回答，同批次另一个调用还在跑 ==",
		model: twoCallModel(),
		tools: []tool.Tool{legacyBatch(), jobWatcher()},
		mwSetup: &middleware.ToolTimeoutOptions{
			Default: bound,
			PerTool: map[string]time.Duration{"job_watcher": 1500 * time.Millisecond},
		},
	}.run()

	demo{
		title:       "== C. 运行被取消：措辞与超时不同 ==",
		model:       echoModel("sleep_only"),
		tools:       []tool.Tool{sleepOnly()},
		bound:       3 * time.Second,
		cancelAfter: cancel,
	}.run()

	fmt.Println(`
读超时事件的五条约定：
  1. 按 CallID 记账，一条调用只在 ToolDone 结算一次。之后还在来的 ToolUpdate 是被放弃
     的那个工具还在上报，只能丢弃——历史里已经没有这条调用的位置了。
  2. IsError 为真就不要显示成完成态。无论是工具自己说的还是循环代答，这条调用都没有
     可用结果。
  3. 超时和"已取消""已回滚"是两件事：循环只是不再等它，副作用可能已经发生，也可能还
     在继续。界面写"结果未知"，不要写"已停止"。
  4. 想区分"工具自报"和"循环代答"，目前只能读文案（见 classify，全示例只这一处）。把它
     集中在一个函数里，将来循环带上机器可读的分类字段时只改这一处。
  5. 上限要小于用户愿意等转圈的时间，又要大于这类工具正常完成的时间。前者归界面，后者
     归工具；两个数颠倒时，超时只是配置造成的噪声。`)
}

// --- 三个场景的配置与运行 ---------------------------------------------------

// demo 是一个场景：一套模型桩、一批工具、上限设在哪、以及 run 何时被取消。
type demo struct {
	title       string
	model       llm.Model
	tools       []tool.Tool
	bound       time.Duration                  // agent.WithToolTimeout，0 表示不设
	mwSetup     *middleware.ToolTimeoutOptions // 非 nil 则加 middleware.ToolTimeout
	cancelAfter time.Duration                  // >0 则在这个时间点取消整个 run
}

func (d demo) run() {
	fmt.Println(d.title)

	opts := []agent.Option{
		agent.WithModel(d.model),
		agent.WithTools(d.tools...),
		agent.WithMaxTurns(3),
	}
	if d.bound > 0 {
		opts = append(opts, agent.WithToolTimeout(d.bound))
	}
	if d.mwSetup != nil {
		opts = append(opts, agent.WithMiddleware(middleware.ToolTimeout(*d.mwSetup)))
	}

	a, err := agent.New(opts...)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	if d.cancelAfter > 0 {
		go func() {
			time.Sleep(d.cancelAfter)
			cancelFn()
		}()
	}

	tr := newTracker()
	run := a.Stream(ctx, "do it")
	for ev, err := range run.Iter() {
		if err != nil {
			// C 场景里 run 被取消，这里拿到 error 是预期内的：已经发生的事件足够说明问题。
			break
		}
		tr.handle(ev)
	}
	_, _ = run.Wait()

	tr.report()
	time.Sleep(quiet) // 让被放弃的那条调用彻底跑完，再进入下一个场景
}

// --- 三个工具：各自代表一种"被上限打断时"的样子 -----------------------------

// fetchRows 每写一行之前先看一次 context，被中断时报告自己写到哪。
func fetchRows() tool.Tool {
	var rows atomic.Int32
	return tool.New("fetch_rows", "逐行写入，随时可被中断",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			for i := 1; i <= 10; i++ {
				select {
				case <-time.After(step):
					rows.Add(1)
					tctx.Update(core.Text{Text: fmt.Sprintf("已写入 %d 行", rows.Load())})
				case <-tctx.Done():
					return "", fmt.Errorf("写完 %d 行后中断（第 %d 行未落盘）：%w",
						rows.Load(), i, tctx.Err())
				}
			}
			return "10 行全部写入", nil
		})
}

// legacyBatch 不看 context，也不管有没有人在听：循环不再等它之后，它还在写、还在上报。
func legacyBatch() tool.Tool {
	return tool.New("legacy_batch", "一批写 6 行，中途不看 context",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			for i := 1; i <= 6; i++ {
				time.Sleep(step)
				tctx.Update(core.Text{Text: fmt.Sprintf("已写入第 %d 行", i)})
			}
			return "6 行写完", nil
		})
}

// jobWatcher 是同一批次里的另一个调用：它给的时间更宽，所以能正常跑完。它的存在让本
// 场景的 run 持续到 1.2s 之后，也就让 legacyBatch 的迟到上报真的能被事件流收到。
func jobWatcher() tool.Tool {
	return tool.New("job_watcher", "每步上报进度，1.2s 完成",
		func(tctx *tool.Context, _ struct{}) (string, error) {
			for i := 1; i <= 8; i++ {
				select {
				case <-time.After(step):
					tctx.Update(core.Text{Text: fmt.Sprintf("作业第 %d 步完成", i)})
				case <-tctx.Done():
					return "", fmt.Errorf("作业第 %d 步被打断：%w", i, tctx.Err())
				}
			}
			return "作业完成，8 步全部执行", nil
		})
}

// sleepOnly 只做一次长阻塞：既不取消也不上报，用于演示运行取消时的措辞。
func sleepOnly() tool.Tool {
	return tool.New("sleep_only", "阻塞 1.5s，不看 context 也不上报",
		func(_ *tool.Context, _ struct{}) (string, error) {
			time.Sleep(1500 * time.Millisecond)
			return "睡满 1.5s", nil
		})
}

// --- 模型桩 -----------------------------------------------------------------

// echoModel 第一轮回一个工具调用，第二轮把工具结果回述一遍。
func echoModel(toolName string) *mock.Model {
	return mock.New("echo", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("模型收到: " + firstLine(resultText(tr)))
		}
		return mock.CallTool("c1", toolName, `{}`)
	})
}

// twoCallModel 第一轮同时请求两个工具（c1 是被上限打断的那个，c2 是宽限的那个），
// 第二轮回述最后一个结果。
func twoCallModel() *mock.Model {
	return mock.New("two", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("模型收到: " + firstLine(resultText(tr)))
		}
		return &llm.Response{
			Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
				core.ToolCall{ID: "c1", Name: "legacy_batch", Args: []byte(`{}`)},
				core.ToolCall{ID: "c2", Name: "job_watcher", Args: []byte(`{}`)},
			}},
			StopReason: llm.StopToolUse,
		}
	})
}

// --- 事件记账 ---------------------------------------------------------------

type verdict string

const (
	verdictDone       verdict = "完成"
	verdictSelfReport verdict = "工具自报中断"
	verdictTimeout    verdict = "超时，结果未知"
	verdictCancelled  verdict = "运行已取消"
	verdictUnsettled  verdict = "未结算"
)

// callRecord 是一条调用的记账。closed 之后的进度算迟到：只计数，不上屏。
type callRecord struct {
	name    string
	start   time.Time
	elapsed time.Duration
	text    string
	verdict verdict
	updates int
	late    int
	closed  bool
}

// tracker 按 CallID 记账。真实界面里这就是一个 map 加一次重绘。
type tracker struct {
	calls map[string]*callRecord
	order []string
}

func newTracker() *tracker {
	return &tracker{calls: map[string]*callRecord{}}
}

func (t *tracker) handle(ev core.Event) {
	switch e := ev.(type) {
	case core.ToolStarted:
		t.calls[e.Call.ID] = &callRecord{name: e.Call.Name, start: time.Now()}
		t.order = append(t.order, e.Call.ID)

	case core.ToolUpdate:
		rec, ok := t.calls[e.CallID]
		if !ok {
			return
		}
		if rec.closed {
			rec.late++ // 被放弃的工具还在上报，这条调用已经结算过了
			return
		}
		rec.updates++

	case core.ToolDone:
		rec, ok := t.calls[e.Result.CallID]
		if !ok {
			rec = &callRecord{name: e.Result.Name, start: time.Now()}
			t.calls[e.Result.CallID] = rec
			t.order = append(t.order, e.Result.CallID)
		}
		rec.elapsed = time.Since(rec.start)
		rec.text = resultText(e.Result)
		rec.verdict = classify(e.Result)
		rec.closed = true // 这条调用到此结算，之后的进度只能丢弃
	}
}

// classify 决定一条已结算的调用在界面上算什么。
//
// ToolDone 能给到的只有 IsError 和一段文本，"这句话出自谁"没有单独的字段，所以循环
// 代答的那两句措辞只能靠文本识别。这是本示例唯一读文案的地方，集中在这一处；识别不
// 出的错误一律当作工具自己的报告，并且不去猜它做到了哪一步。
func classify(tr core.ToolResult) verdict {
	text := resultText(tr)
	switch {
	case !tr.IsError:
		return verdictDone
	case strings.Contains(text, "timed out after"):
		return verdictTimeout
	case strings.Contains(text, "did not finish"):
		return verdictCancelled
	default:
		return verdictSelfReport
	}
}

func (t *tracker) report() {
	for _, id := range t.order {
		rec := t.calls[id]
		if !rec.closed {
			rec.verdict = verdictUnsettled
		}
		fmt.Printf("  %-13s -> %s | 用时 %s | 进度 %d 条，ToolDone 之后丢弃 %d 条\n",
			rec.name, rec.verdict, rec.elapsed.Round(time.Millisecond), rec.updates, rec.late)
		fmt.Printf("     界面文案 -> %s\n\n", clip(firstLine(rec.text), 76))
	}
}

// --- 小工具 -----------------------------------------------------------------

func resultText(tr core.ToolResult) string {
	var b strings.Builder
	for _, p := range tr.Content {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// clip 只为了排版：演示输出对齐用，截掉的只是显示。
func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
