// Command session 演示会话门面（ADR-0034 D3、D7、D8）：OpenSession 绑定一个线程，Send 在这
// 个线程上连续跑，GetHead 取当前事实，ForkFrom 从某条已提交快照拉出子分支，子分支自己继续跑，
// 最后用 History 读"不带正文的摘要分页"。示例用 mock 模型与文件检查点存储，全程离线。
//
//	go run ./examples/session            // 临时目录，结束时删除
//	go run ./examples/session -dir ./tmp-session   // 保留目录，可反复跑同一个线程
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

func main() {
	dir := flag.String("dir", "", "检查点目录；留空则用临时目录并在结束时删除")
	flag.Parse()

	target, cleanup := *dir, func() {}
	if target == "" {
		temp, err := os.MkdirTemp("", "goagent-session-*")
		if err != nil {
			log.Fatal(err)
		}
		target, cleanup = temp, func() { os.RemoveAll(temp) }
	}
	defer cleanup()
	fmt.Printf("📁 检查点目录: %s\n\n", target)

	store, err := checkpoint.NewFile(target)
	if err != nil {
		log.Fatal(err)
	}
	a, err := agent.New(agent.WithModel(answerModel()), agent.WithCheckpointer(store))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	main_, err := a.OpenSession(ctx, "roadshow")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("🔌 OpenSession(%q)：不写任何东西，门面不等于「这个线程存在过」\n", main_.ThreadID())

	for _, input := range []string{"第一句", "第二句"} {
		res, err := main_.Send(ctx, input)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("✅ Send(%q) → run=%s turn=%s revision=%d 边界=%s 回答=%q\n",
			input, res.RunID, res.TurnID, res.Revision, res.CheckpointRef.CheckpointID, res.Result.Message.Text())
	}

	head, err := main_.GetHead(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("🔎 GetHead: %s（每次真读存储，门面不缓存 head）\n\n", head.ID)

	// 从中间那条快照拉分支，而不是从 head：谱系边落在子线程第一条记录上。
	branch, err := main_.ForkFrom(ctx, head.ID, "roadshow-branch")
	if err != nil {
		log.Fatal(err)
	}
	res, err := branch.Send(ctx, "分支上的第一句")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("🌱 ForkFrom(%s) → %q，子分支这次运行 revision=%d，谱系父指向源线程\n", head.ID, branch.ThreadID(), res.Revision)

	sourceHistory, err := main_.History(ctx, checkpoint.ListOptions{Limit: checkpoint.MaxQueryLimit})
	if err != nil {
		log.Fatal(err)
	}
	branchHistory, err := branch.History(ctx, checkpoint.ListOptions{Limit: checkpoint.MaxQueryLimit})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   源线程 %d 条，子线程 %d 条：两条时间线互不改写\n\n",
		len(sourceHistory.Items), len(branchHistory.Items))

	walkProjection(ctx, main_)
	showSummariesHaveNoPayload(sourceHistory)
	showCursorMisuse(ctx, main_)

	_, err = main_.Send(ctx, "想串到别的线程", agent.OnThread("another-thread"))
	after, err2 := main_.History(ctx, checkpoint.ListOptions{Limit: checkpoint.MaxQueryLimit})
	if err2 != nil {
		log.Fatal(err2)
	}
	fmt.Printf("\n🚫 Send 里带 OnThread(\"another-thread\") 被拒：%v\n", err != nil)
	fmt.Printf("   且什么都没跑：绑定线程还是 %d 条记录（拒绝发生在执行之前）\n", len(after.Items))
	fmt.Printf("   绑定线程赢：门面的 fork、读 head、恢复都必须落在同一个线程上\n")
}

// walkProjection 展示分页的核心承诺：第一次查询把投影冻住，此后别的写入进不了这次遍历，
// 换一个新查询才看得见。
func walkProjection(ctx context.Context, session *agent.Session) {
	fmt.Printf("📄 History 分页：先取第一页\n")
	page, err := session.History(ctx, checkpoint.ListOptions{Limit: 1})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   第 1 页 %d 条，cursor=%s…\n", len(page.Items), shorten(page.NextCursor))

	// 冻结之后继续写：这次遍历看不见，新查询才看得见。
	if _, err := session.Send(ctx, "分页之后又跑了一句"); err != nil {
		log.Fatal(err)
	}
	var walked []string
	for page.NextCursor != "" {
		for _, item := range page.Items {
			walked = append(walked, item.Ref.CheckpointID)
		}
		next, err := session.History(ctx, checkpoint.ListOptions{Limit: 1, Cursor: page.NextCursor})
		if err != nil {
			log.Fatal(err)
		}
		page = next
	}
	for _, item := range page.Items {
		walked = append(walked, item.Ref.CheckpointID)
	}
	fresh, err := session.History(ctx, checkpoint.ListOptions{Limit: checkpoint.MaxQueryLimit})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   冻结遍历拿到 %d 条（不重不漏），新查询拿到 %d 条：新写入只对新查询可见\n\n",
		len(walked), len(fresh.Items))
}

// showSummariesHaveNoPayload 用编码后的字节证明摘要页里没有会话正文。
func showSummariesHaveNoPayload(page checkpoint.MetadataPage) {
	encoded, err := json.Marshal(page.Items)
	if err != nil {
		log.Fatal(err)
	}
	leaked := strings.Contains(string(encoded), "第一句")
	fmt.Printf("🧾 摘要页字段：revision/step/身份/时间/终态种类；编码里出现正文吗：%v\n"+
		"   一条摘要：revision=%d step=%d kind=%q created=%s\n\n",
		leaked, page.Items[0].Revision, page.Items[0].Step,
		page.Items[0].CompletionKind, page.Items[0].CreatedAt.Format("15:04:05"))
}

// showCursorMisuse 演示两个可判定的 cursor 错误：换了过滤条件复用同一个 token，以及把别人的
// token 当成翻页依据。过期（ErrCursorExpired）要等投影缓存的 15 分钟或缓存被淘汰，示例不假装
// 能当场触发，只说明处置方式。
func showCursorMisuse(ctx context.Context, session *agent.Session) {
	first, err := session.History(ctx, checkpoint.ListOptions{Limit: 1})
	if err != nil {
		log.Fatal(err)
	}
	_, err = session.History(ctx, checkpoint.ListOptions{Limit: 1, CompletionKind: "completed", Cursor: first.NextCursor})
	fmt.Printf("🚧 换过滤条件复用同一个 cursor：%v\n", errors.Is(err, checkpoint.ErrInvalidCursor))

	_, err = session.History(ctx, checkpoint.ListOptions{Limit: checkpoint.MaxQueryLimit + 1})
	fmt.Printf("   页大小超过上限是查询本身不可用（ErrInvalidQuery）：%v\n", errors.Is(err, checkpoint.ErrInvalidQuery))

	fmt.Printf("   格式没问题但投影已经没了会返回 ErrCursorExpired：处置方式是重新起一次查询，不是重试旧 token\n")
}

func shorten(cursor string) string {
	if len(cursor) <= 12 {
		return cursor
	}
	return cursor[:12]
}

// answerModel 每次直接给一句文本，不叫工具：示例关心的是门面与分页，不是模型行为。
func answerModel() llm.Model {
	return mock.New("m", func(*llm.Request) *llm.Response {
		return mock.Text("已记下这一句。")
	})
}
