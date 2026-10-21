// Command task-loop 演示一次运行里的两个回合：模型第一次回答后，主协程用
// Run.Steer 追加一条消息，终稿判断把它带进下一步的模型上下文，票据结算为
// Delivered，运行最终以第二个终稿结束。运行结束后再追加会得到 ErrRunClosed。
//
// 全程 mock 模型，通道同步，不使用 time.Sleep，无网络、无随机输出：
//
//	go run ./examples/task-loop
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

const (
	initialAsk = "写一句产品介绍。"
	followUp   = "补充：面向离线环境。"
)

func main() {
	ctx := context.Background()
	firstCallStarted := make(chan struct{})
	releaseFirstCall := make(chan struct{})
	var firstCallOnce sync.Once

	model := mock.New("demo", func(req *llm.Request) *llm.Response {
		if hasUserText(req.Messages, followUp) {
			return mock.Text("修订稿：面向离线环境的产品介绍。")
		}
		firstCallOnce.Do(func() { close(firstCallStarted) })
		<-releaseFirstCall
		return mock.Text("初稿")
	})

	a, err := agent.New(agent.WithModel(model))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("初始提问：", initialAsk)
	fmt.Println("追加消息：", followUp)

	run := a.Stream(ctx, initialAsk)
	type steerResult struct {
		ticket *agent.SteerTicket
		err    error
	}
	steered := make(chan steerResult, 1)
	go func() {
		<-firstCallStarted
		ticket, err := run.Steer(core.UserText(followUp))
		steered <- steerResult{ticket: ticket, err: err}
		close(releaseFirstCall)
	}()

	var (
		startedSteps []int
		doneSteps    []int
		runDoneCount int
		runDoneText  string
	)
	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.TurnStarted:
			startedSteps = append(startedSteps, e.Step)
		case core.TurnDone:
			doneSteps = append(doneSteps, e.Step)
		case core.MessageDone:
			if text := e.Message.Text(); text != "" {
				fmt.Println("模型输出：", text)
			}
		case core.RunDone:
			runDoneCount++
			runDoneText = e.Result.Message.Text()
		}
	}

	got := <-steered
	if got.err != nil {
		log.Fatalf("Steer: %v", got.err)
	}
	outcome := got.ticket.Outcome()
	fmt.Printf("票据：Delivered=%v Step=%d Err=%v\n", outcome.Delivered, outcome.Step, outcome.Err)
	if !outcome.Delivered || outcome.Step != 1 {
		log.Fatalf("票据没有在第二步投递：%+v", outcome)
	}

	fmt.Println("TurnStarted 步骤：", startedSteps)
	fmt.Println("TurnDone 步骤：", doneSteps)
	fmt.Printf("RunDone 次数：%d，最终结果：%s\n", runDoneCount, runDoneText)
	if len(startedSteps) != 2 || len(doneSteps) != 2 || runDoneCount != 1 {
		log.Fatalf("事件数量不符：TurnStarted=%v TurnDone=%v RunDone=%d",
			startedSteps, doneSteps, runDoneCount)
	}
	if !strings.Contains(runDoneText, "修订稿") {
		log.Fatalf("第二次模型调用没有看到追加消息，最终结果：%q", runDoneText)
	}

	late, err := run.Steer(core.UserText("运行结束后再追加"))
	if late == nil && errors.Is(err, agent.ErrRunClosed) {
		fmt.Println("运行结束后追加：ErrRunClosed")
		return
	}
	log.Fatalf("运行结束后 Steer = (%v, %v), want (nil, ErrRunClosed)", late, err)
}

func hasUserText(messages []core.Message, want string) bool {
	for _, m := range messages {
		if m.Role == core.RoleUser && m.Text() == want {
			return true
		}
	}
	return false
}
