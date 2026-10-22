// Command input-phases 演示四个输入投递阶段：在工具批次执行期间用 SteerWith
// 追加四条消息，before_model、after_model、after_tools 进入同一步的模型上下文，
// next_turn 等到这一回合结束后的新回合才投递。默认没有开启抢占，Preempt 请求
// 在入队前就被拒为 ErrPreemptionDisabled。
//
// 全程 mock 模型，用通道协调工具执行，不使用 time.Sleep，无网络：
//
//	go run ./examples/input-phases
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

var phases = []core.InputPhase{
	core.InputBeforeModel,
	core.InputAfterModel,
	core.InputAfterTools,
	core.InputNextTurn,
}

type ticketRecord struct {
	phase  core.InputPhase
	ticket *agent.SteerTicket
}

func main() {
	ctx := context.Background()
	toolStarted := make(chan struct{})
	releaseTool := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseTool) })

	lookup := tool.New("lookup", "查询一行数据",
		func(_ *tool.Context, _ struct{}) (string, error) {
			startOnce.Do(func() { close(toolStarted) })
			<-releaseTool
			return "row-1", nil
		})
	model := mock.New("phases", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); !ok {
			return toolBatch()
		}
		if hasText(req, "p:"+string(core.InputNextTurn)) {
			return mock.Text("第二个回合的回答")
		}
		return mock.Text("第一个回合的回答")
	})
	a, err := agent.New(agent.WithModel(model), agent.WithTools(lookup))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("四个阶段：before_model、after_model、after_tools、next_turn")
	fmt.Println("四条消息都在工具批次执行期间追加。")
	run := a.Stream(ctx, "查一行数据")

	type injection struct {
		tickets []ticketRecord
		preempt error
	}
	injected := make(chan injection, 1)
	go func() {
		<-toolStarted
		var out injection
		for _, p := range phases {
			tk, err := run.SteerWith(core.UserText("p:"+string(p)), agent.SteerOptions{Phase: p})
			if err != nil {
				log.Fatalf("SteerWith(%s): %v", p, err)
			}
			out.tickets = append(out.tickets, ticketRecord{phase: p, ticket: tk})
		}
		// 默认没有 WithInputPreemption，抢占请求在入队前被拒。
		_, out.preempt = run.SteerWith(core.UserText("抢占"),
			agent.SteerOptions{Phase: core.InputBeforeModel, Preempt: true})
		releaseOnce.Do(func() { close(releaseTool) })
		injected <- out
	}()

	var (
		delivered    []core.InputDelivered
		startedSteps []int
		doneSteps    []int
	)
	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.InputDelivered:
			delivered = append(delivered, e)
		case core.TurnStarted:
			startedSteps = append(startedSteps, e.Step)
		case core.TurnDone:
			doneSteps = append(doneSteps, e.Step)
		}
	}
	result, err := run.Wait()
	if err != nil {
		log.Fatalf("运行失败：%v", err)
	}
	got := <-injected
	if !errors.Is(got.preempt, agent.ErrPreemptionDisabled) {
		log.Fatalf("Preempt 请求 = %v，want ErrPreemptionDisabled", got.preempt)
	}
	fmt.Printf("默认未开启抢占：Preempt 请求被拒为 %v\n", got.preempt)

	for _, d := range delivered {
		fmt.Printf("投递：source=%s phase=%s step=%d count=%d\n", d.Source, d.Phase, d.Step, d.Count)
	}
	wantStep := map[core.InputPhase]int{
		core.InputBeforeModel: 1,
		core.InputAfterModel:  1,
		core.InputAfterTools:  1,
		core.InputNextTurn:    2,
	}
	if len(delivered) != len(phases) {
		log.Fatalf("投递事件 = %d 条，want %d", len(delivered), len(phases))
	}
	for _, d := range delivered {
		if d.Step != wantStep[d.Phase] || d.Count != 1 {
			log.Fatalf("投递 %s step=%d count=%d，want step=%d count=1",
				d.Phase, d.Step, d.Count, wantStep[d.Phase])
		}
	}
	for _, rec := range got.tickets {
		outcome := rec.ticket.Outcome()
		if !outcome.Delivered || outcome.Step != wantStep[rec.phase] {
			log.Fatalf("票据 %s = %+v，want step=%d 已投递",
				rec.phase, outcome, wantStep[rec.phase])
		}
		fmt.Printf("票据：phase=%s state=%s delivered=%v step=%d\n",
			rec.phase, rec.ticket.State(), outcome.Delivered, outcome.Step)
	}

	fmt.Println("TurnStarted 步骤：", startedSteps)
	fmt.Println("TurnDone 步骤：", doneSteps)
	fmt.Println("最终回答：", result.Message.Text())
	if result.Message.Text() != "第二个回合的回答" {
		log.Fatalf("最终回答 = %q，want 第二个回合的回答", result.Message.Text())
	}
}

func toolBatch() *llm.Response {
	return &llm.Response{
		Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
			core.ToolCall{ID: "c1", Name: "lookup", Args: []byte(`{}`)},
		}},
		StopReason: llm.StopToolUse,
	}
}

func hasText(req *llm.Request, want string) bool {
	for _, m := range req.Messages {
		if m.Text() == want {
			return true
		}
	}
	return false
}
