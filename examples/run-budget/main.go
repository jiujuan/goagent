// Command run-budget is a runnable offline tour of middleware.RunBudget: the
// per-run cost/time cap that ends a budget-exhausted run with a real final
// answer instead of an error. A mock model spends tokens on tool calls until
// the budget trips; RunBudget then warns it, clears the tool table, and the
// run settles on the model's wrap-up reply.
//
//	go run ./examples/run-budget
//
// 无 API key 依赖：全程使用手工构造的 mock 响应（含假 Usage）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/tool"
)

// spender bills 100 in / 50 out tokens per call and keeps calling tools while
// they are offered; with no tools left it must answer.
type spender struct{ calls int }

func (m *spender) Name() string { return "spender" }

func (m *spender) Generate(_ context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		if len(req.Tools) > 0 {
			m.calls++
			yield(&llm.Response{
				Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
					core.ToolCall{ID: fmt.Sprintf("c%d", m.calls), Name: "fetch", Args: json.RawMessage(`{}`)},
				}},
				Usage: &core.Usage{InputTokens: 100, OutputTokens: 50},
			}, nil)
			return
		}
		yield(&llm.Response{
			Message: core.AssistantText("Wrapped up: fetched 2 of the planned 5 documents; the remaining 3 were left for a follow-up run."),
			Usage:   &core.Usage{InputTokens: 10, OutputTokens: 20},
		}, nil)
	}
}

func main() {
	fetch := tool.New("fetch", "fetch a document", func(_ *tool.Context, _ struct{}) (string, error) {
		return "document contents", nil
	})

	guard := middleware.RunBudget(middleware.RunBudgetOptions{
		MaxTotalTokens: 400, // 150 tokens/call → warn at 80% then cap after 3 calls
		Price:          middleware.Price{InputPerMTok: 1, OutputPerMTok: 2},
		OnExceed: func(r middleware.BudgetReport) {
			fmt.Printf("  [budget] exceeded %v: in=%d out=%d turns=%d cost=$%.6f\n",
				r.Exceeded, r.InputTokens, r.OutputTokens, r.Turns, r.CostUSD)
		},
	})

	a, err := agent.New(
		agent.WithModel(&spender{}),
		agent.WithTools(fetch),
		agent.WithMiddleware(guard),
	)
	if err != nil {
		log.Fatal(err)
	}

	for ev := range collect(a.Stream(context.Background(), "read me five documents")) {
		switch e := ev.(type) {
		case core.ToolDone:
			fmt.Printf("  [tool ] %s ran\n", e.Result.Name)
		case core.BudgetExceeded:
			fmt.Printf("  [event] budget exceeded: resource=%s step=%d\n", e.Resource, e.Step)
		case core.MessageDone:
			if len(e.Message.ToolCalls()) == 0 && e.Message.Text() != "" {
				fmt.Printf("  [final] %s\n", e.Message.Text())
			}
		case core.RunFailed:
			fmt.Printf("  [bad  ] run failed: %v\n", e.Err)
		}
	}
}

func collect(run *agent.Run) <-chan core.Event {
	ch := make(chan core.Event)
	go func() {
		defer close(ch)
		for ev, err := range run.Iter() {
			if err != nil {
				return
			}
			ch <- ev
		}
	}()
	return ch
}
