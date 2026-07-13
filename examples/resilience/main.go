// Command resilience demonstrates the P1 resilience decorators: provider
// fallback and the circuit breaker, composed with retry. It uses offline mock
// models (no API key) so you can watch a sick primary trip the breaker and the
// FallbackModel route around it.
//
//	go run ./examples/resilience
//
// In production, swap the mock models for real providers, e.g.
//
//	primary := middleware.CircuitBreaker(
//	    middleware.RetryModel(anthropic.New(...), middleware.RetryOptions{MaxAttempts: 2}),
//	    middleware.CircuitOptions{FailureThreshold: 5, OpenTimeout: 30 * time.Second})
//	model := middleware.FallbackModel(middleware.FallbackOptions{}, primary, openaicompat.New(...))
package main

import (
	"context"
	"fmt"
	"iter"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/middleware"
)

// flakyModel is a stand-in for a sick provider: every call fails pre-stream
// with a 503. It counts how many times it is actually contacted, so we can see
// the breaker spare it once open.
type flakyModel struct {
	name  string
	calls int
}

func (m *flakyModel) Name() string { return m.name }
func (m *flakyModel) Generate(context.Context, *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		m.calls++
		yield(nil, &llm.StatusError{Provider: m.name, Code: 503, Body: "service unavailable"})
	}
}

// healthyModel always answers.
type healthyModel struct{ name string }

func (m healthyModel) Name() string { return m.name }
func (m healthyModel) Generate(_ context.Context, _ *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		yield(&llm.Response{Message: core.AssistantText("backup 已接管，一切正常。"), StopReason: llm.StopEnd}, nil)
	}
}

func main() {
	primaryRaw := &flakyModel{name: "primary"}
	backup := healthyModel{name: "backup"}

	// Breaker trips after 3 consecutive failures; logs every state change.
	primary := middleware.CircuitBreaker(primaryRaw, middleware.CircuitOptions{
		FailureThreshold: 3,
		OpenTimeout:      60, // 任意：本 demo 不推进时钟，保持 Open
		OnStateChange: func(from, to middleware.CircuitState) {
			fmt.Printf("    ⚡ 熔断状态：%s → %s\n", from, to)
		},
	})

	model := middleware.FallbackModel(middleware.FallbackOptions{
		OnFallback: func(from, to string, err error) {
			fmt.Printf("    ↪ 故障转移：%s 失败(%v)，切到 %s\n", from, err, to)
		},
	}, primary, backup)

	fmt.Println("=== 直接调用模型 6 次：观察熔断打开后主模型不再被触达 ===")
	for i := 1; i <= 6; i++ {
		fmt.Printf("第 %d 次调用：\n", i)
		ans, err := generateOnce(model)
		if err != nil {
			fmt.Printf("    结果：错误 %v\n", err)
		} else {
			fmt.Printf("    结果：%s\n", ans)
		}
	}
	fmt.Printf("\n主模型实际被触达 %d 次（熔断打开后应停止增长，调用直接 fail-fast 转备用）\n\n", primaryRaw.calls)

	fmt.Println("=== 作为 agent 的模型使用 ===")
	a, err := agent.New(agent.WithModel(model), agent.WithInstruction("你是助手。"))
	if err != nil {
		panic(err)
	}
	out, err := a.Run(context.Background(), "在吗？")
	if err != nil {
		panic(err)
	}
	fmt.Printf("🤖 %s\n", out)
}

// generateOnce drains one Generate call to its final message.
func generateOnce(m llm.Model) (string, error) {
	var last core.Message
	for resp, err := range m.Generate(context.Background(), &llm.Request{}) {
		if err != nil {
			return "", err
		}
		last = resp.Message
	}
	return last.Text(), nil
}
