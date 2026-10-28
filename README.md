# goagent

`goagent` 是一个面向 Go 的 Agent 应用框架。它把大模型、工具调用、运行状态、记忆、工作流与运行时治理组合成统一的编程模型，让开发者能够用 Go 构建可组合、可观测、可持久化的 AI Agent 与 Multi-Agent 系统。

它既适合从一个带工具调用的聊天助手开始，也适合逐步演进到包含 RAG、人工审批、并行工作流、后台队列、Redis 持久化和 DAG 图式执行计划的生产型应用。

> 当前模块路径：`github.com/jiujuan/goagent`；要求 Go 1.25 或更高版本。

## 主要用途

- 构建能调用业务 API、数据库、脚本或命令行工具的智能助手。
- 构建客服、数据分析、研发助手等按领域分工的 Multi-Agent 系统。
- 编排确定性的串行、并行、循环流程，或由模型驱动的 ReAct / Plan-and-Execute 流程。
- 为 Agent 增加 RAG、分层记忆、跨进程持久化、上下文压缩、限流、重试和人工审核。
- 通过后台任务队列（内存或 Redis）处理长耗时任务，并通过事件总线流式接收进度。

## 核心特性

| 能力 | 说明 |
| --- | --- |
| 单一执行包 | 无独立 `runtime`/`runner` 包：`agent.New` 构建 Agent，`Run`（阻塞返回答案）与 `Stream`（惰性运行句柄）即完整入口。 |
| 类型安全工具 | 通过泛型 `tool.New` 定义 Go 函数；框架从输入结构体反射生成 JSON Schema，并负责参数校验、调用和结果回填。 |
| 多轮工具循环 | LLM 请求工具后，运行时自动执行工具、把结果写入历史，并继续调用模型直到得到最终答复；用 `WithMaxTurns` 兜底。 |
| 流式事件 | `Stream(...).Iter()` 产出一串类型化的 `core.Event`（`MessageDelta`、`MessageDone`、`ToolDone`、`Interrupted` 等），统一承载文本增量、工具调用与最终结果。 |
| Multi-Agent 委派 | 配置 `WithSubAgents` 后自动获得 `transfer_to_agent` 能力（模型驱动下钻/同级转交）；也可用 `AsTool` 把子 Agent 封装成隔离上下文的工具。 |
| 确定性工作流 | `Sequential`、`Parallel`、`Loop` 和 `Pipeline` 把控制流从模型决策中剥离；并行分支在隔离的 `State` 克隆上运行，结束后按声明顺序确定性地合并 KV（可选冲突策略）。 |
| ReAct 与计划执行 | 普通 Agent 即可完成“思考 → 调用工具 → 观察”的 ReAct 循环；`agent.NewPlan` / `agent.NewLLMPlan` 提供带依赖的执行计划（DAG）与并发调度。 |
| 状态与检查点 | 运行状态为显式的 `core.State`（消息、Todos、KV、文件）；`checkpoint` 提供内存与 JSONL 文件后端，支持按 `ThreadID` 跨进程恢复。 |
| 记忆与 RAG | `memory` 提供向量检索（`NewRetriever`）、可持久化向量库（`File`，支持删除与压缩）与按内容去重的 `Upsert`；`middleware.RAG` 自动把检索结果注入系统提示；`memx` 可组合规则、项目、工作、文本记忆，并在运行结束时自动把对话固化为长期记忆。 |
| 技能系统 | `skills` 基于文件系统的技能包支持按需渐进加载 `SKILL.md`、资源和脚本，可将资源通过 `go:embed` 打进二进制。 |
| 可组合中间件 | 内置重试、限流、上下文压缩、运行中 steering 和 Human-in-the-Loop；中间件以装饰器方式包裹模型。 |
| 安全执行与外部工具 | 提供受策略约束的命令沙箱（`sandbox`）、MCP 客户端（`tool/mcp`）以及网页搜索/抓取工具（`tool/web`）。 |
| 多模型与多模态 | `llm` 抽象与厂商解耦，含 Anthropic、OpenAI 兼容接口、Agnes 适配与 Mock；`llm/agnes` 客户端另提供图像生成与可轮询/恢复的视频任务。 |
| 韧性与可观测性 | `llm` 韧性层支持多供应商回退与熔断；`obs/otel` 接入 OpenTelemetry 追踪；`logger` 基于 zerolog；`config` 基于 viper；`eval` 提供打分/评测。 |
| 异步任务与事件总线 | `queue` 支持内存队列与 Redis Stream，由 Worker 池后台处理；`bus` 只有进程内 pub/sub，跨进程发送进度事件用 `queue` 的 Redis 后端（`queue.NewBus(queue.WithRedis(url))`，实现见 `queue/redis_pubsub.go`，持久化队列见 `queue/redis_stream.go`）。 |

## 架构设计

框架以 `core` 中的消息、事件、状态和指令作为公共语言。`agent` 既是 Agent 的声明（配置）也是可运行句柄——它自身就是运行时，没有独立的 runner/runtime 层：`Run` 立即驱动执行；普通 `Stream` 创建冻结输入的惰性句柄，启动并获得 admission 后才加载状态、驱动模型与工具循环、发布事件并写检查点。

```mermaid
flowchart TB
    U["调用方 / UI / API"] --> A["agent\nNew + Run / Stream / Resume\n（内含运行时循环）"]
    A --> P["prompt\n可组合系统提示"]
    A --> M["llm.Model\nAnthropic / OpenAI 兼容 / Agnes / Mock"]
    A <--> T["tool\n类型化本地函数 / MCP / Web / Exec"]
    A --> MW["middleware\n重试、限流、压缩、Steering、HITL、RAG"]
    A <--> MEM["memory\n向量检索与分层记忆（memx）"]
    A --> WF["工作流 / 计划\nSequential / Parallel / Loop / Pipeline / NewPlan"]
    A --> CP["checkpoint\n内存 / JSONL 文件（按 ThreadID 恢复）"]
    A --> BUS["bus + queue\n事件总线、后台任务、Redis"]
    T --> SB["sandbox\n受限进程执行"]
    A --> E["core.Event\n文本、工具、进度、结果"]
    E --> U
```

### 分层职责

- `core`：跨模块共享的数据模型——`Message`、`Event`（类型化事件）、`State`、工具调用/结果、指令（Directive）。
- `agent`：Agent 的声明与运行时本身；实现 LLM 循环、工作流（Sequential/Parallel/Loop/Pipeline）、DAG 计划、委派与 HITL。
- `llm`：供应商无关的模型接口与韧性层；适配器只需实现统一的请求/响应流（`anthropic`、`openaicompat`、`agnes`、`mock`）。
- `tool`：工具契约与泛型函数工具，另提供 `mcp`、`web`、`exec` 等现成工具。
- `checkpoint`：显式状态快照的持久化——内存与 JSONL 文件后端，支撑跨进程 resume。
- `memory`：向量检索与自动 RAG；`memx` 装配规则/项目/工作/文本多层记忆，并挂载运行结束时的记忆固化。
- `middleware`：以装饰器方式包裹模型，加入重试、限流、压缩、steering、HITL、RAG 等通用策略。
- `prompt`：可组合的系统提示 Section（身份、环境、工具指导、状态）。
- `bus`、`queue`、`sandbox`、`vfs`：事件总线、异步后台执行、受控外部进程、虚拟文件系统后端。
- `config`、`logger`、`eval`、`obs/otel`：配置、日志、评测与可观测性等运行时治理设施。

## 安装

在你的 Go 项目中安装：

```bash
go get github.com/jiujuan/goagent
```

或克隆本仓库后直接运行示例：

```bash
git clone https://github.com/jiujuan/goagent.git
cd goagent
go test ./...
```

离线示例均使用 `llm/mock` 和模拟 Embedding，因此不需要 API Key 或网络模型调用。

## Quick Start

下面是一个最小闭环：模型先请求天气工具，框架执行工具并将结果放回消息历史，模型随后给出最终答复。`Run` 一行拿到答案，`Stream(...).Iter()` 则可观察同一次运行的事件流。

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	// 类型化工具：JSON schema 从参数结构体反射生成。
	weather := tool.New("get_weather", "查询某城市的当前天气",
		func(_ *tool.Context, in struct {
			City string `json:"city" desc:"城市名"`
		}) (string, error) {
			return in.City + "：晴，25°C", nil
		})

	// mock 模型：首轮请求工具，拿到工具结果后给出最终答复。
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("天气结果：" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "get_weather", `{"city":"北京"}`)
	})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("你是一个友好的天气助手。"),
		agent.WithTools(weather),
		agent.WithMaxTurns(8), // 模型<->工具循环的安全上限
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// A) 一行拿到答案。
	answer, err := a.Run(ctx, "北京天气怎么样？")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("answer:", answer)

	// B) 或流式观察同一次运行的事件。
	for ev, err := range a.Stream(ctx, "上海呢？").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			fmt.Printf("🔧 %s -> %s\n", e.Result.Name, e.Result.Content[0].(core.Text).Text)
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("🤖", t)
			}
		}
	}
}
```

直接运行仓库中的完整 Quick Start：

```bash
go run ./examples/quickstart
```

把 mock 模型替换成 `llm/anthropic` 或 `llm/openaicompat` 的模型实现，即可接入真实文本模型；图像和视频场景可使用 `llm/agnes` 客户端。请将密钥放在运行环境中，不要写入源码或提交到版本库。

## 常用开发命令

| 命令 | 用途 |
| --- | --- |
| `go test ./...` | 运行全部测试。 |
| `go build ./...` | 编译全部包与示例。 |
| `go run ./examples/quickstart` | 运行离线最小工具调用闭环（无需密钥）。 |
| `go run ./examples/workflow` | 运行确定性串行/并行/循环工作流（需 Agnes 凭证）。 |
| `go run ./examples/react-agent` | 观察 ReAct 的多步工具调用轨迹。 |

## 示例目录详解

示例均是独立的 `main` 包，优先阅读文件顶部注释，再运行相应命令。使用真实模型的示例需要设置对应环境变量（多为 `AGNES_API_KEY` / `AGNES_MODEL`），未设置时通常只打印用法后退出。

### 入门与核心循环

| 示例 | 运行方式 | 展示内容 |
| --- | --- | --- |
| `quickstart` | `go run ./examples/quickstart` | 离线冒烟测试：mock 模型上的类型化工具调用与流式事件。 |
| `task-loop` | `go run ./examples/task-loop` | 终稿后的追加消息与投递票据：一次运行两个回合，`TurnDone` 可出现多次、`RunDone` 只出现一次。 |
| `input-phases` | `go run ./examples/input-phases` | 四个投递阶段：`before_model`/`after_model`/`after_tools` 进入同一步，`next_turn` 等新回合；抢占默认关闭。 |
| `inflight-recovery` | `go run ./examples/inflight-recovery` | 在途批次的崩溃恢复：`RunStatus.InFlight`、`Resume` 按台账执行 pending 调用且不重复副作用。 |
| `runtime-hardening` | `go run ./examples/runtime-hardening` | 运行时加固：根 Task 与 `AsTool` 子 Task、任务树预算耗尽后的工具错误、`Parallel` 失败后取消兄弟分支、同一个 `Session` 两次 `Send` 的先进先出执行。 |
| `quickstart-chat` | `go run ./examples/quickstart-chat` | 真实模型的最小聊天启动模板。 |
| `quickstart-tool` | `go run ./examples/quickstart-tool` | 真实模型调用一个类型化工具的启动模板。 |
| `quickstart-stream` | `go run ./examples/quickstart-stream` | 流式接收真实模型输出的启动模板；显式持有 Run，说明 break 后的退订与取消。 |
| `lazy-run-lifecycle` | `go run ./examples/lazy-run-lifecycle`（T10 完成后） | 全 mock 离线断言：零调用创建、单次启动、Await 超时隔离、有限尾部、worker 启动权与 Wait/WaitRecorded。 |
| `agent-tutorial` | `go run ./examples/agent-tutorial` | 对公共 API 的引导式完整走读。 |
| `react-agent` | `go run ./examples/react-agent` | 单 Agent 多轮“思考 → 工具 → 观察”的 ReAct 循环。 |

### 多 Agent 与工作流

| 示例 | 运行方式 | 展示内容 |
| --- | --- | --- |
| `multiagent` | `go run ./examples/multiagent` | 多 Agent 委派：`transfer_to_agent` 与 agent-as-tool。 |
| `multi-subagent` | `go run ./examples/multi-subagent` | 三级 Agent 树：顶层协调者委派给两个子专家。 |
| `subagent` | `go run ./examples/subagent` | 通过 `AsTool` 实现子 Agent 的上下文隔离。 |
| `workflow` | `go run ./examples/workflow` | `Sequential + Parallel + Loop` 与 Pipeline 构建器的编排教程。 |
| `pipeline` | `go run ./examples/pipeline` | 用 Pipeline 构建器以流水线方式组装多阶段流程。 |
| `refine` | `go run ./examples/refine` | 用 `Loop` 工作流做迭代式精炼，`exit_loop` 收敛退出。 |

### 计划执行（DAG）

| 示例 | 运行方式 | 展示内容 |
| --- | --- | --- |
| `planning` | `go run ./examples/planning` | 对比 `write_todos` 软规划与工作流硬执行的差异。 |
| `plan-dag` | `go run ./examples/plan-dag` | `agent.NewPlan` 的 DAG 执行器：带依赖的节点、真并发、最终审批与崩溃恢复。 |
| `plan-llm` | `go run ./examples/plan-llm` | LLM 生成计划（`agent.NewLLMPlan`）而非手工构建 DAG。 |
| `plan-approval` | `go run ./examples/plan-approval` | DAG 计划中的逐节点人工审批。 |
| `plan-replan` | `go run ./examples/plan-replan` | 动态重规划：执行器在运行中改写剩余计划。 |

### 记忆、RAG 与技能

| 示例 | 运行方式 | 展示内容 |
| --- | --- | --- |
| `memory-layers` | `go run ./examples/memory-layers` | 规则、项目、工作、文本记忆的分层装配、注入与恢复。 |
| `skills` | `go run ./examples/skills` | 技能三层渐进披露：发现技能、读取 `SKILL.md`/资源、在沙箱执行脚本。 |
| `prompt` | `go run ./examples/prompt` | 用身份、环境、工具指导、状态等可组合 Section 构建系统提示。 |

### 工作区（Workspace）

| 示例 | 运行方式 | 展示内容 |
| --- | --- | --- |
| `workspace/anatomy` | `go run ./examples/workspace/anatomy` | 工作区装配全貌：根解析与约定目录、四段提示词、文件工具的包含性、只读 git 快照、沙箱工作目录与提示词同源。 |
| `workspace/skills-gate` | `go run ./examples/workspace/skills-gate` | 技能侧接线：全局/工作区技能合并、Level-1 清单、`allowed-tools` 免审名单外的调用停在人工批准并批准后续跑。 |
| `workspace/policies` | `go run ./examples/workspace/policies` | 三层权限边界：`os.Root` 路径包含性、沙箱命令白名单、`skills.Gate` 与 `middleware.Permission` 的折叠结果。 |

三个示例都离线、无需密钥（`anatomy`/`policies` 不用模型，`skills-gate` 用 `llm/mock` 的确定性剧本）。真实模型的完整装配形态见 `examples/task-runner`。

### 中间件、HITL 与外部工具

| 示例 | 运行方式 | 展示内容 |
| --- | --- | --- |
| `middleware` | `go run ./examples/middleware` | 中间件教程：重试、限流、压缩、steering、RAG 等横切策略。 |
| `middleware-hooks` | `go run ./examples/middleware-hooks` | 把循环的六个阶段作为显式钩子逐一观察。 |
| `step-view` | `go run ./examples/step-view` | 每步冻结执行视图：同名注入同时替换 schema 与实现，AfterModel 读取本步模型实际看到的工具、设置与摘要。 |
| `mcp` | `go run ./examples/mcp` | 连接 MCP 服务并把远程工具提供给 Agent。 |
| `websearch` | `go run ./examples/websearch` | `web_search` 与 `web_fetch` 工具教程（默认离线）。 |

### 持久化、后台任务与运行时治理

| 示例 | 运行方式 | 展示内容 |
| --- | --- | --- |
| `persistent` | `go run ./examples/persistent` | 基于文件检查点的跨进程持久化运行；多次运行可见历史增长。 |
| `session` | `go run ./examples/session`（加 `-dir ./tmp-session` 保留目录，换个进程还能读同一个线程） | 会话门面：`OpenSession`/`Send`/`GetHead`/`ForkFrom`/子分支再跑，以及不带正文的 `History` 摘要分页与 cursor 误用。 |
| `managed-run` | `go run ./examples/managed-run`（`-dir` 保留共享目录；`-role submit\|worker\|status\|cancel` 配合 `-run`、`-redis` 分进程跑） | 受管任务：提交、worker 执行、只凭 RunID 重连等待、跨客户端取消、未知投递结果保持排队、显式修复与状态列表。 |
| `background` | `go run ./examples/background` | 离线 Pool 示例：Wait 取执行结果，WaitRecorded 等记录尝试，最后等待 worker 退出。 |
| `background-status` | `go run ./examples/background-status` | 后台运行的落盘与查询：`Handle.WaitRecorded` 确认终态记录、`Agent.RunStatus` 从文件存储读回状态与文件。 |
| `redis` | `go run ./examples/redis` | 基于 Redis 的持久化后台队列（需先启动 Redis）。 |
| `resilience` | `go run ./examples/resilience` | LLM 韧性层：多供应商回退与熔断。 |
| `config` | `go run ./examples/config` | 配置系统（基于 viper）的完整用法。 |
| `logger` | `go run ./examples/logger` | 日志系统（基于 zerolog）的用法。 |
| `otel` | `go run ./examples/otel` | 通过 `obs/otel` 接入 OpenTelemetry 追踪。 |

### 评测（eval）

| 示例 | 运行方式 | 展示内容 |
| --- | --- | --- |
| `eval-quickstart` | `go run ./examples/eval-quickstart` | 最小评测：对单个回答打分。 |
| `eval-dataset` | `go run ./examples/eval-dataset` | 对小型数据集运行打分评测。 |
| `eval-judge` | `go run ./examples/eval-judge` | LLM-as-judge：基于评分标准的打分。 |
| `eval-tool` | `go run ./examples/eval-tool` | 工具使用评测：检查工具调用是否正确。 |
| `eval-trajectory` | `go run ./examples/eval-trajectory` | 轨迹评测：不仅评结果，也评过程。 |
| `eval-reflect` | `go run ./examples/eval-reflect` | 自我反思：Agent 对自身输出进行批评与修订。 |

## 从示例走向应用

1. 从 `examples/quickstart` 复制 Agent 与工具的最小骨架，用 `agent.New(...)` + `Run`/`Stream`。
2. 用真实模型适配器替换 `llm/mock`，并通过环境变量或密钥管理服务注入凭证。
3. 需要连续对话与跨进程恢复时，用 `agent.WithCheckpointer(checkpoint.NewFile(dir))`，并在运行时用 `agent.OnThread(id)` 指定线程。
4. 有知识库需求时，用 `memory.NewRetriever(...)` 配合 `middleware.RAG(...)` 自动注入，或提供检索工具让模型按需调用。
5. 业务步骤固定时用 `Sequential` / `Parallel` / `Loop` / `Pipeline`；带依赖的复杂任务用 `agent.NewPlan`；需要领域专家协作时配置 `WithSubAgents` 或 `AsTool`。
6. 对外部命令、删除、支付等高风险动作，同时使用 `sandbox` 与 `middleware.Permission(...)`。

### 终稿后的追加与运行结束

- 运行中追加临时消息用 `Run.Steer`，它返回一张票据；`Steer` 成功只表示消息被队列接受，不表示已经进入模型上下文。
- 票据只保证进入某一步的模型上下文：`Delivered` 与 `Step` 说明哪一步会看到它，不表示检查点已经提交。模型给出终稿后才到达的消息进入下一回合，`Step` 是下一步。
- 一次运行可以有多个回合：`TurnDone` 会重复出现，`RunDone` 只在运行结束时出现一次。判断运行结束以 `RunDone` 或 `Run.Wait` 为准，不要把 `TurnDone` 当成结束信号。
- `Sequential`/`Loop` 等工作流内部的成员不启用终稿续跑，追加消息由共享队列按原有轮次处理。
- 跨进程、跨暂停的输入不能依赖内存队列，改用持久收件箱（`Durable.Deliver`/`Inbox`）。

完整可运行示例见 `go run ./examples/task-loop`。

### Run 的启动、等待与观察

生命周期的决策与兼容迁移见 [ADR-0035](docs/adr/ADR-0035-lazy-run-lifecycle.md) 和 [变更说明](docs/changes/2026-10-28-lazy-run-lifecycle.md)。

普通 `Stream` 创建句柄时冻结输入，保留惰性；取得 admission 后才恢复 checkpoint 历史。方法职责如下：

| 调用 | 启动 | 等待或观察 |
| --- | --- | --- |
| `run.Start()` | 幂等、非阻塞，只启动一次 | 不订阅，不等待 |
| `run.Await(waitCtx)` | 否 | 只等内存结算；超时只结束当前等待者，结果副本独立 |
| `run.Wait()` | 是 | `Start + Await(context.Background())`，无事件订阅 |
| 消费 `run.Iter()` | 是 | 先订阅再 Start；break 自动退订，不自动取消执行 |
| `run.Events(mode)` | 否 | 只订阅；调用方立即 `defer unsubscribe()` |
| `handle.Wait()` / `WaitRecorded()` | 否 | 依赖队列 worker，分别等结算 / 记录尝试 |

原始 channel 的用法是先订阅，再启动，并保持消费：

```go
run := a.Stream(ctx, "hello")
events, unsubscribe := run.Events(bus.Lossless)
defer unsubscribe() // 提前退出也要执行，解除背压或尾部交付等待。
run.Start()
for ev := range events { // 读到尾部后的 channel 关闭；按需处理 ev。
    _ = ev
}
result, err := run.Await(ctx)
_ = result
_ = err
```

只限制等待时间，用 `run.Await(waitCtx)`；需要停止执行，则先 `run.Cancel()` 再 `run.Await(context.Background())` 等实际结算。Prepared Cancel 同步内存结算，不发布 RunStarted 或根 TaskDone，尾部只有 RunFailed；Started Cancel 只提出请求，模型、工具、hook 和 store 仍需协作响应 context。

晚订阅活动 Run 不补历史进度；已封闭 Run 的 Iter/Events 仅返回有限尾部（根 TaskDone + 单个 RunDone/RunFailed/Interrupted；未进入根 Task 时只有 RunFailed），随后结束，多次消费不重跑。Wait 返回不保证观察者已读到尾部或尾部已进入 channel；不要在 Wait 后用非阻塞 select/default drain 判断流已结束。普通 Lossless 进度仍会背压，child TaskDone 仍是普通可取消事件。完整离线断言见 [lazy-run-lifecycle](examples/lazy-run-lifecycle/main.go)。

RecordApproval 的持久 receipt 是权威事实；sealed Run 不因再次记录审批而重开流。继续执行用 Resume，不依赖重订阅旧流确认审批。

### 生命周期观测

生命周期只向已有 span 添加 events，不创建新 span。记录创建、启动请求、真实 admission、首个有效模型 response 和结算时点。四个秒单位 histogram 为 `agent.run.creation_to_start`、`agent.run.start_to_admission`、`agent.run.admission_to_first_response`、`agent.run.start_to_settlement`，不附加 runID/threadID/prompt/tool args 等高基数或内容 labels。

生命周期 metrics 使用全局 `otel.GetMeterProvider()`，应用需通过 `otel.SetMeterProvider(...)` 配置；单独的 `obs/otel.WithMeterProvider` 仅影响 middleware。缺失阶段和负区间省略。Resume 可先 admission 再创建 Run，保留真实时间；首个有效 response 是非 nil 且无错误的 partial 或最终响应，不是首 token。

### 会话快照与并发

- checkpoint 历史保持 append-only。Memory 和 File checkpointer 都会冻结状态数据与可快照文件，`Load`、`Latest`、`History` 和 Fork 返回的内容可独立修改，不会反向污染历史。
- 普通 `Stream` 在创建句柄时冻结输入但不读取 checkpoint；`Start`、`Wait` 或消费 `Iter` 请求执行，取得 admission 后才加载当前 head；`Await`/`Events` 不启动。
- 同线程准入：普通 `Agent.Run`/`Stream` 默认快速失败，忙时在模型和工具执行前返回 `checkpoint.ErrClaimConflict`；`Session.Send` 默认按先进先出排队，等前一次运行结束并提交后才开始；`Resume` 固定快速失败，不接受运行选项。
- 排队默认允许每个线程 16 个等待者，超过返回 `agent.ErrAdmissionQueueFull`；在自己运行的线程里再次启动运行会直接返回 `agent.ErrAdmissionReentrant`，不会排队。不同线程互不影响。
- 运行级覆盖用 `agent.WithAdmission(agent.AdmissionOptions{Policy: agent.AdmissionQueue})`（或 `AdmissionFail`），`Agent.Run`/`Stream` 与 `Session.Send` 都接受；`Session.Send` 不传时使用排队。
- 持久 claim 仍是跨进程的正确性来源：准入名额只在一个 Agent 进程内排序并减少冲突；另一个进程或另一个 Agent 仍由 `Claim`/租约拒绝。
- 暂停线程必须用 `Resume` 继续，普通 `Run`/`Stream` 返回 `checkpoint.ErrResumeRequired`。Prepared Resume 在 drive 前会验证租约，过期时返回 `checkpoint.ErrStaleFencingToken`，不会重新执行已批准工具。
- Memory/File 默认实现线程协调。第三方 checkpointer 只有显式实现 `checkpoint.ThreadCoordinator` 才有同等跨实例保证；可用 `agent.WithThreadCoordinationRequired(true)` 在 `agent.New` 时强制检查。
- `vfs.InState` 等 `core.Snapshottable` 后端会进入快照。真实目录或远端等外部托管后端需要在每个恢复进程重新传 `agent.WithRunFiles`，其内容不随时间旅行回滚。
- `checkpoint.Fork` 现在返回 `(*checkpoint.Checkpoint, error)`，且只构造未保存的分支快照；调用方必须检查错误并显式 `Save`。完整迁移方式和并发边界见 [ADR-0032](docs/adr/ADR-0032-session-snapshot-consistency.md)。

### 任务树预算

- `agent.WithTaskBudget` 约束一次运行开始的整棵任务树：默认最多嵌套 8 层、64 个子任务，模型调用次数默认不限（`MaxTurns: 0`）。子 Agent、`Parallel` 分支和计划节点都从同一棵树扣额度；每次派生子任务先检查层级，再占用一个子任务名额。
- 超预算在发生处是硬错误，不会转成“要一句总结”的收尾：`AsTool` 把错误作为工具结果交回模型，工作流分支或计划节点在启动时失败。`MaxTurns` 大于 0 时，树内每次模型尝试也计数，重试同样计入。
- 暂停后恢复会开一棵新树、计数从零开始；`TaskID` 等 Task 信息只是运行期元数据，不写进 checkpoint。需要跨恢复累计时，由调用方自己记账。
- 和 `middleware.RunBudget` 不同：后者约束单次运行的轮数、token、成本与时长，可以收尾成一句回答；任务树预算没有 token 维度，超限就是错误。
- `Sequential`/`Parallel`/`Loop`/`Plan` 这类工作流根没有公开的预算选项，使用默认的 8 层 / 64 个子任务；只有 `agent.New(agent.WithTaskBudget(...))` 创建的 LLM Agent 可以配置。它作为别人的子任务运行时，使用父树的预算。

### 输入的投递阶段与崩溃恢复

- 消息可以指定最晚投递点：`before_model`（默认）、`after_model`、`after_tools` 会在下一次 `PrepareTurn` 或工具批次结算后进入上下文；`next_turn` 只在新回合开始时投递，回合中途一直留在队列里。`Run.Steer(msg)` 等价于 `SteerWith(msg, SteerOptions{})`，需要别的阶段时用 `SteerWith`。
- 票据只说明消息进入了哪一步的模型上下文，不代表检查点已提交。临时消息本身不落盘；持久收件箱的消息与消费它的那一步同一次提交（确认随检查点一起生效）。跨进程、跨暂停的输入用 `Durable.Deliver`/`Inbox`。
- 模型调用抢占默认关闭。开启 `agent.WithInputPreemption(true)` 后，只有临时消息的 `before_model` 阶段可以带 `Preempt: true`，取消当前模型调用并重试同一步；未开启时 `SteerWith` 返回 `ErrPreemptionDisabled`，阶段不符返回 `ErrPreemptionPhase`，两者都不会生成票据。
- 工具批次执行前会先把「这一步要执行哪些调用」写成 `InFlight` 边界。进程在批次中途消失时，head 停在 `InFlight`，`RunStatus.InFlight=true`、`Paused=false`；它与等待审批的暂停是两回事：暂停在等人作决定，在途没有人要等，直接用 `Agent.Resume` 继续。
- `Resume` 按台账补齐：`pending` 调用执行一次，`completed` 结果直接回放且不再跑 handler；`claimed` 表示上次执行过但没有留下结果，框架不猜结局、不重放副作用，以错误结果交给模型，只有外部系统能查证时才由业务侧另行对账。
- 恢复完成后每个计划调用仍然恰好一条结果，最终头部不再带 `InFlight`。可运行示例见 `go run ./examples/input-phases` 与 `go run ./examples/inflight-recovery`。

### 后台运行的终态与状态查询

- 归属：入队成功后运行归队列所有。取消传给 `EnqueueAgent` 的 context 只结束提交这一侧；`Pool` 的 context 取消只是停止取新任务并等待在途任务。要中止一次已经跑起来的后台运行，用 `Handle.Cancel`（同时中止运行并释放队列持有的执行 context）。
- 启动权：`Handle.Wait` 改为纯等待，不能驱动运行；需要启动 `Pool`/worker，或改用直接执行的 `Agent.Run`。Handle 不提供 Start/Await。未消费的 prepared 任务取消后，Wait 可以返回；无 worker 的 WaitRecorded 仍等待。后续 worker 不执行模型，但仍尝试记录；无本次提交边界时保留 `agent.ErrNoCommittedBoundary`。
- 两个终点：`Handle.Wait` 只说"运行已内存结算"；`Handle.WaitRecorded` 还要等 worker 尝试记录终态并保存收尾结果，返回 `errors.Join(运行错误, 记录错误)`。completed 且错误为 nil 才是"记录已确认"；暂停的运行没有终态可记，返回结果与 nil 记录错误。没有任何 worker 消费的任务，`WaitRecorded` 会一直等下去——它是等 worker，不自己驱动运行。
- 谁写终态：只有 `Agent.RecordCompletion(ctx, run)`（队列在运行结束后调用它）。默认前台 `Run`/`Stream`/`Resume` 不写终态记录。它拒绝：`nil` 运行（`agent.ErrNoRun`）、别的 Agent 的运行（`agent.ErrRunNotOwned`）、未结束（`agent.ErrRunNotFinished`）、暂停（`agent.ErrRunPaused`）、这次运行没有自己的成功提交边界（`agent.ErrNoCommittedBoundary`）、边界停在未解决的工具批次（`checkpoint.ErrInFlightConflict`）、存储不支持原子追加（`checkpoint.ErrDurableUnavailable`，不会退回普通 `Save`）。
- 怎么写：从冻结的提交边界 `Load` 并核对线程/ID/revision，克隆状态与文件快照，用 `ExpectedRevision` 追加一条带 `Completion` 的快照。head 已被别的运行推进 → `checkpoint.ErrRevisionConflict`；线程被别人占着 → `checkpoint.ErrClaimConflict`；本次租约过期 → `checkpoint.ErrStaleFencingToken`。框架不会读新 head 再把旧终态改写一遍——遇到冲突就接受冲突，用 `RunStatus` 看当前事实，要不要再记录由那一轮自己决定。
- 崩溃窗口：进程在记录写完之前退出，这次运行就没有终态记录；存储里的 head 是运行中间提交的那一条，可以直接恢复继续跑，但不要把它当"本次运行的结果"。无人等待 `WaitRecorded` 时，框架不会主动通知记录失败。
- 三条时间线各管一件事：记录写入用 `context.WithoutCancel` 派生的 30 秒期限，只约束这次 I/O 的合作式等待（存储自己不看 context 就拦不住）；整次运行的挂钟归 `middleware.RunBudget.MaxDuration`，到点是"撤下工具、要一句最终回答"的正常收尾，不是打断在途调用；单个工具调用归 `agent.WithToolTimeout`，它才是能放弃不回应的工具的那条线。
- `Agent.RunStatus(ctx, threadID)` 是纯读：不写、不取 claim、不续租。未知线程不是错误（`Exists=false`、`Latest=nil`），存储错误原样返回，非法线程 ID 在触达存储之前被拒。`Paused` 只说明 head 上有暂停或待办元数据，不保证 `Resume` 一定成功（审批、租约、恢复类型仍要过）。head 上的 `Completion` 属于写下它的那次运行：新一轮可能已经开始但还没提交任何东西，此时读到的还是上一轮的终态——不能读成"当前没有在跑"。
- 暂停与在途是两件事，`RunStatus` 用 `Paused` 和 `InFlight` 分开报告。`Paused` 表示 head 在等人作决定（审批或待办工具），下一步是带审批的 `Resume`；`InFlight` 表示 head 是一个工具批次执行前的边界，批次结果还没全部落账，没有人要等，下一步是 `Resume` 按台账补齐。两者互斥：存储拒绝同时带 `Pending`/`Pause` 与 `InFlight` 的快照；普通新运行、`Fork` 和终态记录都会拒绝在途头。`Session.History` 的摘要也分开这两个布尔值。

  | 维度 | 人工审批暂停 | 在途批次 |
  | --- | --- | --- |
  | 存储字段 | `Pending` + `Pause` | `InFlight` |
  | 是否等待外部决定 | 是 | 否 |
  | `RunStatus.Paused` / `RunStatus.InFlight` | true / false | false / true |
  | 恢复入口 | `Resume` + 审批 | `Resume` |
  | 恢复时是否重跑 `BeforeTool` | 是 | 否 |
  | 恢复时是否使用台账 | 是 | 是 |
  | 普通新运行 / 终态记录 | 拒绝 | 拒绝 |
  | `Fork` | 清空暂停元数据后允许 | 拒绝 |

保持不做：公开 detach API、事件重放、跨进程 agent job、"正在运行"登记表、整次运行的硬期限、前台路径自动写终态记录。

其中"跨进程 agent job"与"按 RunID 查询"已由 [ADR-0034](docs/adr/ADR-0034-session-task-management.md) 以受管任务形态落地（版本化 Type/Payload + 独立任务记录 + 只读客户端）；本节其余各项仍不做，`EnqueueAgent` 的闭包路径与 `Agent.RunStatus` 的旧契约不变。

### 会话门面与受管任务

- 三个等待点分别回答三个问题。`Handle.Wait`（闭包路径）说"这次运行结束了"；`Handle.WaitRecorded` 还要等 worker 把终态快照写进检查点存储；`RunClient.WaitRun(ctx, runID)` 只看任务记录，任何拿着 RunID 的新进程都能用，它不驱动运行、不修复记录、不续租。
- `WaitRun` 把 paused 也算作本次运行的结束：暂停的运行要继续时是新建一条记录，由建记录的一方用 `ResumeOfRunID` 指回旧的那条（存储把它当不可变身份），所以等待不会挂在等人审批上（`RunClient.GetRun` 同样只回报记录此刻的样子，租约过期不当失败也不当取消）。
- 执行结局与记账事实是两条独立记录：任务记录的 `Error` 说运行本身，`RecordError` 说终态快照没写成功（错误码 `completion_record_failed`，可重试）。运行 succeeded 而 `RecordError` 非空是合法组合，`RunSummary` 两个错误码都带。
- 投递结果不确定时不猜：队列错误不证明拒收就保持 `queued` 并挂 `dispatch_unknown`，RunID 照样还给调用方；只有明确拒收才写 `failed/enqueue_failed`。重复投递由领取那一次 compare-and-swap 挡住，不会第二次调模型和工具；副作用已经发生而结果未知时，框架不自动重放，也不替它编结局。
- 崩溃之后的修复是显式入口的活：`ReconcileRun` 只认这条 RunID 已提交的终态快照，凭它补齐任务记录；找不到匹配证据就报 `stale_no_completion` 并原样保留记录，证据互相矛盾就报 `contradictory_evidence` 拒绝修。陈旧记录不会被任何读取或等待自动重启。
- 摘要分页不带正文：`Session.History`、`QueryCheckpoints`、`ListRuns` 返回身份、状态、时间和引用，不含消息、KV 或文件字节。第一次查询冻结投影，续页读同一代，所以边走边写不会重复或漏项；新写入只对新建的查询可见。token 被改内容、换过滤条件复用会报 `ErrInvalidCursor`，投影过期（15 分钟或超出 128 份缓存）报 `ErrCursorExpired`——处置方式是重起一次查询，不是重试旧 token。
- 能力缺失就明确拒绝：只实现 `checkpoint.Checkpointer` 四个方法的外部存储仍可跑普通与受管执行，但 `Session.History` 报 `ErrCapabilityUnavailable`、`Session.ForkFrom` 与终态记录报 `ErrDurableUnavailable`，`NewRunReconciler` 在构造期就拒。框架不会退化成"把整个线程读一遍"。
- 外部托管的文件后端不随作业搬运、也不随快照回滚：`AgentResolver` 在每个 worker 进程重建 agent 与后端，运行级选项 `agent.WithRunFiles` 由要用它的那次执行自己带上。
- 本批按单个可信存储目录设计，不接收客户端自带的 tenant 字符串当作隔离手段；多租户要靠上层认证后选择独立目录/存储，完整 namespace 能力与服务端鉴权另立 ADR。

## 目录概览

```text
agent/        Agent 声明与运行时；工作流与 DAG 计划
core/         消息、事件、状态、指令等公共模型
llm/          模型抽象、韧性层及厂商适配器（anthropic/openaicompat/agnes/mock）
tool/         类型化工具、MCP、Web 与命令执行工具
checkpoint/   显式状态快照持久化（内存 / JSONL 文件）
memory/       向量检索、RAG 与分层记忆（memx/rules/projectmem/workingmem/textmem）
middleware/   重试、限流、压缩、Steering、HITL、RAG
prompt/       可组合系统提示 Section
bus/          进程内事件总线（pub/sub）
queue/        后台任务队列与 Worker 池（内存 / Redis Stream）
sandbox/      外部进程执行策略与默认实现
skills/       文件系统技能包与脚本资源
vfs/          虚拟文件系统后端（core.FileStore 实现）
config/       配置系统（基于 viper）
logger/       日志系统（基于 zerolog）
eval/         评测与打分
obs/          可观测性（OpenTelemetry 集成）
examples/     可独立运行的端到端示例
```

## 贡献与反馈

欢迎通过 Issue 或 Pull Request 提交问题、示例和改进。提交前请至少运行：

```bash
go test ./...
```
