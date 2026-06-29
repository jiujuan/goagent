# ADR 0026：评估系统 —— Scorer 统一抽象 + 在线自纠/离线回归双闭环

状态：提议

> 本文给 v2 增加"评估"能力：评 LLM 回答质量、评 Agent 执行轨迹、评 Tool 执行结果，
> 并把评估接回执行，形成两条**闭环**——在线自纠（运行时把这次答案做对）与离线回归
> （迭代里把系统整体做好），目标是让产出的答案尽量高质量、符合人的期望。

---

## 背景

到目前为止 goagent v2 能"把事做完"（AgentLoop + 工具 + 中间件 + 子智能体 + 持久化），
但没有任何东西回答"**做得好不好**"。缺了这一环会有三个具体问题：

1. **无客观度量**：改了 prompt / 换了模型 / 调了工具，答案是变好还是变差，只能靠人肉抽看。
2. **无回归防线**：一次改动修好 A 场景却悄悄弄坏 B 场景，CI 拦不住。
3. **无自我纠偏**：模型给出一个粗糙答案就直接返回给用户，运行时没有"再想一遍"的机会。

业界参考：OpenAI Evals / Ragas / DeepEval 都把"评估"建模为**一组打分器（metric）作用在
一条样本上**；LangSmith / braintrust 进一步把"在数据集上批量跑 + 回归对比"产品化；
Reflexion / Self-Refine 则把"模型自己评、自己改"做成运行时循环。

本项目刻意**不引入新范式**。我们已经有三个最小扩展点（呼应 [ADR 0016](0016-memory-system.md)
的判断）：`prompt.Section`、`agent.Middleware`、`tool.Tool`，以及一个被验证过的精炼循环
原语 `agent.Loop` + `ExitLoopTool`（见 `examples/refine`）。评估系统应当**长在这些原语上**，
而不是另造一套 Harness 框架。

## 决策

新建顶层包 `eval`，只依赖下层（`core` / `llm` / `tool` / `agent` / `embeddings`），
无人反向依赖它——DAG 不变。围绕**一个核心抽象**与**两条闭环**展开。

### 一、核心抽象：一切皆 `Scorer`

把"被评判的对象"与"如何评判"解耦，是同一套设计能覆盖 answer / agent / tool 三类评估的关键。

```go
// Sample 是一次被评判的对象；三类评估按需填字段。
type Sample struct {
    Input     string         // 用户请求
    Output    string         // 被评的答案
    Reference string         // 可选：gold/参考答案
    Traj      *Trajectory    // 可选：整条 Agent 轨迹（评 Agent）
    Tool      *ToolEpisode   // 可选：单次工具调用（评 Tool）
    Meta      map[string]any
}

// Score 归一到 0..1，可组合（Sub 给出分项明细）；Reason 同时是在线闭环的反馈来源。
type Score struct {
    Name   string
    Value  float64
    Passed bool
    Reason string
    Sub    []Score
}

// Scorer 是唯一的评估契约——规则、嵌入、LLM 裁判全都实现它。
type Scorer interface {
    Name() string
    Score(ctx context.Context, s Sample) (Score, error)
}
```

`Trajectory` / `ToolEpisode` 直接复用 `core` 词汇（`Message` / `ToolCall` / `ToolResult` /
`Usage`），不发明新结构：

```go
type ToolEpisode struct { Call core.ToolCall; Result core.ToolResult; Latency time.Duration }
type Trajectory struct {
    Input    string
    Messages []core.Message
    Tools    []ToolEpisode
    Final    core.Result
    Usage    core.Usage
    Steps    int
}
```

### 二、Scorer 家族（从便宜到贵，可叠加）

| 层 | 文件 | 成本 | 代表 |
|---|---|---|---|
| 规则（确定性、无 LLM） | `rule.go` | 0 | `ExactMatch` `Contains` `Regex` `JSONValid` `JSONSchema` `NumericClose` `NoToolError` `MaxSteps` `TokenBudget` |
| 嵌入相似度 | `embed.go` | 低 | `SemanticSimilarity(embeddings.Embedder)`（cosine→0..1） |
| LLM-as-Judge | `judge.go` | 高 | `Rubric` `Reference` `Faithfulness` `Pairwise` `TrajectoryJudge` |
| 组合 | `composite.go` | — | `Weighted` `All` `Any`（产出带 `Sub` 的聚合分） |

**LLM-as-Judge 约定**：裁判用一个独立（通常更便宜的）`llm.Model`，被要求返回结构化
`{"score": n, "reason": "..."}` JSON（沿用仓库"结构化输出走 JSON"的做法），解析失败重试；
`Pairwise` 正反两序各跑一次消除位置偏置。裁判离线可测——`llm/mock` 脚本化返回固定评分。

**三类评估如何映射**（只是填 `Sample` 不同字段 + 选不同 Scorer，无新机制）：

| 评估对象 | 填充字段 | 典型 Scorer 组合 |
|---|---|---|
| Tool 执行结果 | `Sample.Tool` | `NoToolError` + `JSONSchema` + `Faithfulness` |
| LLM 回答质量 | `Sample.Output/Reference` | `Rubric` + `SemanticSimilarity` + `Reference` |
| Agent 执行结果 | `Sample.Traj` | `Weighted(TrajectoryJudge, MaxSteps, TokenBudget, 终局 Reference)` |

### 三、数据采集：runtime → eval 的唯一桥

Agent 已经把 `core.Event` 打到 `bus`、`Run.Wait()` 返回 `core.Result`。`record.go` 提供一个
订阅者，把事件流装配成 `Trajectory`，评估器只消费**已有的可观测事件**，不碰 agent 内部——
与 `middleware.Tracing` 同一性质（纯观察者）。

```go
func Record(run *agent.Run) (*Trajectory, core.Result, error)
```

### 四、闭环 A —— 在线自纠（运行时，落在 `agent.Loop` 上）

> ⚠️ 设计约束（已对照 `agent/loop.go` 核实）：AgentLoop 在模型给出**无工具调用的终答**时
> 立即返回，`AfterModel` 返回 `Continue` **不能**阻止这次终止。因此"再想一遍"必须由
> **外层 `agent.Loop`** 驱动重跑，而非在单次 run 内续写。这正是 `examples/refine` 的形态。

把 LLM-as-Judge 接成一个 **Escalate 门**中间件，挂在 worker 上，外面套 `agent.Loop`：

```
agent.Loop("refine", N, worker)         // worker.WithMiddleware(eval.Gate(scorer, thr))
   每轮:  worker 产出终答 ─AfterModel─▶ Gate 打分
                                          │ Passed?
                              ┌───────────┴────────────┐
                           是 │                        │ 否
                              ▼                        ▼
                     返回 Escalate(跳出 Loop)   把 Reason 写回 State 作为下一轮反馈
                                                返回 Continue(让 Loop 再跑一轮，上限 N)
```

```go
// Gate：达标→Escalate 跳出外层 Loop；不达标→把批评写进 State 反馈下一轮。
func Gate(scorer Scorer, threshold float64) agent.Middleware
// ToolGuard：AfterTool 评分，坏结果改写为 IsError+批评，模型在下一轮自然换参重试。
func ToolGuard(check Scorer) agent.Middleware
```

`Gate` 复用 `terminalFromDirective`（Escalate 即终止本 run 并由 `loopRunner` 跳出循环）
与 State 共享（`loopRunner` 各轮共用同一 `RunContext`，反馈自然流入下一轮历史）。
**不改 AgentLoop 一行**。这是"让答案尽量高质量"的运行时杠杆。

### 五、闭环 B —— 离线回归（数据集，CI 门禁）

```go
type Case    struct { Name, Input, Reference string }
type Dataset []Case
type Harness struct { Agent *agent.Agent; Scorers []Scorer }
func (h *Harness) Run(ctx context.Context, ds Dataset) (Report, error) // 每例 Run→Record→打分

type Report struct { Cases []CaseResult; Mean map[string]float64; PassRate float64 }
func (r Report) Print()        // 终端表格
func (r Report) JSON() []byte  // CI artifact
```

闭环：`跑 Dataset → Report → 看回归/失败 → 改 prompt/工具/模型 → 再跑`；用 `Pairwise`
比"新版 vs 旧版"做回归门禁；**失败 case 回流进 Dataset**，评估集越长越强。

### 六、包布局

```
eval/
  eval.go       Sample / Score / Scorer / Trajectory / ToolEpisode
  rule.go       确定性评分器
  embed.go      SemanticSimilarity(embeddings.Embedder)
  judge.go      LLM-as-Judge（裁判返回结构化 JSON）
  composite.go  Weighted / All / Any
  record.go     Record(run) → Trajectory（bus 桥）
  harness.go    Case / Dataset / Harness / Report（闭环 B）
  loop.go       Gate / ToolGuard 中间件（闭环 A）
examples/eval-{quickstart,judge,tool,trajectory,dataset,reflect}/
```

对齐 ADR 0016：eval 通过 **Middleware（闭环 A）+ 独立 Harness（闭环 B）+ 可选 self-check
Tool（让 agent 自评）** 三个扩展点接入，不侵入核心。

## 理由

- **零新范式**：`Scorer` 与 `tool.Tool` 一样是小接口；闭环 A = `Directive`+`Loop`+`Middleware`，
  闭环 B = 在 `Run`/`Record` 外面套个数据集循环。`agent`/`core` 契约不变。
- **三评估一抽象**：answer/agent/tool 只是 `Sample` 的不同切面，避免三套并行框架。
- **由便宜到贵**：规则与嵌入零/低成本兜底，裁判只在需要语义判断时上场，CI 成本可控。
- **离线可测**：裁判用 `mock` 脚本化，全部 demo 默认离线 `go run`，`EVAL_LIVE=1` 切真模型。

## 后果

- 裁判会引入额外 model 调用（成本与延迟）；用规则/嵌入前置过滤、`Weighted` 控权重缓解。
- LLM-as-Judge 有自身偏差（位置、长度、自我偏好）；`Pairwise` 双序、量规明确化只能缓解不能根除，
  关键场景仍需人工校准评估集。
- `Trajectory` 目前从 `bus` 事件重建，token 统计依赖 provider 在 `Usage` 上的填充质量。
- `Gate` 的反馈注入依赖 `loopRunner` 各轮共享 `RunContext`/State 这一现状；若未来工作流隔离子运行
  状态，需要改走显式 `State.KV` 传递（接口不变）。

## 备选方案

- **单独的 Eval 框架（仿 OpenAI Evals 的 YAML + registry）**：表达力强但与 Go 代码割裂、样板多；
  本项目选择"评估器即普通 Go 值（Scorer）"，可直接组合、单测、塞进 CI。
- **在线闭环用 `AfterModel` 返回 Continue 续写**：已核实不可行（无工具调用终答会直接返回），
  故改用外层 `agent.Loop` + Escalate 门，与既有 `refine` 模式一致。
- **把评估塞进 `middleware` 包**：评估是独立关注点且要被 CI 单独引用，单列 `eval` 包更清晰；
  `eval` 反过来**产出** `agent.Middleware`（`Gate`/`ToolGuard`），依赖方向仍是 eval→agent。
