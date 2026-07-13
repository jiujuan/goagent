# eval —— 评估系统与评估闭环

`eval` 给 goagent 回答一个问题:**产出的答案到底好不好?** 它评估三类对象,并把评估接回
执行,形成两条闭环,让答案质量尽量高、符合人的期望。

- **评 LLM 回答质量** —— 最终答案 vs 参考答案 / 评分量规
- **评 Agent 执行结果** —— 整条轨迹:用对工具没?有没绕圈?步数/token 预算?结局对不对?
- **评 Tool 执行结果** —— 单次 `ToolCall`+`ToolResult`:schema 合法?是否报错?是否解答了调用?

> 设计依据见 [ADR 0026](../docs/adr/0026-eval-system.md);完整用法见 [docs/EVAL.md](../docs/EVAL.md)。
> 本包只依赖下层(`core`/`llm`/`tool`/`agent`/`embeddings`),无人反向依赖它,依赖图保持无环。
> 全部示例默认离线 `go run`(裁判用 `llm/mock` 脚本化),设 `EVAL_LIVE=1` 且配 `AGNES_API_KEY` 切真实模型。

---

## 核心抽象:一切皆 `Scorer`

把「被评判的对象」与「如何评判」解耦,是同一套设计能覆盖三类评估的关键。

```go
// Sample 是一次被评判的对象;三类评估按需填字段。
type Sample struct {
    Input     string       // 用户请求
    Output    string       // 被评的答案
    Reference string       // 可选:gold/参考答案
    Traj      *Trajectory  // 可选:整条 Agent 轨迹
    Tool      *ToolEpisode // 可选:单次工具调用
}

// Score 归一到 0..1,可组合(Sub 给分项明细);Reason 也是在线闭环的反馈来源。
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

最小上手:

```go
sc, _ := eval.Contains("退款").Score(ctx, eval.Sample{Output: "请在订单页申请退款"})
fmt.Println(sc.Value, sc.Passed) // 1 true
```

---

## Scorer 家族(从便宜到贵,可叠加)

| 类别 | 用途 | 代表 |
|---|---|---|
| 规则(零成本) | 精确/包含/正则/JSON/数值/工具无错/步数/预算 | `ExactMatch` `Contains` `Regex` `JSONValid` `JSONSchema` `NumericClose` `NoToolError{}` `MaxSteps` `TokenBudget` |
| 嵌入相似度 | 与参考答案的语义接近度 | `SemanticSimilarity(embedder)` |
| LLM 裁判 | 需要语义/主观判断 | `Rubric` `Reference` `Faithfulness` `TrajectoryJudge` `Pairwise`(正反两序去位置偏置) |
| 组合 | 加权/全过/任一过/改名/取反 | `Weighted(Weight(s,w)...)` `All(...)` `Any(...)` `Named("退款", ...)` `Not(...)` |

```go
scorer := eval.Weighted(
    eval.Weight(eval.Contains("退款"), 0.3),
    eval.Weight(eval.Rubric(judge, "回答是否专业、可执行"), 0.7),
)
sc, _ := scorer.Score(ctx, sample) // sc.Sub 给出每项明细
```

> 同种评分器用多次(如两个 `Contains`)记得 `Named("退款", Contains("退款"))` 起唯一名,
> 否则在 `Report` 里会按相同 `Name` 合并成一列。

---

## 评估闭环

### 闭环 A · 在线自纠(运行时把答案做对)

把裁判接成 **Escalate 门**中间件,挂在 worker 上,外面套 `agent.Loop`:不达标就用裁判的
意见反馈、重跑一轮;达标就 `Escalate` 跳出循环。这复用了 `examples/refine` 的精炼形态,
**不改 AgentLoop 一行**。

```go
worker, _ := agent.New(
    agent.WithModel(model),
    agent.WithInstruction("回答问题;若上文有【评审意见】就据此改进。"),
    agent.WithMiddleware(
        eval.Gate(eval.Rubric(judge, "是否完整、准确、可执行"), 0.8), // 达标→跳出
        eval.ToolGuard(eval.JSONSchema(schema)),                   // 坏工具结果→打回重试
    ),
)
answer, _ := agent.Loop("refine", 3, worker).Run(ctx, task)
```

### 闭环 B · 离线回归(数据集 + CI 门禁)

```go
rep, _ := (&eval.Harness{
    Agent:   sut,
    Scorers: []eval.Scorer{eval.Reference(judge), eval.SemanticSimilarity(embedder)},
}).Run(ctx, eval.Dataset{
    {Name: "退货", Input: "我想退货", Reference: "在订单页申请退款,7 天内原路退回"},
    // ...失败用例回流进来,评估集越跑越强
})
rep.Print()                         // 终端表格:每例分数 + 均值/通过率
if rep.PassRate < 0.9 { os.Exit(1) } // 质量门禁:不达标则拦截 CI
```

`Record(run)` 是 runtime→eval 的桥:订阅一次 run 的事件流重建 `Trajectory`,供轨迹/工具评分。

---

## 示例

全部离线 `go run` 可跑;设 `EVAL_LIVE=1 AGNES_API_KEY=sk-...` 切真实模型。

| 示例 | 演示 |
|---|---|
| [`examples/eval-quickstart`](../examples/eval-quickstart) | 规则 + 裁判组合给单个答案打分,看 `Sub` 分项 |
| [`examples/eval-judge`](../examples/eval-judge) | `Rubric` 绝对打分 + `Pairwise` A/B 相对比较(去位置偏置) |
| [`examples/eval-tool`](../examples/eval-tool) | 单次工具结果评估(纯规则,零 LLM):正常/报错/结构不合约 |
| [`examples/eval-trajectory`](../examples/eval-trajectory) | 跑 agent → `Record` 轨迹 → 轨迹质量+步数+终答加权评分 |
| [`examples/eval-dataset`](../examples/eval-dataset) | `Harness` 批量打分 → `Report` 表格 → CI 门禁(通过率不达标 exit 1) |
| [`examples/eval-reflect`](../examples/eval-reflect) | 在线闭环自纠:粗答 → 裁判打回 → 带反馈重写 → 达标 |

```bash
go run ./examples/eval-quickstart     # 离线 mock
go run ./examples/eval-reflect        # 看「答案自动变好」
go run ./examples/eval-dataset        # 看 CI 门禁(故意 exit 1)
EVAL_LIVE=1 AGNES_API_KEY=sk-... go run ./examples/eval-judge   # 真实裁判
```

---

## 测试

```bash
go test ./eval/
```

确定性规则评分器、`extractJSON` 容错、`Pairwise` 去偏置、`Record` 轨迹重建、`Harness` 聚合、
`Gate`/`ToolGuard` 端到端自纠均有覆盖,全程用 `mock` 裁判离线运行。
