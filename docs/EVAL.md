# Eval 评估系统使用指南

> 设计依据见 [ADR 0026](adr/0026-eval-system.md)。本文讲**怎么用**。

评估系统回答一个问题：**答案到底好不好？** 它覆盖三类对象——LLM 回答质量、Agent 执行轨迹、
Tool 执行结果——并把评估接回执行，形成两条闭环：

- **在线自纠**（闭环 A）：运行时让模型"再想一遍"，把这次答案做对。
- **离线回归**（闭环 B）：在数据集上批量打分 + 版本对比，把系统整体做好。

所有示例默认离线可跑（裁判用 `llm/mock` 脚本化）；设 `EVAL_LIVE=1` 并提供 API Key 切真模型。

---

## 1. 核心概念：`Scorer`

一切评估都是一个 `Scorer` 作用在一个 `Sample` 上，产出归一到 `0..1` 的 `Score`。

```go
sample := eval.Sample{Input: "我想退货", Output: "可在订单页申请退款，7 天内原路退回。"}

sc, _ := eval.Contains("退款").Score(ctx, sample)
fmt.Println(sc.Value, sc.Passed) // 1.0 true
```

### Scorer 家族

| 类别 | 用途 | 例子 |
|---|---|---|
| 规则（零成本） | 精确/包含/正则/JSON/数值/工具无错/步数/预算 | `ExactMatch` `Contains("x")` `Regex(re)` `JSONSchema(s)` `NumericClose(42, 0.01)` `NoToolError{}` `MaxSteps(8)` `TokenBudget(4000)` |
| 嵌入相似度 | 与参考答案语义接近度 | `SemanticSimilarity(embedder)` |
| LLM 裁判 | 需要语义/主观判断 | `Rubric(judge, "是否专业可执行")` `Reference(judge)` `Faithfulness(judge)` `TrajectoryJudge(judge, "...")` `Pairwise(judge)` |
| 组合 | 加权/全过/任一过/改名 | `Weighted(Weight(s,w)...)` `All(...)` `Any(...)` `Named("退款", Contains("退款"))` |

### 组合打分

```go
scorer := eval.Weighted(
    eval.Weight(eval.Contains("退款"), 0.3),
    eval.Weight(eval.Rubric(judge, "回答是否专业可执行"), 0.7),
)
sc, _ := scorer.Score(ctx, sample)
// sc.Value 是加权总分；sc.Sub 给出每个分项的明细
```

---

## 2. 三类评估

### 评 LLM 回答质量

```go
eval.Weighted(
    eval.Weight(eval.SemanticSimilarity(embedder), 0.4),
    eval.Weight(eval.Reference(judge), 0.6), // 对照 gold 判正确性
).Score(ctx, eval.Sample{
    Input: q, Output: answer, Reference: gold,
})
```

### 评 Tool 执行结果

```go
eval.All(
    eval.NoToolError{},               // result.IsError == false
    eval.JSONSchema(weatherSchema),   // 输出符合约定结构
).Score(ctx, eval.Sample{Tool: &eval.ToolEpisode{Call: call, Result: result}})
```

### 评 Agent 执行轨迹

先用 `Record` 从一次 run 收集轨迹，再打分：

```go
run := a.Stream(ctx, "订单 A1001 运到上海一共多少钱？")
traj, res, _ := eval.Record(run)

eval.Weighted(
    eval.Weight(eval.TrajectoryJudge(judge, "工具用得对、无冗余调用、步数合理"), 0.5),
    eval.Weight(eval.MaxSteps(8), 0.2),
    eval.Weight(eval.Reference(judge), 0.3),
).Score(ctx, eval.Sample{
    Traj: traj, Output: res.Message.Text(), Reference: gold,
})
```

---

## 3. 闭环 A —— 在线自纠（运行时把答案做对）

把裁判接成 **Escalate 门**中间件，挂在 worker 上，外面套 `agent.Loop`：不达标就用裁判的
意见反馈、重跑一轮；达标就跳出循环。这复用了 `examples/refine` 的精炼循环形态。

```go
worker, _ := agent.New(
    agent.WithModel(model),
    agent.WithInstruction("把需求拆成验收标准；若上文有评审意见就据此改进。"),
    agent.WithMiddleware(
        eval.Gate(eval.Rubric(judge, "是否完整、准确、可验收"), 0.8), // 达标→跳出
        eval.ToolGuard(eval.NoToolError{}),                        // 坏工具结果→打回重试
    ),
)

// 最多精炼 3 轮：达标提前停，否则带着反馈再来一轮。
refine := agent.Loop("refine", 3, worker)
answer, _ := refine.Run(ctx, "把这段需求拆成验收标准")
```

- `Gate(scorer, threshold)`：worker 终答达标 → 返回 `Escalate` 跳出 `agent.Loop`；不达标 →
  把 `Score.Reason` 写回 State 作为下一轮反馈，返回 `Continue` 让 Loop 再跑（上限由 Loop 的轮数控制）。
- `ToolGuard(check)`：`AfterTool` 给工具结果打分，不达标改写为 `IsError` + 批评，模型下一轮自然换参重试。

---

## 4. 闭环 B —— 离线回归（数据集 + CI 门禁）

```go
ds := eval.Dataset{
    {Name: "退货", Input: "我想退货", Reference: "在订单页申请退款，7 天内原路退回"},
    {Name: "改地址", Input: "怎么改收货地址", Reference: "未发货前可在订单详情修改地址"},
}

rep, _ := (&eval.Harness{
    Agent:   a,
    Scorers: []eval.Scorer{eval.Reference(judge), eval.SemanticSimilarity(embedder)},
}).Run(ctx, ds)

rep.Print()                    // 终端表格：每例分数 + 聚合均值/通过率
if rep.PassRate < 0.9 {        // 质量低于阈值则 fail CI
    os.Exit(1)
}
```

> 同种评分器用多次时（如两个 `Contains`），用 `eval.Named("退款", eval.Contains("退款"))` 给每个
> 起唯一名字，否则它们在 `Report` 里会按相同的 `Name` 合并成一列。

**版本回归对比**：用 `Pairwise(judge)` 比"新版 vs 旧版"在同一数据集上的答案，作为合并门禁。

**评估集会成长**：把线上/测试中发现的坏 case 回填进 `Dataset`，评估覆盖面越跑越强。

---

## 5. 示例索引

| 示例 | 演示 | 状态 |
|---|---|---|
| `examples/eval-quickstart` | 规则 + 裁判给单个答案打分 | ✅ 已实现 |
| `examples/eval-judge` | `Rubric` 量规 + `Pairwise` A/B 对比 | ✅ 已实现 |
| `examples/eval-tool` | 单次工具结果评估（纯规则，无需 LLM） | ✅ 已实现 |
| `examples/eval-trajectory` | 跑 agent → `Record` → 评轨迹 | ✅ 已实现 |
| `examples/eval-dataset` | `Harness` 批量 + `Report`（CI 门禁） | ✅ 已实现 |
| `examples/eval-reflect` | 在线闭环自纠（粗答→裁判打回→重答变好） | ✅ 已实现 |

```bash
go run ./examples/eval-quickstart            # 离线 mock
EVAL_LIVE=1 AGNES_API_KEY=sk-... go run ./examples/eval-reflect   # 真模型
```
