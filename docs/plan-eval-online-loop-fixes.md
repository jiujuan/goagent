# 实施方案：在线评估闭环的三处第 0 档缺陷修复

- 范围：`eval/loop.go`、`eval/composite.go`、`eval/eval.go` 的缺陷 ③④⑤（评审发现报告中的第 0 档 3/4/5 条）
- 日期：2026-09-29
- 前置事实（已 grep 核实）：
  - `Gate` 只构造 `Sample{Input, Output}`（`eval/loop.go:48`），而它自己的注释推荐可用 `Faithfulness`（`eval/loop.go:29-30`）；`Faithfulness` 无 grounding 时返回 error（`eval/judge.go:77-80`），`TrajectoryJudge` 要求 `Sample.Traj`（`eval/judge.go:92-94`）。
  - `Gate.AfterModel` / `ToolGuard.AfterTool` 把 scorer 的 error 原样上抛（`eval/loop.go:50`、`eval/loop.go:91`），钩子 error 会经 `l.fail`（`agent/loop.go:184`）终止整次 run，已生成的答案被丢弃。
  - `All`/`Any`/`Weighted` 把 `Score.Reason` 覆盖成 `summarize()` 的记账串（`eval/composite.go:70,92,112`），即 `"rubric=0.80✓ contains=0.00✗"`；`Not` 只翻 `Value`/`Passed`，`Reason` 仍是原判定文本（`eval/composite.go:21-24`）。`Gate` 回喂的批评正是 `sc.Reason`（`eval/loop.go:61-62`）。
  - `LoopContext` 内嵌 `*RunContext`，含 `Step`/`History`（`agent/loopctx.go:14-19`）→ Gate 能拿到完整对话，无需改 agent 层。
  - `AfterTool` 只收到 `*core.ToolResult`（`agent/middleware.go:27`）→ 拿不到 `ToolCall`，除非改 agent 签名。
  - `tool.New` 的 string 返回值原样成为 `core.Text` part（`tool/function.go:61`），无 JSON 转义噪声。
  - `log.Logger` 模式已有先例：`memory/memx/consolidation.go:41-43,58-61`。
  - 外部使用点只有 `examples/eval-reflect/main.go:61`（`eval.Gate`）与 `eval/loop_test.go`；`core/event.go` 正被 loop-guard 分支改动。

## 总设计立场（三个 TASK 共用）

1. **零 core 改动、零新依赖**。诊断信息不进事件流（`core.Event` 是密封联合，加变体要动 `core/event.go` + `eventjson.go`，与进行中的 ADR-0023 冲突），只走 `*log.Logger` + 错误包装。
2. **`Scorer` 保持黑盒**。不为 scorer 增加"我需要哪些字段"的接口能力（那会污染 `eval.go:55-58` 的唯一契约，且第三方 `scorerFunc` 无法声明）；字段可用性由 Gate/ToolGuard 这两个唯一知道上下文的消费者来陈述。
3. **`Score` 只加一个字段**，语义是"给模型看的批评"，与"给人看的记账"分离；老 scorer 无需改动（空串即回退到 `Reason`）。
4. 两个构造函数改为**变参可选参数**，既有调用点零改动即兼容。

---

## TASK-EVAL-01：让运行时 scorer 拿到可评的 Sample

### 改动

**新增 `eval/samplectx.go`**（纯函数，便于单测）：

- `buildTrajectory(history []core.Message, steps int) *Trajectory`
  - 从 assistant 消息收集 `core.ToolCall`（按 `ID` 索引），遇到 `core.ToolResult` 时配对成 `ToolEpisode{Call, Result}`（`Latency` 留零值并在注释说明：AfterTool 阶段无法可靠测量，属缺陷 ⑩ 的范围）；同时把消息原样收进 `Traj.Messages`。
  - `Input` 复用现有 `firstUserText`（`eval/loop.go:103-112`）；`Final`/`Usage`/`Steps` 由调用方补，Gate 侧传 `lc.Step+1` 作为步数下界。
  - 无工具时返回 `&Trajectory{Input, Messages, Steps}`（非 nil），这样 `MaxSteps` 之类轨迹型 scorer 也能用。
- `sampleFromHistory(lc *agent.LoopContext, answer string) Sample`
  - 填 `Input`/`Output`/`Traj` 三项 —— 即 Gate 的样本从"2 个字段"变成"3 个字段"。
- `episodeFromResult(tr *core.ToolResult) *ToolEpisode`
  - `Call` 留零值 + 注释说明是 agent 层签名限制（见"不做"）。

**`eval/loop.go`**：

- `gate.AfterModel` 改用 `sampleFromHistory(lc, answer)`（替换 `eval/loop.go:48`）。
- `toolGuard.AfterTool` 的 `Sample` 补 `Tool: episodeFromResult(tr)` 与 `Input: firstUserText(lc.History)`（现在 `Input` 是空的，`eval/loop.go:86-89`）。
- 重写文件头 `eval/loop.go:11-25` 的示例注释与 `gate` 的文档注释：**删掉**"scorer 应当是 reference-free"这句已被推翻的措辞，改为字段清单：
  | 挂载点 | 填充字段 | 可用于 |
  | --- | --- | --- |
  | `Gate` | `Input` `Output` `Traj` | `Rubric` `Faithfulness` `TrajectoryJudge` `MaxSteps` `TokenBudget` |
  | `ToolGuard` | `Input` `Output`(结果文本) `Tool` | `JSONSchema` `Regex` `Contains` `NoToolError` |
  并明确 `Reference` 在运行期不存在（gold 答案只属于离线 Harness）。

### 验收

- `go test ./eval/`：
  - `TestGateSampleCarriesTrajectory`：自定义 `newScorer` 断言 `s.Traj != nil && len(s.Traj.Tools) == 1 && s.Traj.Tools[0].Call.Name == "lookup" && s.Input != ""`。
  - `TestGateFaithfulnessEndToEnd`：worker 先 `mock.CallTool` 再 `mock.Text`，工具返回含标记串（如 `巴黎在 rainy season`）；judge 用 `scriptedJudge`（`eval/judge_test.go:15`）变体，判据关键词命中"依据材料"段。断言首次因批评改进、最终 Escalate 成功。
- **反向验证**：临时把 `buildTrajectory` 降级为只返回 nil，`TestGateFaithfulnessEndToEnd` 必须失败于 `Faithfulness needs grounding` —— 证明断言不是空转。
- 全仓 `go vet ./... && go test ./...` 跑绿。

---

## TASK-EVAL-02：评估器故障不再摧毁答案（Failure 策略）

### 改动

**`eval/loop.go`** 新增两组选项（结构与措辞对齐 `ConsolidationConfig` 的"注入 logger、失败只记账"立场）：

```go
// ScoreFailure 决定 scorer 返回 error 时（judge 不可解析、模型不可用、
// 样本字段不满足）在线闭环怎么走。
type ScoreFailure int

const (
    ScoreFailureStop   ScoreFailure = iota // 既有行为：上抛，run 失败
    ScoreFailurePass                        // 视为已达标：接受当前答案并记一条日志
    ScoreFailureReject                      // 视为未达标：把失败原因当批评 Steer 回去
)

type GateOptions struct {
    Failure ScoreFailure
    Log     *log.Logger // nil 用 log.Default()；仅 Pass/Reject 两种策略会写
}
type GateOpt func(*GateOptions)
func WithScoreFailure(ScoreFailure) GateOpt
func WithScoreLog(*log.Logger) GateOpt

func Gate(scorer Scorer, threshold float64, opts ...GateOpt) agent.Middleware
func ToolGuard(check Scorer, opts ...GateOpt) agent.Middleware   // 复用同一组选项
```

策略语义（两种挂载点各一份落点）：

| 策略 | `Gate` | `ToolGuard` |
| --- | --- | --- |
| `Stop` | 原样上抛（默认，向后兼容） | 原样上抛 |
| `Pass` | 返回 `Escalate{Reason: "eval gate unavailable: …"}`，答案即最终稿 | 不改写 `tr`，工具结果原样交给模型 |
| `Reject` | `Steer` 一条 `评审器未能给出评分（原因），请保留答案要点并重述`，返回空 directive（继续） | 翻 `IsError` 并追加 `[评估器不可用] …` |

`Reject` 不伪造数值分（不写 `Value: 0.2`），文案与"评分未达标"分开，避免与 `Gate` 的阈值语义纠缠。

**诊断包装**（解决"字段不满足"这一类配置错误的可读性）：

- 新增 `sampleNeeds(name string) string`，按 scorer 名返回其最低字段要求（`reference`→`Reference`；`faithfulness`→`Traj.Tools` 或 `Reference`；`trajectory_judge`/`max_steps`/`token_budget`→`Traj`；未知名→`""`）。
- 上抛/包装错误统一为 `fmt.Errorf("eval: %s scorer %q failed: %w (scorer 需要 %s；本挂载点提供 Input/Output/Traj…)", where, scorer.Name(), err, need)`，把已有 `Sample` 实际填充的字段名列出。字段不满足从"隐形误配"变成"一眼可诊断"。

### 验收

- `TestGateScorerErrorPropagatesByDefault`：scorer 返回 error ⇒ `run` 返回该 error（锁住默认行为）。
- `TestGateScorerFailurePass`：`WithScoreFailure(ScoreFailurePass)` ⇒ 拿到第一版答案且 `Wait` 无 error；注入的 `*log.Logger`（写到 `bytes.Buffer`）含 `eval gate unavailable`。
- `TestGateScorerFailureReject`：`Reject` + 只失败一次（`scoreFailureOnce` 辅助 scorer）⇒ worker 第二轮收到含"评审器"的 steering 消息，循环正常收敛。
- `TestToolGuardScorerFailureVariants`：三策略下分别断言 `IsError` 的取值与 `Log` 输出。
- **反向验证**：把 `Pass` 分支误写成上抛 ⇒ `TestGateScorerFailurePass` 必失败。

---

## TASK-EVAL-03：`Critique`（给模型）与 `Reason`（给人）分离

### 改动

**`eval/eval.go`**：`Score` 增一个字段（JSON tag 与既有风格一致，`omitempty`）：

```go
// Critique 是"下一步该怎么改"的文本，供在线闭环回喂给模型；
// Reason 保持"本次判定依据什么"的记账语义（规则 scorer 的观测值、
// 复合 scorer 的子项摘要）。两者可以不同。
Critique string `json:"critique,omitempty"`
```

**`eval/composite.go`**：

- 新增 `critiqueFailed(subs []Score) string`：按子项**原始顺序**（在 `sortScores` 之前捕获）拼接未通过子项的 `orCritique(sub)`，用 `"; "` 分隔；全部通过则取所有子项（或空串）。
- `All`/`Any`/`Weighted`：`Reason` 继续是 `summarize(subs)`（**报告列不回归**），额外填 `Critique: critiqueFailed(subs)`。
- `Not`：`Value` 取反会毁掉子项摘要，改为
  `Reason: "not(" + sc.Name + "): " + sc.Reason`，并对 `Critique` 加 `"不应触发「」："` 前缀；`Name` 仍加 `not_` 前缀（`composite.go:21`）。
- 新增 `orCritique(s Score) string { if s.Critique != "" { return s.Critique }; return s.Reason }` —— 单 scorer（含规则、裁判）不强制填 `Critique`，`Reason` 本身就是合格的批评（如 `boolScore` 的 `"got %q, want %q"`，`eval/eval.go:115-121`）。

**`eval/loop.go`**：Gate 与 ToolGuard 的反馈文本改用 `orCritique(sc)`（`eval/loop.go:61-62`、`eval/loop.go:96-98`）。

### 验收

- `TestWeightedCritiqueNamesFailingItem`：`Weighted(Weight(Named("contains", Contains("完整")), 1), Weight(Named("ok", ExactMatch("x")), 1))` ⇒ `Reason` 仍是 `contains=0.00✗ ok=1.00✓`（列不回归），`Critique` 含 `substring "完整" present=false` 且**不含** `✓`。
- `TestGateSteersCritiqueNotSummary`：worker 记录收到的历史；用 `Gate(All(Named("a", …失败项…), Named("b", …通过项…)), 0.5)`，断言 steering 消息含失败项的具体判定文本、不含 `all=0.` 摘要串。
- `TestNotRewritesReason`：`Not(Contains("x"))` ⇒ `Reason` 以 `not(contains):` 开头，`Critique` 带"不应触发"前缀。
- `Score` 的 JSON 往返：既有 `Score` 的 marshal 测试（若有）仍通过；新字段 `omitempty` 不改变旧输出字节。
- **反向验证**：把 Gate 的 `orCritique(sc)` 改回 `sc.Reason` ⇒ `TestGateSteersCritiqueNotSummary` 失败。

---

## 执行顺序与提交切分

1. TASK-EVAL-03 → 2. TASK-EVAL-01 → 3. TASK-EVAL-02。
   顺序理由：03 只动数据结构与文案（一个 commit，风险最低）；01 依赖 03 的 `orCritique` 才不至于在 Gate 里二次改同一行；02 的 `Reject` 文案又要用 01 的样本信息。
   每个 commit 单独 `go build ./... && go vet ./... && go test ./...` 校验后再下一个（按仓库既有的 hunk 级拆分惯例）。
2. 收尾一并更新 `examples/eval-reflect/main.go`：加一段可选的 `Faithfulness` 演示（`WithTool` 的 worker + `Gate(eval.Faithfulness(judge), 0.7, eval.WithScoreFailure(eval.ScoreFailureReject))`），让"缺陷 ③ 修好了"有一个能跑的例子；并在 `docs/adr/` 落 `ADR-0026-eval-system.md`（`eval/eval.go:3` 已引用但文件不存在，属缺陷 ②，本方案只做补齐引用，不重写设计）。

## 明确不做（留给后续档）

- **`ToolGuard` 填 `ToolEpisode.Call`**：需要把 `*core.ToolCall` 传进 `AfterTool`（`agent/middleware.go:27` 的签名变更），会波及所有实现 `AfterTool` 的中间件（`middleware/permission.go`、`skills` 的 Gate、`workspace`）。收益只是让"参数质量 judge"可用，属第 2 档能力，应作为独立提案。
- **分数持久化 / 事件发布**（缺陷 ⑥）：`State.KV` 或新事件需要先定形态，且 `core/event.go` 正被 ADR-0023 改动，现在动必然冲突。
- **`Gate` 的 `maxTurns` 预算冲突与 best-of-N**（缺陷 ⑦⑧）：需要新的循环形态，属第 1 档。
- **`Record` 的 `Latency` 观察者侧计时**（缺陷 ⑩）：本方案同样不修 `ToolEpisode.Latency`，两处一起改更合适。

## 风险

| 风险 | 影响 | 处置 |
| --- | --- | --- |
| `Traj` 进入 Gate 样本后，自定义 scorer 的行为改变（之前看到 `s.Traj == nil` 的分支不再走） | 低：仓库内仅 `examples/eval-reflect` 与测试 | 属"修复"而非回归；在 CHANGELOG 段落记一句 |
| `Not` 改写 `Reason` 破坏依赖旧串的报告列 | 低（已核实）：`Not` 仅一个使用点 `examples/eval-dataset/main.go:15`（作为 `Named("无套话", …)` 的一列），且 `eval/` 现有测试**没有任何** `Not` 用例 —— 即改动无断言保护，也无人依赖旧串 | 必须在 TASK-EVAL-03 里补 `TestNotRewritesReason` 把新语义钉住（现状是零覆盖，比破坏更值得担心的反而是无声漂移） |
| 并行会话正在改 `core/`、`middleware/`（loop-guard） | 低：本方案只碰 `eval/`、`examples/eval-reflect`、`docs/adr/` | 不动 `core/event.go`、`agent/middleware.go`；提交前 `git status` 复核暂存范围 |
