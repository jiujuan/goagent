# P0-4 上下文溢出重试实施计划

> 执行方式：按任务卡顺序实施，每张卡先写失败测试，再写最小实现，验证通过后独立提交。除非用户明确授权，不启动并行子代理。本文只覆盖 P0-4。

**目标：**按照 [上下文溢出重试设计稿](../analysis/context-overflow-retry-design.md) §5–§10，把「提供方返回上下文超限后整次运行失败」改成「在同一步骤内压缩工作历史、先落盘、再重试一次」。提供方的 400/413 超限响应升级为可识别的类型化错误；`Compaction` 和 `Window` 通过新的中间件能力各提供一种恢复方式；默认每个步骤最多恢复一次，`0` 关闭。

**架构：**在 `llm` 包新增 `ContextLengthError` 与 `NewStatusError` 分类器，OpenAI 兼容层和 Anthropic 提供方改用分类器；在 `agent` 包新增 `ContextOverflowRecoverer` 能力接口、`Stack.RecoverContext` 折叠、`WithContextOverflowRetry` 选项和 `runStep` 内的恢复状态机；在 `middleware` 包让 `Compaction` 做强制摘要、`Window` 做紧急尾部淘汰；在 `core` 包新增 `ContextOverflowRetried` 事件。重试留在原步骤的 `CallModel` 阶段，请求从冻结视图重建，恢复后的历史先写一个 `Release:false` 的中途检查点，提交成功后才发布事件并发起第二次模型调用。

**技术栈：**Go 1.25（本机 go1.26.4）、标准库、现有 mock LLM、checkpoint Memory、bus。不新增第三方依赖。

- 状态：待实施。
- 日期：2026-10-22。
- 基线：`2f18017`。实施前执行 `git status --short`、`git log -1 --oneline`、`go test ./... -count=1` 确认基线。
- 设计依据：[context-overflow-retry-design.md](../analysis/context-overflow-retry-design.md) §5–§10。
- 工作目录：`D:/codeproject/mygo/goagent`；下文代码路径均相对此根目录。
- 范围：P0-4 上下文溢出重试。P0-1 的冻结步骤视图、P0-2 的步骤/任务拆分、P0-3 的工具分段并发已经体现在基线中，不在本计划内。

当前工作区有未跟踪的分析文档、`.scratch/` 目录和几份未入库的计划文档（包括本文和设计稿）。执行时保留它们，不要清理，不要使用 `git add .` 把无关文件混入提交。任务卡里的提交只包含该卡列出的代码和测试；本文与设计稿的入库由用户另行安排。

## 1. 现有能力与行为变化

### 1.1 现在一次模型调用怎么走

1. `runStep` 在步骤开始时创建 `LoopContext`，发布 `TurnStarted`，先 drain steering 和持久化收件箱，再调用 `CompactHistory` 让持久模式的压缩改写工作历史（[agent/loop.go:247](../../agent/loop.go#L247)、[agent/loop.go:253](../../agent/loop.go#L253)、[agent/loop.go:281](../../agent/loop.go#L281)、[agent/loop.go:282](../../agent/loop.go#L282)）。
2. 请求由系统提示、当前历史、工具 schema 组装，`lc.Request` 指向它；`ModifyRequest` 只跑一次，随后 `freezeStepView` 冻结视图、`checkReplayDrift` 检查恢复基线（[agent/loop.go:303](../../agent/loop.go#L303)、[agent/loop.go:305](../../agent/loop.go#L305)、[agent/loop.go:306](../../agent/loop.go#L306)、[agent/loop.go:309](../../agent/loop.go#L309)、[agent/loop.go:314](../../agent/loop.go#L314)）。
3. `ModelContext` 为这一次提供方调用派生上下文，`streamModel` 调用模型并发布增量/最终消息（[agent/loop.go:322](../../agent/loop.go#L322)、[agent/loop.go:324](../../agent/loop.go#L324)、[agent/loop.go:479](../../agent/loop.go#L479)）。
4. 模型返回错误时，`streamModel` 调用一次 `OnError`，然后返回 `ok=false`；`runStep` 直接把错误交给 `fail`，没有重试入口（[agent/loop.go:485](../../agent/loop.go#L485) 到 [agent/loop.go:487](../../agent/loop.go#L487)、[agent/loop.go:325](../../agent/loop.go#L325) 到 [agent/loop.go:327](../../agent/loop.go#L327)）。
5. `fail` 把当前历史裁到可重放边界，写一个 `Release:true` 的终态检查点，然后结束运行（[agent/loop.go:567](../../agent/loop.go#L567)、[agent/loop.go:572](../../agent/loop.go#L572)、[agent/loop.go:573](../../agent/loop.go#L573)）。
6. 两个聊天提供方对非 200 响应直接构造 `*llm.StatusError`，400 被 `Retryable()` 判为不可重试，但错误里没有「超限」类型，也没有提供方给出的上限/请求量（[llm/openaicompat/openaicompat.go:89](../../llm/openaicompat/openaicompat.go#L89)、[llm/anthropic/anthropic.go:89](../../llm/anthropic/anthropic.go#L89)、[llm/error.go:28](../../llm/error.go#L28)）。
7. `RetryModel` 只在「一个响应都没产出」且还有次数时重试；它不区分错误类型，一个被网关编码成 429/5xx 的超限会被原样重发（[middleware/retry.go:46](../../middleware/retry.go#L46)、[middleware/retry.go:64](../../middleware/retry.go#L64)）。
8. `Compaction` 和 `Window` 只在步骤开始的 `CompactHistory` 或请求改写阶段按自己的阈值/预算工作；阈值来自估算，估算偏离实际时它们不会因为提供方明确报超限而补一次更激进的压缩（[middleware/compaction.go:105](../../middleware/compaction.go#L105)、[middleware/window.go:224](../../middleware/window.go#L224)）。
9. 检查点只在步骤结束或失败时写；一次模型调用失败不会把「准备重试的那份历史」持久化（[agent/loop.go:514](../../agent/loop.go#L514)）。

### 1.2 本计划要解决的问题

- **超限错误不可识别。** 提供方已经说明「上下文太长」，但调用方只能看到一个普通的 400，无法做针对性处理。
- **同一步没有恢复路径。** 失败后只能结束运行，下一次 `Resume` 会从同一个过大的请求重新开始，还是同样的结果。
- **阈值估算会失手。** 正常压缩按估算触发，估算偏低或系统提示/工具 schema 突然变大时，请求已经超限而压缩没有触发。
- **恢复结果没有落盘。** 即使中间件在请求上临时缩短了消息，检查点里仍是旧历史，崩溃后恢复还是会重发超限请求。
- **重试没有边界。** 不限制次数会形成压缩/重试循环；不区分「已经输出过」会重复输出。

### 1.3 可观察行为变化

| 场景 | 现在 | 本计划之后 | 原因 |
| --- | --- | --- | --- |
| 400/413 超限，尚未输出任何消息，有恢复者，默认配置 | 运行失败，错误是一个普通 `StatusError` | 同一步骤压缩历史、写中途检查点、发布 `ContextOverflowRetried`，再调用一次模型 | 新增恢复状态机 |
| 已经发布过 `MessageDelta` 或 `MessageDone` 后超限 | 运行失败 | 仍然失败，不重试 | 不能撤回已经输出的内容 |
| 没有恢复者，或恢复者返回的历史没有严格变短 | 运行失败 | 仍然失败，错误是 `*ContextLengthError` | 恢复是能力，不是保证 |
| `WithContextOverflowRetry(0)` | 没有该选项 | 运行失败，错误仍是 `*ContextLengthError` | 0 表示关闭恢复 |
| 恢复后第二次调用仍超限 | 不可能发生 | 返回第二次的超限错误；默认配置下不再有第三次调用 | 每次失败都是一次独立判定 |
| 恢复后第二次调用返回别的错误 | 不可能发生 | 返回第二次的错误；历史保持恢复后的版本 | 真正失败的是新请求 |
| 400 响应带超限标记但没有数字 | 普通 `StatusError` | `*ContextLengthError`，`Limit`/`Requested` 为 0 | 知道是超限比知道数字更重要 |
| 400 响应不含超限标记 | 普通 `StatusError` | 仍然是普通 `StatusError` | 只按标记分类，不按状态码猜 |
| `RetryModel` 包住提供方 | 400 不重试；429/5xx 的无输出超限会重试同一请求 | 超限一律不重试，交给循环恢复 | `RetryModel` 只处理瞬时错误 |
| 请求模式的 `Compaction`/`Window` | 只改本次请求，不落盘 | 超限恢复会把结果写进中途检查点（一次性覆盖 `Persist:false`） | 恢复必须可崩溃恢复 |
| 运行过程中读取 `store.Latest` 的观察者 | 每个步骤最多一个快照 | 恢复发生的步骤会多一个同步骤号、无 `Pending` 的快照 | 压缩历史先落盘的代价 |
| 事件订阅方 | 没有该事件 | 恢复时多一条恢复者事件和一条 `ContextOverflowRetried` | 新增可观察性 |

`OnError` 仍然在每次失败的模型调用上执行一次。一次成功恢复的运行，`OnError` 会看到第一次超限；如果第二次也失败，还会看到第二次错误。这是既有钩子的语义，不在本计划改变。

### 1.4 边界

本计划只处理「提供方明确报告 400/413 上下文超限，且本次调用还没有输出」的同步骤恢复。不做跨提供方切换、不做无限重试、不改正常压缩阈值、不改检查点结构、不改 `Resume` 语义、不处理已经输出一半的回复。完整清单见 §9。

## 2. 已核实的事实

下表是实施前核对过的代码位置。执行时如果行号已经漂移，按函数名和代码内容定位，不要按行号硬改。

### 2.1 llm 包

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `StatusError` 的字段、错误串、`Retryable` | [llm/error.go:14](../../llm/error.go#L14)、[llm/error.go:20](../../llm/error.go#L20)、[llm/error.go:28](../../llm/error.go#L28) | 新类型嵌入它，错误串保持不变 |
| `IsRetryable` 先查取消，再用 `errors.As` 找 `*StatusError` | [llm/error.go:37](../../llm/error.go#L37) 到 [llm/error.go:49](../../llm/error.go#L49) | 新类型必须在 `errors.As` 之前拦截 |
| OpenAI 兼容层的非 200 分支 | [llm/openaicompat/openaicompat.go:89](../../llm/openaicompat/openaicompat.go#L89) | 换成 `llm.NewStatusError` |
| Anthropic 的非 200 分支 | [llm/anthropic/anthropic.go:89](../../llm/anthropic/anthropic.go#L89) | 换成 `llm.NewStatusError` |
| 两个提供方的测试目前只覆盖流解析 | [llm/openaicompat/openaicompat_test.go:26](../../llm/openaicompat/openaicompat_test.go#L26)、[llm/anthropic/anthropic_test.go:46](../../llm/anthropic/anthropic_test.go#L46) | 需要补 HTTP 错误分类测试 |
| 现有 `StatusError`/`IsRetryable` 测试 | [llm/error_test.go:12](../../llm/error_test.go#L12)、[llm/error_test.go:37](../../llm/error_test.go#L37) | 追加新用例 |
| `RetryModel` 的放弃条件 | [middleware/retry.go:64](../../middleware/retry.go#L64) | 追加超限判断 |

### 2.2 core 包

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `HistoryCompacted` 定义和注释 | [core/event.go:122](../../core/event.go#L122) 到 [core/event.go:131](../../core/event.go#L131) | 注释需要覆盖恢复路径 |
| `WindowTrimmed` 定义和注释 | [core/event.go:133](../../core/event.go#L133) 到 [core/event.go:147](../../core/event.go#L147) | `Strategy` 需要增加 `"overflow_tail"` |
| marker 区 | [core/event.go:180](../../core/event.go#L180) 到 [core/event.go:202](../../core/event.go#L202) | 新增事件的 marker |
| `eventWire` 字段 | [core/eventjson.go:18](../../core/eventjson.go#L18) 到 [core/eventjson.go:50](../../core/eventjson.go#L50) | 新增七个线字段 |
| 事件编码分支 | [core/eventjson.go:104](../../core/eventjson.go#L104) 到 [core/eventjson.go:113](../../core/eventjson.go#L113) | 新增 marshal 分支 |
| 事件解码分支 | [core/eventjson.go:168](../../core/eventjson.go#L168) 到 [core/eventjson.go:182](../../core/eventjson.go#L182) | 新增 decode 分支 |
| 未知事件类型错误 | [core/eventjson.go:184](../../core/eventjson.go#L184) | 跨版本升级边界 |
| 较新事件的往返测试在 `stuckevent_test.go` | [core/stuckevent_test.go:37](../../core/stuckevent_test.go#L37)、[core/stuckevent_test.go:69](../../core/stuckevent_test.go#L69)、[core/stuckevent_test.go:85](../../core/stuckevent_test.go#L85) | 新测试放这里 |
| 旧往返表只覆盖更早的事件 | [core/eventjson_test.go:8](../../core/eventjson_test.go#L8) | 不要只改这张表 |

### 2.3 agent 包

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `runStep` 和各阶段边界 | [agent/loop.go:247](../../agent/loop.go#L247) | 恢复状态机的位置 |
| `LoopContext` 创建 | [agent/loop.go:253](../../agent/loop.go#L253) | `Attempts` 不是字段，计数放在循环局部 |
| 请求构造和 `lc.Request` | [agent/loop.go:303](../../agent/loop.go#L303) 到 [agent/loop.go:305](../../agent/loop.go#L305) | 重建请求时替换它 |
| `ModifyRequest` 只跑一次 | [agent/loop.go:306](../../agent/loop.go#L306) | 重试不重跑 |
| 冻结视图、漂移检查、`ModelContext` | [agent/loop.go:309](../../agent/loop.go#L309)、[agent/loop.go:314](../../agent/loop.go#L314)、[agent/loop.go:322](../../agent/loop.go#L322) | 重试时只重跑 `ModelContext` |
| 第一次模型调用 | [agent/loop.go:324](../../agent/loop.go#L324) | 改造成循环 |
| `AfterModel` 在模型成功之后 | [agent/loop.go:330](../../agent/loop.go#L330) | 重试成功后只调用一次 |
| `streamModel` 的错误/增量/最终分支 | [agent/loop.go:479](../../agent/loop.go#L479)、[agent/loop.go:485](../../agent/loop.go#L485) 到 [agent/loop.go:494](../../agent/loop.go#L494) | 增加 `emitted` |
| `commitOptions` 和 `checkpoint` | [agent/loop.go:502](../../agent/loop.go#L502)、[agent/loop.go:514](../../agent/loop.go#L514) | 中途检查点复用 |
| `fail` 写终态检查点 | [agent/loop.go:567](../../agent/loop.go#L567)、[agent/loop.go:572](../../agent/loop.go#L572) | 失败语义不变 |
| `AgentLoop` 字段和 `newLoop` | [agent/loop.go:31](../../agent/loop.go#L31)、[agent/loop.go:50](../../agent/loop.go#L50) | 新增重试预算字段 |
| `config` 和 `New` 的默认值 | [agent/options.go:16](../../agent/options.go#L16)、[agent/agent.go:37](../../agent/agent.go#L37)、[agent/agent.go:38](../../agent/agent.go#L38) | 默认 1 只能在这里种下 |
| `WithMaxTurns`/`WithCheckpointer` 的位置 | [agent/options.go:93](../../agent/options.go#L93)、[agent/options.go:148](../../agent/options.go#L148) | 新选项放在同一区域 |
| `HistoryCompacter`/`Stack`/`CompactHistory`/`OnError` | [agent/middleware.go:158](../../agent/middleware.go#L158)、[agent/middleware.go:185](../../agent/middleware.go#L185)、[agent/middleware.go:272](../../agent/middleware.go#L272)、[agent/middleware.go:332](../../agent/middleware.go#L332) | 新能力的写法参照 |
| `BaseMiddleware` 没有 `RecoverContext` | [agent/middleware.go:164](../../agent/middleware.go#L164) 到 [agent/middleware.go:181](../../agent/middleware.go#L181) | 嵌入不会让所有中间件变成恢复者 |
| `AddTool` 的函数起点和冻结守卫 | [agent/loopctx.go:64](../../agent/loopctx.go#L64)、[agent/loopctx.go:66](../../agent/loopctx.go#L66) | 设计稿引用 64，实际守卫在 66 |
| `StepView` 的 `Model`/`System`/`Options`/`Messages`/`Tools` | [agent/stepview.go:114](../../agent/stepview.go#L114)、[agent/stepview.go:122](../../agent/stepview.go#L122)、[agent/stepview.go:131](../../agent/stepview.go#L131)、[agent/stepview.go:140](../../agent/stepview.go#L140)、[agent/stepview.go:148](../../agent/stepview.go#L148) | 重建请求的四个来源 |
| `ToolView.Schemas` 返回副本 | [agent/stepview.go:201](../../agent/stepview.go#L201) | 重试请求不会共享可变切片 |
| `snapshotTools` 重新读运行期工具表 | [agent/stepview.go:271](../../agent/stepview.go#L271) | 冻结失败用例的触发点 |
| `freezeStepView` 校验 schema 一致 | [agent/stepview.go:388](../../agent/stepview.go#L388)、[agent/stepview.go:407](../../agent/stepview.go#L407) 到 [agent/stepview.go:409](../../agent/stepview.go#L409) | 恢复后重新冻结 |
| `checkReplayDrift` 只用于恢复后的第一次冻结 | [agent/stepview.go:437](../../agent/stepview.go#L437) | 重试不再调用 |
| `prepare` 恢复后追加输入 | [agent/run_prepare.go:23](../../agent/run_prepare.go#L23)、[agent/run_prepare.go:72](../../agent/run_prepare.go#L72) 到 [agent/run_prepare.go:73](../../agent/run_prepare.go#L73) | 集成测试需要预置历史 |
| 提交路径和 `ReleaseClaim` | [agent/durable_runtime.go:339](../../agent/durable_runtime.go#L339)、[agent/durable_runtime.go:355](../../agent/durable_runtime.go#L355)、[agent/durable_runtime.go:396](../../agent/durable_runtime.go#L396) 到 [agent/durable_runtime.go:403](../../agent/durable_runtime.go#L403) | 中途检查点 `Release:false` |

### 2.4 middleware 包

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `CompactionOptions` 和工厂 | [middleware/compaction.go:20](../../middleware/compaction.go#L20)、[middleware/compaction.go:56](../../middleware/compaction.go#L56) | 恢复路径读取同一份配置 |
| `compaction` 结构 | [middleware/compaction.go:76](../../middleware/compaction.go#L76) | 新增方法挂在这里 |
| 摘要标记 | [middleware/compaction.go:87](../../middleware/compaction.go#L87) | 恢复结果复用 |
| `CompactHistory` 的内联发布 | [middleware/compaction.go:105](../../middleware/compaction.go#L105)、[middleware/compaction.go:114](../../middleware/compaction.go#L114) 到 [middleware/compaction.go:116](../../middleware/compaction.go#L116) | 抽成 `publishCompacted` |
| `compact` 的阈值和保留条数 | [middleware/compaction.go:123](../../middleware/compaction.go#L123) | 恢复路径不调用它 |
| `safeCut` | [middleware/compaction.go:146](../../middleware/compaction.go#L146) | 恢复裁剪点复用 |
| `calibratedEstimate` 和 `summarize` | [middleware/compaction.go:175](../../middleware/compaction.go#L175)、[middleware/compaction.go:179](../../middleware/compaction.go#L179) | 恢复的目标和摘要复用 |
| `groupUnits`/`keptMessages`/`repairPairing` | [middleware/window.go:43](../../middleware/window.go#L43)、[middleware/window.go:94](../../middleware/window.go#L94)、[middleware/window.go:117](../../middleware/window.go#L117) | 恢复的整单元淘汰复用 |
| `Window` 工厂和结构 | [middleware/window.go:190](../../middleware/window.go#L190)、[middleware/window.go:198](../../middleware/window.go#L198) | 新增方法和 `estimate` |
| `apply` 的计数闭包 | [middleware/window.go:242](../../middleware/window.go#L242) | 改为委托 `estimate` |
| `publishTrimmed` 写死策略名 | [middleware/window.go:256](../../middleware/window.go#L256) | 抽成 `publishTrimmedAs` |
| `RecentN`/`SlidingWindow`/`Select` | [middleware/window.go:324](../../middleware/window.go#L324)、[middleware/window.go:358](../../middleware/window.go#L358)、[middleware/window.go:369](../../middleware/window.go#L369) | 最新单元无条件保留的规则 |
| `Select` 的 `start <= 0` 返回输入 | [middleware/window.go:386](../../middleware/window.go#L386) | 恢复也采用同样的「没得丢」判断 |
| RAG 追加系统提示 | [middleware/rag.go:45](../../middleware/rag.go#L45)、[middleware/rag.go:58](../../middleware/rag.go#L58) 到 [middleware/rag.go:61](../../middleware/rag.go#L61) | 重跑 `ModifyRequest` 会重复追加 |
| RunBudget 收尾改写 | [middleware/runbudget.go:176](../../middleware/runbudget.go#L176) 到 [middleware/runbudget.go:184](../../middleware/runbudget.go#L184) | 重跑会重复追加指令 |
| 收尾判定需要 `caps` | [middleware/runbudget.go:203](../../middleware/runbudget.go#L203) | 测试要设置任一上限 |
| `WarnMarkerBudget` | [middleware/runbudget.go:97](../../middleware/runbudget.go#L97) | 断言只出现一次 |

### 2.5 检查点与测试夹具

| 事实 | 位置 | 用途 |
| --- | --- | --- |
| `NewMemory`/`Save`/`Latest` | [checkpoint/memory.go:27](../../checkpoint/memory.go#L27)、[checkpoint/memory.go:46](../../checkpoint/memory.go#L46)、[checkpoint/memory.go:69](../../checkpoint/memory.go#L69) | 预置历史、读取最新状态 |
| `Memory.BeginRun` 拒绝 pending/pause 头部 | [checkpoint/coordinator_memory.go:11](../../checkpoint/coordinator_memory.go#L11)、[checkpoint/coordinator_memory.go:25](../../checkpoint/coordinator_memory.go#L25) 到 [checkpoint/coordinator_memory.go:28](../../checkpoint/coordinator_memory.go#L28) | 预置一个普通快照不会阻塞新运行 |
| `CommitRequest.ReleaseClaim` | [checkpoint/durable.go:95](../../checkpoint/durable.go#L95)、[checkpoint/durable.go:101](../../checkpoint/durable.go#L101) | 观察中途检查点是否释放租约 |
| `durableOnlyRecorder` 模式 | [agent/lease_config_test.go:302](../../agent/lease_config_test.go#L302)、[agent/lease_config_test.go:319](../../agent/lease_config_test.go#L319) | 只嵌入 `checkpoint.Durable`，没有 `BeginRun` |
| `scriptModel` 每次调用只产出一个响应或一个错误 | [agent/failure_test.go:23](../../agent/failure_test.go#L23)、[agent/failure_test.go:31](../../agent/failure_test.go#L31) 到 [agent/failure_test.go:43](../../agent/failure_test.go#L43) | 不能表达「先 partial 后 error」 |
| `scriptModel` 的请求/调用读取 | [agent/failure_test.go:46](../../agent/failure_test.go#L46)、[agent/failure_test.go:56](../../agent/failure_test.go#L56) | 需要增加 `requests()` |
| `phase1Drain` | [agent/phase1_interrupt_test.go:20](../../agent/phase1_interrupt_test.go#L20) | 复用事件收集 |
| `phase1FailOnSaveStore` 的非持久路径 | [agent/phase1_failure_test.go:21](../../agent/phase1_failure_test.go#L21)、[agent/phase1_failure_test.go:29](../../agent/phase1_failure_test.go#L29)、[agent/phase1_failure_test.go:52](../../agent/phase1_failure_test.go#L52) | 走 `Save`，可以精确计数 |
| mock 的 `Partial`/`NewStream` | [llm/mock/mock.go:68](../../llm/mock/mock.go#L68)、[llm/mock/mock.go:81](../../llm/mock/mock.go#L81) | 构造已输出场景 |
| 内部测试的 `newLC` | [middleware/compaction_internal_test.go:61](../../middleware/compaction_internal_test.go#L61) | 单元测试的 `LoopContext` |
| 窗口测试的消息夹具和计数器 | [middleware/window_internal_test.go:17](../../middleware/window_internal_test.go#L17)、[middleware/window_internal_test.go:27](../../middleware/window_internal_test.go#L27)、[middleware/window_internal_test.go:35](../../middleware/window_internal_test.go#L35)、[middleware/window_internal_test.go:248](../../middleware/window_internal_test.go#L248) | 构造工具调用/结果和确定性计数 |
| 现有窗口事件测试 | [middleware/window_test.go:363](../../middleware/window_test.go#L363) | 复用事件收集方式 |

### 2.1 对设计稿的按当前代码修正

以下各条在实施时以本计划为准；设计稿的结论不变，只是细节需要对齐当前代码。

1. **设计稿的循环片段没有把 `req` 换成 `retryReq`。** [设计稿 §5.7](../analysis/context-overflow-retry-design.md) 的片段只赋值了 `lc.Request = retryReq`。如果不同时执行 `req = retryReq`，第二次 `streamModel` 仍会发送第一次的请求，恢复等于没发生。CO-03 必须同时更新两个变量，并加一个 `AfterModel` 观察 `lc.Request` 的测试。
2. **新事件的往返测试不能只加在 `core/eventjson_test.go` 的旧表里。** 那张表只列了更早的事件（[core/eventjson_test.go:8](../../core/eventjson_test.go#L8)）；较新事件在 `core/stuckevent_test.go` 里有独立测试（[core/stuckevent_test.go:37](../../core/stuckevent_test.go#L37)、[core/stuckevent_test.go:69](../../core/stuckevent_test.go#L69)、[core/stuckevent_test.go:85](../../core/stuckevent_test.go#L85)）。新测试放 `stuckevent_test.go`，再补一个 `omitempty` 的原始 JSON 断言。
3. **`AddTool` 的冻结守卫在 66 行，不是 64 行。** 64 是函数起点，66 是 `if lc.view != nil || lc.Request == nil { return }`（[agent/loopctx.go:64](../../agent/loopctx.go#L64)、[agent/loopctx.go:66](../../agent/loopctx.go#L66)）。冻结失败用例依赖这个守卫：恢复者在视图冻结后调用 `AddTool`，只会改运行期工具表。
4. **`SlidingWindow` 的最新单元规则在 369–386 行。** 设计稿引用的 353 行是注释（[middleware/window.go:353](../../middleware/window.go#L353)）；实际无条件保留逻辑在 [middleware/window.go:372](../../middleware/window.go#L372) 到 [middleware/window.go:385](../../middleware/window.go#L385)，`start <= 0` 在 [middleware/window.go:386](../../middleware/window.go#L386)。
5. **内部测试直接构造 `newLoop(config{...})` 会绕过 `New` 的默认值。** `newLoop` 不种默认值（[agent/loop.go:50](../../agent/loop.go#L50)），而默认 1 种在 `agent.New`（[agent/agent.go:38](../../agent/agent.go#L38)）。现有内部测试见 [agent/stepview_internal_test.go:48](../../agent/stepview_internal_test.go#L48)、[agent/stepview_internal_test.go:83](../../agent/stepview_internal_test.go#L83)、[agent/task_loop_internal_test.go:30](../../agent/task_loop_internal_test.go#L30)。恢复的集成测试必须用公开 `agent.New`；如果确实要测 `newLoop`，显式设置字段。
6. **`scriptModel` 不能在同一个 `Generate` 里先发 partial 再返回错误。** 它每次调用只 `pick` 一次，要么 `yield` 一个响应，要么 `yield` 一个错误（[agent/failure_test.go:31](../../agent/failure_test.go#L31) 到 [agent/failure_test.go:43](../../agent/failure_test.go#L43)）。「已输出」用例需要在本卡新增一个局部模型，直接实现 `llm.Model`，不要改写现有 `scriptModel` 的行为。
7. **集成测试必须预置多消息历史。** 第一次运行的 `prepare` 只在恢复出的状态后面追加一条输入（[agent/run_prepare.go:72](../../agent/run_prepare.go#L72) 到 [agent/run_prepare.go:73](../../agent/run_prepare.go#L73)）；空线程的历史只有这条输入，任何恢复者都无法再缩短。测试先用 `store.Save` 写入一个包含若干条消息的普通快照，再用 `Stream(..., OnThread(tid))`。
8. **通用兼容标记不做数字猜测。** 设计稿 §5.2.1 的通用行写「能解析到数字就填」，但没有标准模式。本计划锁定：通用标记只负责分类，`Limit`/`Requested` 保持 0；只有 OpenAI 和 Anthropic 的明确模式填数字。这样不会把响应体里的其他数字误当成 token 数。
9. **恢复者事件早于中途检查点。** `Compaction` 的 `HistoryCompacted` 在 `RecoverContext` 内部发布（[设计稿 §5.4](../analysis/context-overflow-retry-design.md)）；此时历史只在内存里变短，还没有落盘。事件顺序固定为：恢复者事件 → 中途检查点提交 → `ContextOverflowRetried` → 第二次调用。
10. **`ContextLengthError` 必须通过 `NewStatusError` 构造，或显式设置嵌入的 `*StatusError`。** 如果测试直接写 `&llm.ContextLengthError{Limit: 1}`，访问 `cle.Provider` 会因空嵌入而 panic。`NewStatusError` 之外的手工构造只用于 `IsRetryable` 这类不读嵌入字段的测试，并且也要设置嵌入值。
11. **RunBudget 收尾改写要求 `caps` 为真。** `finalTurn` 先看 `b.caps`（[middleware/runbudget.go:203](../../middleware/runbudget.go#L203) 到 [middleware/runbudget.go:204](../../middleware/runbudget.go#L204)）。测试要设置任一非零上限，同时用 `agent.WithMaxTurns(1)` 让当前步骤成为循环最后一步。
12. **`checkpoint.Memory` 同时实现 `ThreadCoordinator`。** 用 `NewMemory` 预置历史后运行会走 `BeginRun`，拿到租约。观察 `ReleaseClaim` 的测试改用只嵌入 `checkpoint.Durable` 的包装（模式见 [agent/lease_config_test.go:302](../../agent/lease_config_test.go#L302)），这样没有 `BeginRun`/写守卫，提交仍然走 `Durable.Commit`。
13. **`phase1FailOnSaveStore` 只实现 `Checkpointer`。** 它会让提交走 `Store.Save`（[agent/durable_runtime.go:355](../../agent/durable_runtime.go#L355) 到 [agent/durable_runtime.go:363](../../agent/durable_runtime.go#L363)），因此可以用 `SaveCount` 精确断言「中途检查点失败」和「只写了终态检查点」。
14. **`HistoryCompacted` 的注释现在只描述持久模式。** 恢复路径也会发布它，注释要说明「持久压缩或超限恢复」；`WindowTrimmed.Strategy` 的取值列表要加 `"overflow_tail"`。

## 3. 锁定的设计决定

### D1 分类在 llm 包、在构造错误时一次完成

提供方只调用 `llm.NewStatusError(provider, code, body)`；是不是超限、能解析出哪些数字，都在这一刻决定。循环和中间件不再看响应体，也不按错误字符串二次判断。这样分类逻辑只有一处，提供方以后新增时不会各写一套。

### D2 `ContextLengthError` 嵌入 `*StatusError`，覆盖 `Retryable`

```go
type ContextLengthError struct {
    *StatusError
    Limit     int
    Requested int
}
```

- 不重写 `Error()`，日志仍是 `<provider>: status <code>: <body>`。
- 显式 `Unwrap() error { return e.StatusError }`，让旧的 `errors.As(err, &se)` 继续可用。
- `Retryable()` 恒为 false。`IsRetryable` 必须先调用 `IsContextLength`，否则 `errors.As` 会找到嵌入的 `*StatusError`，一个 429/5xx 编码的超限又会被判为可重试。

### D3 只对 400/413 分类，标记优先，数字尽力而为

- 状态码不是 400/413 时，无论响应体写什么都返回普通 `*StatusError`。
- 400/413 命中标记就升级为 `ContextLengthError`，即使一个数字都解析不出来。
- 数字模式只覆盖 OpenAI 和 Anthropic 的明确写法；通用标记只分类，不填数字。
- 标记匹配大小写不敏感，响应体原样保留在 `Body` 里，便于排查。

### D4 恢复是中间件能力，`Stack` 取第一个严格更短的结果

`agent.ContextOverflowRecoverer` 与 `HistoryCompacter` 并列；`BaseMiddleware` 不实现它，避免所有中间件都被当成恢复者。`Stack.RecoverContext` 按注册顺序询问，只接受 `changed && len(out) > 0 && len(out) < len(history)` 的结果。严格更短的检查由 `Stack` 统一做，不信任单个恢复者的 `changed`。

### D5 默认一次，按步骤计数，0 关闭

默认值在 `agent.New` 的 `config` 字面量里种下：`contextOverflowRetry: 1`。0 是合法值，表示关闭，不能用零值当默认。负数在 `WithContextOverflowRetry` 里归 0。计数在 `runStep` 内，每个步骤重新从 0 开始；`MaxTurns` 不变，恢复不额外占一步。

### D6 恢复的前置条件按成本从低到高排列

只有同时满足以下条件才调用恢复者：

1. 错误能被 `errors.As` 解析为 `*llm.ContextLengthError`；
2. `genCtx.Err() == nil`（运行上下文仍然有效）；
3. `emitted == false`（本次调用没有发布过任何响应）；
4. `attempts < l.contextOverflowRetry`。

顺序不能换。取消/超时、已输出、预算用尽都不应该再花一次摘要模型调用。

### D7 重试请求从冻结视图重建，不重跑 `BeforeModel`/`ModifyRequest`

`retryReq` 的 `System`、`Tools`、`Options` 直接取冻结视图，只有 `Messages` 换成恢复后的历史。原因有三处现有证据：RAG 会追加系统提示（[middleware/rag.go:58](../../middleware/rag.go#L58) 到 [middleware/rag.go:61](../../middleware/rag.go#L61)）、RunBudget 收尾会追加指令并清空工具（[middleware/runbudget.go:180](../../middleware/runbudget.go#L180) 到 [middleware/runbudget.go:182](../../middleware/runbudget.go#L182)）、`AddTool` 会同时写运行期注入表和请求（[agent/loopctx.go:64](../../agent/loopctx.go#L64) 到 [agent/loopctx.go:69](../../agent/loopctx.go#L69)）。这些都不是幂等改写。

代价是请求级、改写消息的中间件不会在重试时重跑。需要这类改写的部署应实现自己的 `ContextOverflowRecoverer`，在返回历史前完成改写。

### D8 必须同时更新 `req` 和 `lc.Request`

设计稿的伪代码遗漏了 `req = retryReq`。实际实现里两个都要赋值：`req` 决定下一次 `streamModel` 发什么，`lc.Request` 决定 `AfterModel` 和后续中间件看到什么。CO-03 用两个测试分别盯住这两条。

### D9 先写中途检查点，再发布事件，再重试

中间检查点使用同一个步骤号、`Release:false`、新视图的 `snapshot()`。提交成功后才发布 `ContextOverflowRetried`，然后重新推导 `ModelContext` 并 `continue`。这样订阅方收到事件时读 `store.Latest`，已经是恢复后的历史。

中途检查点失败就不重试，交给 `fail` 再尝试一次 `Release:true` 的收尾检查点，错误用 `errors.Join` 合并。冻结失败同理，不写中途检查点、不发重试事件。

### D10 恢复失败不掩盖原错误

- 没有恢复者、恢复者返回等长/更长/空历史：返回最初的超限错误。
- 摘要失败或恢复后的估算没有变小：由恢复者返回 false，循环返回最初的超限错误。
- 冻结失败：返回 `errors.Join(超限错误, 冻结错误)`。
- 中途检查点失败：返回 `errors.Join(超限错误, 检查点错误)`，`fail` 还可能再合并终态检查点错误。
- 第二次调用失败：返回第二次的错误，不保留第一次的；如果第二次不是超限，也照原样返回。

### D11 `Compaction` 恢复：强制摘要，目标减半

- 目标：`min(limit*3/4, before/2)`（`limit > 0`）；未知上限时 `before/2`。
- 从 `keep = len-1` 递减，用 `safeCut` 找第一个使保留后缀估算 `<= target/2` 的合法边界；找不到就退回 `safeCut(msgs, 1)`；仍为 0 则放弃。
- 摘要前缀，结果为 `[summary] + 后缀`。
- 只有消息条数和估算都严格变小才返回成功。
- 不检查 `MaxTokens`，不使用 `KeepRecent`；恢复不看正常阈值。
- 发布 `HistoryCompacted`，字段与持久模式一致：`Dropped=cut`、`Kept=len(out)-1`、`EstTokens=before`、`Step=lc.Step`。
- 请求模式（`Persist:false`）也实现恢复；正常请求模式仍然不发布该事件。

### D12 `Window` 恢复：整单元尾部淘汰

- 目标：`min(limit/2, before/2)`（`limit > 0`）；未知上限时 `before/2`。
- 用 `groupUnits` 从后往前整单元保留，最新单元无条件保留；超过目标就停。
- 用 `repairPairing` 修复孤立工具结果；修复后为空、等长、更长或估算没有变小都放弃。
- 不调用配置的 `RecentN`/`SlidingWindow`/`ImportanceWeighted` 预算。
- 事件复用 `WindowTrimmed`，`Strategy` 用 `"overflow_tail"`，`Dropped`/`Kept` 按修复后的消息数统计。

### D13 事件字段固定，顺序固定

```go
type ContextOverflowRetried struct {
    Step           int
    Attempt        int
    Model          string
    Provider       string
    Limit          int
    Requested      int
    MessagesBefore int
    MessagesAfter  int
}
```

`Attempt` 从 1 开始，表示本步骤内第几轮恢复。`Model` 取冻结视图的 `Options.Model`，`Provider`/`Limit`/`Requested` 取类型化错误，未知时为空/0。每个恢复轮次一条。JSON 类型名 `context_overflow_retried`，字段全部 `omitempty`。

### D14 第二次失败用第二次的错误

恢复后的历史已经变了，第二次请求才是当前真实请求。第一次的错误只用于决定是否恢复；最终失败时返回第二次的错误，这样调用方看到的 `Limit`/`Requested` 与最后一次请求对应。

### D15 既有行为保持不变

- 正常 `Compaction` 的阈值、`KeepRecent`、持久/请求模式不变。
- 正常 `Window` 的三个策略、预算、事件不变。
- 检查点结构、`StepSnapshot` 字段、`Resume`/HITL 语义不变。
- `StatusError` 的字段、`Error()` 字符串、直接构造方式不变。
- `Agent.New` 的其他默认值、`MaxTurns`、工具执行路径不变。
- `RetryModel` 对其他错误的退避和次数不变。

## 4. 内部契约

### 4.1 llm 错误类型与分类

`llm/error.go` 新增：

```go
type ContextLengthError struct {
    *StatusError
    Limit     int
    Requested int
}

func (e *ContextLengthError) Unwrap() error { return e.StatusError }
func (e *ContextLengthError) Retryable() bool { return false }

func NewStatusError(provider string, code int, body string) error
func IsContextLength(err error) bool
```

`NewStatusError` 的规则：

```go
func NewStatusError(provider string, code int, body string) error {
    se := &StatusError{Provider: provider, Code: code, Body: body}
    if code != 400 && code != 413 {
        return se
    }
    limit, requested, matched := parseContextOverflow(body)
    if !matched {
        return se
    }
    return &ContextLengthError{StatusError: se, Limit: limit, Requested: requested}
}
```

标记和数字模式：

| 来源 | 状态 | 标记（大小写不敏感） | 数字模式 |
| --- | --- | --- | --- |
| OpenAI | 400 | `context_length_exceeded`、`maximum context length` | `maximum context length is <N> tokens` → `Limit`；`resulted in <M> tokens` 或 `requested <M> tokens` → `Requested` |
| Anthropic | 400 | `prompt is too long`、`exceed context limit` | `prompt is too long: <M> tokens > <N> maximum` → `Requested=M, Limit=N`；`exceed context limit: <A> + <B> > <N>` → `Requested=A+B, Limit=N` |
| 通用兼容层 | 400/413 | `too many tokens`、`input is too long`、`context window exceeded`、`request too large` | 只分类，不填数字 |

`IsRetryable` 的顺序：

```go
func IsRetryable(err error) bool {
    if err == nil {
        return false
    }
    if IsContextLength(err) {
        return false
    }
    if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
        return false
    }
    var se *StatusError
    if errors.As(err, &se) {
        return se.Retryable()
    }
    return true
}
```

两个提供方把 [llm/openaicompat/openaicompat.go:89](../../llm/openaicompat/openaicompat.go#L89) 和 [llm/anthropic/anthropic.go:89](../../llm/anthropic/anthropic.go#L89) 的裸构造改成 `llm.NewStatusError(...)`。

`RetryModel` 的放弃条件改成：

```go
if yielded || llm.IsContextLength(failed) || attempt >= r.max-1 {
    yield(nil, failed)
    return
}
```

判断放在退避之前，避免为一个注定由循环恢复的错误等待。

### 4.2 agent 能力与 `Stack` 折叠

`agent/middleware.go` 在 `HistoryCompacter` 之后新增：

```go
type ContextOverflowRecoverer interface {
    RecoverContext(lc *LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool)
}
```

`Stack` 新增：

```go
func (s *Stack) RecoverContext(lc *LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool) {
    for _, m := range s.mws {
        r, ok := m.(ContextOverflowRecoverer)
        if !ok {
            continue
        }
        out, changed := r.RecoverContext(lc, history, overflow)
        if changed && len(out) > 0 && len(out) < len(history) {
            return out, true
        }
    }
    return history, false
}
```

`BaseMiddleware` 不新增方法。`Stack` 的注释写明：严格更短由折叠层兜底；第一个生效，后面的恢复者不再询问。

### 4.3 选项与字段

`agent/options.go` 的 `config` 新增 `contextOverflowRetry int`，并新增：

```go
// WithContextOverflowRetry bounds how many recovery rounds one step may run
// after a typed context-length overflow. The default is 1; 0 disables recovery.
// Values above 1 allow that many compression rounds in the same step; each
// round must shrink the history further. Negative values are treated as 0.
func WithContextOverflowRetry(n int) Option {
    return func(c *config) {
        if n < 0 {
            n = 0
        }
        c.contextOverflowRetry = n
    }
}
```

`agent.New` 的默认字面量改为：

```go
c := config{maxTurns: defaultMaxTurns, claimLease: checkpoint.DefaultLease, contextOverflowRetry: 1}
```

`AgentLoop` 新增 `contextOverflowRetry int`，`newLoop` 复制它。`newLoop` 不做默认值兜底，保持「默认值只在 `New` 种下」的单一入口。

### 4.4 `streamModel` 增加 `emitted`

```go
func (l *AgentLoop) streamModel(genCtx context.Context, rc *RunContext, lc *LoopContext, req *llm.Request) (final *llm.Response, ok bool, emitted bool, err error)
```

- 发布 `Partial` 时 `emitted = true`；发布最终 `MessageDone` 时 `emitted = true`。
- 出错时 `ok=false`，`emitted` 保留真实值。
- 成功时 `ok=true`、`err=nil`。模型一个响应都不给仍按空回复成功处理，与今天一致。
- 唯一调用点在 [agent/loop.go:324](../../agent/loop.go#L324)，改造成下面的循环。

### 4.5 循环恢复状态机

下面是 `runStep` 中替换第一次 `streamModel` 调用的完整形状。`final` 在循环外声明，循环结束后继续原来的 `AfterModel` 流程。

```go
attempts := 0
var final core.Message
for {
    finalResp, _, emitted, err := l.streamModel(genCtx, rc, lc, req)
    if err == nil {
        final = finalResp.Message
        break
    }

    var cle *llm.ContextLengthError
    isOverflow := errors.As(err, &cle)
    if !isOverflow || genCtx.Err() != nil || emitted || attempts >= l.contextOverflowRetry {
        return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
    }

    before := len(history)
    recovered, changed := l.mw.RecoverContext(lc, history, cle)
    if !changed {
        return l.fail(rc, step, history, lc.viewSnapshot(), err), st, false
    }

    retryReq := &llm.Request{
        System:   view.System(),
        Messages: recovered,
        Tools:    view.Tools().Schemas(),
        Options:  view.Options(),
    }
    retryView, ferr := l.freezeStepView(lc, retryReq)
    if ferr != nil {
        return l.fail(rc, step, history, lc.viewSnapshot(), errors.Join(err, ferr)), st, false
    }

    history = recovered
    lc.History = history
    lc.Request = retryReq
    req = retryReq
    rc.State.Messages = history
    if cerr := l.checkpoint(rc, commitOptions{
        Step:     step,
        Snapshot: retryView.snapshot(),
        Release:  false,
    }); cerr != nil {
        return l.fail(rc, step, history, retryView.snapshot(), errors.Join(err, cerr)), st, false
    }

    view = retryView
    lc.view = view
    attempts++
    rc.publish(core.ContextOverflowRetried{
        Step:           step,
        Attempt:        attempts,
        Model:          view.Model(),
        Provider:       cle.Provider,
        Limit:          cle.Limit,
        Requested:      cle.Requested,
        MessagesBefore: before,
        MessagesAfter:  len(history),
    })
    genCtx = l.mw.ModelContext(lc, rc.Context)
}
```

实现时注意：

- `final` 的类型是 `core.Message`；原来的 `finalResp.Message` 只在成功路径读取。
- `ok` 在错误路径上恒为 false，片段用 `_` 丢弃；如果实现选择保留变量名，不要把 `ok` 当成 `emitted`。
- `l.checkpoint` 的 `Release` 字段显式写 `false`，不要依赖零值，方便审阅。
- 重试前不发布 `TurnStarted`/`TurnDone`，不重跑 `BeforeModel`、`ModifyRequest`、`checkReplayDrift`、steering/inbox、工具执行。
- 恢复后的历史一旦写入 `rc.State.Messages`，即使第二次失败，`fail` 写出的终态检查点也是恢复后的版本。
- `cle` 来自 `errors.As`，可以是包装在别的错误里的值；`cle.Provider` 等字段要求嵌入的 `*StatusError` 非空。

### 4.6 请求重建

```go
retryReq := &llm.Request{
    System:   view.System(),          // 已包含 RAG/收尾指令等改写
    Messages: recovered,              // 唯一变化的部分
    Tools:    view.Tools().Schemas(), // 已包含 AddTool 注入
    Options:  view.Options(),         // 已 Apply 过 modelOpts + ModifyRequest 的改动
}
```

`view.Tools().Schemas()` 和 `view.Options()` 都返回副本或安全值（[agent/stepview.go:201](../../agent/stepview.go#L201)、[agent/stepview.go:131](../../agent/stepview.go#L131)），重试不会改动冻结视图。`view.Messages()` 不使用；消息来自恢复者的返回值。

### 4.7 中途检查点

```go
l.checkpoint(rc, commitOptions{
    Step:     step,
    Snapshot: retryView.snapshot(),
    Release:  false,
})
```

`Release:false` 表示运行仍在继续，durable 运行保留线程租约。步骤结束时的检查点仍按原来路径决定 `Release`。这一步失败时 `fail` 会再写一个 `Release:true` 的终态检查点；两者错误都保留。

### 4.8 `Compaction` 恢复

在 `middleware/compaction.go` 新增：

```go
func compactionRecoveryTarget(before, limit int) int {
    if limit > 0 {
        return min(limit*3/4, before/2)
    }
    return before / 2
}

func forceCut(c *compaction, lc *agent.LoopContext, msgs []core.Message, target int) int {
    for keep := len(msgs) - 1; keep >= 1; keep-- {
        cut := safeCut(msgs, keep)
        if cut > 0 && c.calibratedEstimate(lc, msgs[cut:]) <= target/2 {
            return cut
        }
    }
    return safeCut(msgs, 1)
}

func (c *compaction) RecoverContext(lc *agent.LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool) {
    if c.model == nil || len(history) <= 1 {
        return history, false
    }
    before := c.calibratedEstimate(lc, history)
    target := compactionRecoveryTarget(before, overflow.Limit)
    if target <= 0 || target >= before {
        return history, false
    }
    cut := forceCut(c, lc, history, target)
    if cut <= 0 {
        return history, false
    }
    summary, err := c.summarize(lc.Context, history[:cut])
    if err != nil || summary == "" {
        return history, false
    }
    note := core.Message{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: summaryMark + summary}}}
    out := append([]core.Message{note}, history[cut:]...)
    if len(out) >= len(history) || c.calibratedEstimate(lc, out) >= before {
        return history, false
    }
    publishCompacted(lc, before, cut, len(out)-1)
    return out, true
}
```

把 [middleware/compaction.go:114](../../middleware/compaction.go#L114) 到 [middleware/compaction.go:116](../../middleware/compaction.go#L116) 的内联发布抽成：

```go
func publishCompacted(lc *agent.LoopContext, before, cut, kept int) {
    if lc.Bus == nil {
        return
    }
    lc.Bus.Publish(lc.Topic, core.HistoryCompacted{
        Dropped: cut, Kept: kept, EstTokens: before, Step: lc.Step,
    })
}
```

正常持久压缩和恢复路径共用它，避免字段口径漂移。`var _ agent.ContextOverflowRecoverer = (*compaction)(nil)` 加到现有的接口断言区。

### 4.9 `Window` 恢复

在 `middleware/window.go` 新增：

```go
func windowRecoveryTarget(before, limit int) int {
    if limit > 0 {
        return min(limit/2, before/2)
    }
    return before / 2
}

func (w *window) estimate(lc *agent.LoopContext, msgs []core.Message) int {
    return int(float64(w.counter(msgs)) * loadCalibDefaultAt(lc, kvWindow))
}

func keepTailWithin(lc *agent.LoopContext, w *window, msgs []core.Message, target int) []core.Message {
    units := groupUnits(msgs)
    used, start := 0, len(units)
    for i := len(units) - 1; i >= 0; i-- {
        size := w.estimate(lc, msgs[units[i].start:units[i].end])
        if i != len(units)-1 && used+size > target {
            break
        }
        start = i
        used += size
    }
    if start <= 0 {
        return msgs
    }
    return keptMessages(msgs, units[start:])
}

func (w *window) RecoverContext(lc *agent.LoopContext, history []core.Message, overflow *llm.ContextLengthError) ([]core.Message, bool) {
    if w.strategy == nil || len(history) <= 1 {
        return history, false
    }
    before := w.estimate(lc, history)
    target := windowRecoveryTarget(before, overflow.Limit)
    if target <= 0 || target >= before {
        return history, false
    }
    out := keepTailWithin(lc, w, history, target)
    out = repairPairing(out)
    after := w.estimate(lc, out)
    if len(out) == 0 || len(out) >= len(history) || after >= before {
        return history, false
    }
    w.publishTrimmedAs(lc, "overflow_tail", history, out, before)
    return out, true
}
```

`apply` 的局部 `count` 闭包改成委托 `w.estimate`，正常路径和恢复路径使用同一套校准。`publishTrimmed` 拆成：

```go
func (w *window) publishTrimmed(lc *agent.LoopContext, in, out []core.Message, est int) {
    w.publishTrimmedAs(lc, w.strategy.Name(), in, out, est)
}

func (w *window) publishTrimmedAs(lc *agent.LoopContext, strategy string, in, out []core.Message, est int) {
    if lc.Bus == nil {
        return
    }
    lc.Bus.Publish(lc.Topic, core.WindowTrimmed{
        Strategy:  strategy,
        Dropped:   len(in) - len(out),
        Kept:      len(out),
        EstTokens: est,
        Step:      lc.Step,
    })
}
```

`var _ agent.ContextOverflowRecoverer = (*window)(nil)` 加到现有的接口断言区。

### 4.10 事件与 JSON

`core/event.go` 在 `WindowTrimmed` 之后新增结构体，在 marker 区新增 `func (ContextOverflowRetried) isEvent() {}`。`HistoryCompacted` 的注释改为覆盖「持久压缩或超限恢复」；`WindowTrimmed.Strategy` 的取值列表增加 `"overflow_tail"`。

`core/eventjson.go` 的 `eventWire` 增加：

```go
Attempt        int    `json:"attempt,omitempty"`
Model          string `json:"model,omitempty"`
Provider       string `json:"provider,omitempty"`
Limit          int    `json:"limit,omitempty"`
Requested      int    `json:"requested,omitempty"`
MessagesBefore int    `json:"messages_before,omitempty"`
MessagesAfter  int    `json:"messages_after,omitempty"`
```

编码分支：

```go
case ContextOverflowRetried:
    w.Type, w.Step, w.Attempt = "context_overflow_retried", e.Step, e.Attempt
    w.Model, w.Provider = e.Model, e.Provider
    w.Limit, w.Requested = e.Limit, e.Requested
    w.MessagesBefore, w.MessagesAfter = e.MessagesBefore, e.MessagesAfter
```

解码分支：

```go
case "context_overflow_retried":
    return ContextOverflowRetried{
        Step: w.Step, Attempt: w.Attempt,
        Model: w.Model, Provider: w.Provider,
        Limit: w.Limit, Requested: w.Requested,
        MessagesBefore: w.MessagesBefore, MessagesAfter: w.MessagesAfter,
    }, nil
```

字段全部 `omitempty`，不影响旧事件的编码。旧版本 `UnmarshalEvent` 遇到新类型仍会返回 `unknown event type`，跨进程部署需要同步升级消费者。

## 5. 交付顺序与提交

### 5.1 预检

```bash
git status --short
git log -1 --oneline
go build ./...
go vet ./...
go test ./... -count=1
```

确认基线为 `2f18017`，工作区里只有已知的未跟踪文档。如果有额外的已修改文件，先弄清楚来源，不要清理或覆盖。

### 5.2 顺序

| 顺序 | 任务卡 | 内容 | 依赖 | 建议提交信息 |
| --- | --- | --- | --- | --- |
| 1 | CO-01 | llm 错误类型、分类器、提供方接入、RetryModel | 无 | `feat(llm): classify context length overflows` |
| 2 | CO-02 | `ContextOverflowRetried` 事件与 JSON | 无 | `feat(core): add context overflow retry event` |
| 3 | CO-03 | agent 能力、选项、`streamModel`/`runStep` 状态机 | CO-01、CO-02 | `feat(agent): retry a model call after context overflow` |
| 4 | CO-04 | `Compaction` 恢复与端到端测试 | CO-03 | `feat(middleware): recover compaction on context overflow` |
| 5 | CO-05 | `Window` 恢复、注释、全量门禁 | CO-04 | `feat(middleware): recover window on context overflow` |

每张卡提交前至少运行该卡的验证命令。不要把多张卡混在一个 commit 里；不要把未跟踪的分析/计划文档加进提交。

## 6. 任务卡

### CO-01 llm 超限分类与重试中间件

**目标：**提供方返回 400/413 上下文超限时，错误是 `*llm.ContextLengthError`，带可解析的 `Limit`/`Requested`；`RetryModel` 不再重发这类请求。

**先写的测试：**

1. `llm/error_test.go`（包 `llm_test`）新增 `TestNewStatusErrorClassifiesContextLength`，表驱动覆盖：
   - OpenAI 400：`maximum context length is 8192 tokens, however you requested 9000 tokens` → `Limit=8192`、`Requested=9000`；
   - OpenAI 400：`... resulted in 9000 tokens` → `Requested=9000`、`Limit=0`；
   - OpenAI 400：`... requested 9000 tokens` → `Requested=9000`；
   - Anthropic 400：`prompt is too long: 210000 tokens > 200000 maximum` → `Requested=210000`、`Limit=200000`；
   - Anthropic 413：`exceed context limit: 100 + 50 > 120` → `Requested=150`、`Limit=120`；
   - 通用 400：`input is too long` → 类型化，`Limit/Requested=0`；
   - 通用 413：`request too large` → 类型化；
   - 400 且无标记：`invalid model` → 普通 `*StatusError`，不是 `*ContextLengthError`；
   - 429 且带标记：仍是普通 `*StatusError`（只对 400/413 分类）；
   - 500 且带标记：仍是普通 `*StatusError`。
2. `TestContextLengthErrorUnwrapsStatusError`：`Error()` 字符串与 `StatusError` 一致；`errors.As` 能拿到 `*StatusError` 和 `*ContextLengthError`。
3. `TestIsRetryableExcludesContextLength`：手工构造一个嵌入 429 `StatusError` 的 `*ContextLengthError`（显式设置嵌入值），`IsRetryable` 必须为 false；用 `fmt.Errorf("wrap: %w", ...)` 包装后仍为 false。
4. `TestRetryModelDoesNotRetryContextLength`（`middleware/middleware_test.go`）：模型第一次返回 `llm.NewStatusError("test", 400, "maximum context length is 10 tokens, however you requested 20 tokens")`，之后本可成功；`RetryOptions{MaxAttempts: 3, BaseDelay: time.Millisecond}`；断言模型只被调用 1 次，返回的错误仍是 `*ContextLengthError`。
5. `llm/openaicompat/openaicompat_test.go` 新增 `TestGenerateClassifiesContextLength`：`httptest.NewServer` 返回 400 与 OpenAI 风格响应体；调用 `New(Config{BaseURL: server.URL, ...}).Generate`，断言错误可解析为 `*ContextLengthError` 且数字正确。
6. `llm/anthropic/anthropic_test.go` 新增同名测试：`httptest.NewServer` 返回 400 与 Anthropic 风格响应体；`New(..., WithBaseURL(server.URL))`；断言同上。

**实现要求：**

- `llm/error.go`：新增 `ContextLengthError`、`Unwrap`、`Retryable`、`NewStatusError`、`IsContextLength`、`parseContextOverflow`；在 `IsRetryable` 的取消检查之前加入 `IsContextLength` 前置判断；更新 `StatusError` 注释，说明新代码用 `errors.As`，直接类型断言在超限时拿不到。
- 解析器大小写不敏感；先找标记，再按来源匹配数字；通用标记只返回 matched=true。
- `llm/openaicompat/openaicompat.go:89`、`llm/anthropic/anthropic.go:89` 改用 `llm.NewStatusError`。
- `middleware/retry.go:64` 的放弃条件加入 `llm.IsContextLength(failed)`，并在注释里说明「超限由循环恢复，不在这里重发」。
- 不改变 `StatusError` 的字段、`Error()` 和直接构造方式。

**验证命令：**

```bash
go test ./llm -run 'Test(NewStatusError|ContextLengthError|IsRetryable)' -count=1
go test ./llm/... -count=1
go test ./middleware -run 'TestRetryModel' -count=1
go vet ./llm/... ./middleware
```

**完成标准：**上述测试全过；`go test ./llm/...` 和 `TestRetryModel*` 全过；`go vet` 无输出。

**反向验证（在实现完成后做，逐条用 Edit 还原）：**

| 改错 | 预期失败 |
| --- | --- |
| 删掉 `IsRetryable` 里的 `IsContextLength` 前置判断 | `TestIsRetryableExcludesContextLength` |
| 删掉 `RetryModel` 的 `llm.IsContextLength(failed)` | `TestRetryModelDoesNotRetryContextLength` |
| 把 `NewStatusError` 的状态门改成 `code >= 400` | 429/500 两个用例 |
| 删掉 `Unwrap` | `TestContextLengthErrorUnwrapsStatusError` |
| 交换 Anthropic 的 `Limit`/`Requested` 赋值 | Anthropic 两个用例 |

### CO-02 上下文溢出重试事件

**目标：**新增 `core.ContextOverflowRetried`，可序列化/反序列化，字段完整，旧事件编码不变。

**先写的测试：**

1. `core/stuckevent_test.go` 新增 `TestContextOverflowRetriedEventRoundTrip`，所有字段非零，断言 `MarshalEvent`/`UnmarshalEvent` 往返相等。
2. 同一个测试文件新增 `TestContextOverflowRetriedEventOmitsUnknownFields`：`Limit/Requested/Attempt` 等为零时，编码后的 JSON 不包含对应键；用 `json.Unmarshal` 到 `map[string]any` 断言键不存在。不要伪造完整 JSON 字符串，让编码器自己产生输出。
3. 可选但推荐：在同一个测试里断言未知事件类型仍返回错误，确认新增分支没有放宽 `default`。

**实现要求：**

- `core/event.go`：在 `WindowTrimmed` 之后新增结构体；marker 区在 `WindowTrimmed` 之后新增方法；更新 `HistoryCompacted` 注释为「持久压缩或超限恢复」；`WindowTrimmed.Strategy` 注释增加 `"overflow_tail"`（这一条也可以留到 CO-05，但一次改完更省事）。
- `core/eventjson.go`：`eventWire` 增加七个字段；`MarshalEvent` 增加 `ContextOverflowRetried` 分支，类型字符串 `context_overflow_retried`；`UnmarshalEvent` 增加对应分支；字段顺序与设计稿一致。
- 所有字段带 `omitempty`；不改现有事件的编码分支。
- 不在事件里加入恢复者名字；恢复者自己的 `HistoryCompacted`/`WindowTrimmed` 已经给出来源。

**验证命令：**

```bash
go test ./core -run 'Test(ContextOverflowRetried|HistoryCompacted|WindowTrimmed|StepViewDrifted)' -count=1
go test ./core -count=1
gofmt -l core/event.go core/eventjson.go core/stuckevent_test.go
```

**完成标准：**往返测试和 `omitempty` 测试通过；core 全量测试通过；`gofmt -l` 无输出（Windows 上既有 CRLF 文件不在此列，只检查本卡改动的文件）。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 删掉 marshal 里的 `w.Provider = e.Provider` | `TestContextOverflowRetriedEventRoundTrip` |
| 删掉 decode 里的 `MessagesBefore` | 往返测试 |
| 去掉某个字段的 `omitempty` | `TestContextOverflowRetriedEventOmitsUnknownFields` |
| 把类型字符串改成 `context_overflow_retry` | 往返测试（解码未知类型） |

### CO-03 agent 恢复状态机与选项

**目标：**默认每个步骤允许一次超限恢复：类型化错误、未输出、上下文有效、预算未用尽时，询问恢复者；恢复后重建请求、重新冻结、写 `Release:false` 的中途检查点、发事件、重试。`0` 关闭。

**先写的测试：**新建 `agent/overflow_test.go`（包 `agent_test`），并复用现有 `phase1Drain`、`viewShapeTool`、`phase1FailOnSaveStore`。测试夹具：

- `overflowError(limit, requested int) error`：`llm.NewStatusError("test", 400, fmt.Sprintf("maximum context length is %d tokens, however you requested %d tokens", limit, requested))`。
- `seededStore(t, msgs)`：`checkpoint.NewMemory()`，用 `store.Save` 写入 `Checkpoint{ID:"seed", ThreadID:tid, Step:0, State: core.State{Messages: msgs}}`。
- `dropOldest{n}`：嵌入 `agent.BaseMiddleware`，实现 `RecoverContext`，`len(history) > n` 时返回 `history[n:], true`。
- `partialThenErrorModel`：直接实现 `llm.Model`，第一次 `yield(mock.Partial("half"), nil)`，第二次 `yield(nil, overflowError(...))`。
- `releaseRecorder`：嵌入 `checkpoint.Durable`，覆写 `Commit` 记录 `req.ReleaseClaim`，其余委托；预置历史用它的底层 `Save`。
- `countingMW`：记录 `BeforeModel`、`ModifyRequest`、`ModelContext` 次数，并可选择在 `ModifyRequest` 里追加系统标记。

测试清单：

1. `TestContextOverflowRecoversAndRetries`：预置 6 条历史，恢复者用 `dropOldest{5}`，模型第一次超限、第二次 `mock.Text("ok")`；默认配置；断言模型调用 2 次、运行成功、第二次请求更短、恰有一条 `ContextOverflowRetried`（`Attempt=1`、`MessagesBefore` 等于第一次请求的消息数、`MessagesAfter` 等于第二次请求的消息数、`Provider="test"`、`Limit/Requested` 正确）。
2. `TestContextOverflowDisableOption`：`agent.WithContextOverflowRetry(0)`；断言模型只调用 1 次、错误 `errors.As` 到 `*ContextLengthError`、没有重试事件；用 `phase1FailOnSaveStore` 的空 fail 表断言 `SaveCount()==1`（只有终态检查点）。
3. `TestContextOverflowDoesNotRetryAfterPartial`：`partialThenErrorModel`；断言模型调用 1 次、收到一个 `MessageDelta`、没有 `ContextOverflowRetried`、错误是类型化超限。
4. `TestContextOverflowPersistsBeforeRetry`：模型第二次被调用时读 `store.Latest`；断言最新快照 `Step==0`、`Pause==nil`、`State.Messages` 等于第二次请求的消息、快照 ID 不是预置的 `"seed"`。
5. `TestContextOverflowMidStepKeepsClaim`：`releaseRecorder` + 默认恢复者；断言提交序列为 `[false, true]`（中途检查点不释放租约，步骤结束释放）。
6. `TestContextOverflowSecondFailureReturnsSecondError`：模型三次返回不同 `Requested` 的超限错误；`WithContextOverflowRetry(2)`；断言调用 3 次、两条事件 `Attempt=1,2`、最终错误里的 `Requested` 是第三次的值。
7. `TestContextOverflowNoRecovererFails`：不装恢复者；断言调用 1 次、错误类型化、没有事件。
8. `TestContextOverflowRequiresShrink`：恢复者返回 `(history, true)` 和 `(nil, true)` 两种；断言都不重试、没有事件。
9. `TestContextOverflowCheckpointFailureDoesNotRetry`：`phase1FailOnSaveStore` 让第 1 次 `Save` 失败（第 1 次就是中途检查点），种子写入底层；断言模型调用 1 次、错误同时 `errors.Is` 超限错误和保存错误、没有重试事件、`SaveCount()==2`。
10. `TestContextOverflowFreezeFailureDoesNotRetry`：agent 带一个 `viewShapeTool{name:"x", desc:"old", schema:...}`；恢复者先 `lc.AddTool(&viewShapeTool{name:"x", desc:"new", schema:...})` 再返回更短历史；断言模型调用 1 次、错误 `errors.Is` `agent.ErrToolViewInvalid` 和超限错误、没有事件。
11. `TestContextOverflowCanceledContextDoesNotRecover`：用 `context.WithCancel`，模型在返回超限前调用 `cancel()`；断言模型调用 1 次、没有事件、运行以错误结束。
12. `TestContextOverflowAfterModelSeesRetryRequest`：自定义中间件在 `AfterModel` 记录 `lc.Request.Messages`；断言等于第二次请求的消息，且比第一次短。这个用例防止只改 `req` 不改 `lc.Request`。
13. `TestContextOverflowModifyRequestRunsOnce`：`countingMW`；断言 `BeforeModel=1`、`ModifyRequest=1`、`ModelContext=2`。
14. `TestContextOverflowDoesNotDuplicateSystemRewrite`：`countingMW` 在 `ModifyRequest` 里给 `req.System` 追加唯一标记；断言第一次和第二次请求里标记都只出现一次（第二次不是追加两次）。
15. `TestContextOverflowRunBudgetWrapUp`：`agent.WithMaxTurns(1)` + `middleware.RunBudget(RunBudgetOptions{MaxTurns: 100})`；第一次超限、第二次成功；断言第二次请求 `Tools` 为空、`System` 里 `middleware.WarnMarkerBudget` 只出现一次。
16. `TestContextOverflowOptionNegativeIsDisabled`：`WithContextOverflowRetry(-2)`；断言行为与 0 相同，模型只调用 1 次。

**实现要求：**

- `agent/middleware.go`：新增接口和 `Stack.RecoverContext`；不把它加到 `BaseMiddleware`。
- `agent/options.go`：`config` 字段和 `WithContextOverflowRetry`；`agent/agent.go:38` 的默认字面量加 1。
- `agent/loop.go`：`AgentLoop` 字段、`newLoop` 复制、`streamModel` 的 `emitted` 返回值、`runStep` 的恢复循环（见 §4.5）。严格按 §4.5 同时更新 `req` 和 `lc.Request`。
- `agent/failure_test.go`：给 `scriptModel` 增加只读的 `requests() []*llm.Request`（复制切片），供需要读取请求的测试使用；不改变它的 `Generate` 行为。
- 不在重试路径调用 `checkReplayDrift`、`BeforeModel`、`ModifyRequest`、`CompactHistory`、工具执行。
- 不新增 `LoopContext` 字段；重试次数是 `runStep` 的局部变量。

**验证命令：**

```bash
go test ./agent -run 'TestContextOverflow' -count=1
go test ./agent -count=1
go test -race ./agent -run 'TestContextOverflow' -count=1
go vet ./agent
```

**完成标准：**16 个用例全过；agent 全量测试和 race 通过；`go vet` 无输出；没有修改现有测试的预期行为。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 去掉 `emitted` 条件 | `TestContextOverflowDoesNotRetryAfterPartial` |
| 去掉 `genCtx.Err() == nil` | `TestContextOverflowCanceledContextDoesNotRecover` |
| 去掉 `req = retryReq` | `TestContextOverflowRecoversAndRetries`（第二次请求仍超限）或 `AfterModel` 用例 |
| 去掉 `lc.Request = retryReq` | `TestContextOverflowAfterModelSeesRetryRequest` |
| 把 `Release:false` 改成 `true` | `TestContextOverflowMidStepKeepsClaim` |
| 在重试前不写中途检查点 | `TestContextOverflowPersistsBeforeRetry` |
| 把 `attempts` 上限判断去掉 | `TestContextOverflowSecondFailureReturnsSecondError` |
| 恢复者结果不做严格更短检查 | `TestContextOverflowRequiresShrink` |
| 重跑 `ModifyRequest` | `TestContextOverflowModifyRequestRunsOnce`、`TestContextOverflowDoesNotDuplicateSystemRewrite` |
| 中途检查点失败后仍重试 | `TestContextOverflowCheckpointFailureDoesNotRetry` |

### CO-04 Compaction 恢复与端到端

**目标：**`Compaction` 实现 `ContextOverflowRecoverer`，在超限时强制摘要并落盘；正常阈值不受影响。

**先写的测试：**

1. `middleware/compaction_internal_test.go`（包 `middleware`）新增 `TestCompactionRecoveryTarget`：`before=100, limit=0 → 50`；`before=100, limit=200 → 50`（`before/2` 更小）；`before=100, limit=60 → 45`；`before=100, limit=40 → 30`；`before=1, limit=100 → 0`（由调用方放弃）。
2. `TestForceCut`：构造包含 assistant tool call + tool result 的历史，固定计数器；断言：
   - 目标允许时返回合法边界，且 `msgs[cut]` 不是 tool 结果；
   - 目标很紧时退到 `safeCut(msgs, 1)`；
   - 没有合法边界时返回 0。
3. `TestCompactionRecoverContext`：`Compaction{Model: summaryMock, MaxTokens: 1<<30, KeepRecent: 2, Counter: newCounter(10)}`（`newCounter` 让每条消息固定计 10，见 [middleware/window_internal_test.go:248](../../middleware/window_internal_test.go#L248)）；历史 8 条；`overflow.Limit=100`；调用 `RecoverContext`；断言结果更短、首条含 `summaryMark`、估算更小、返回 `true`。
4. `TestCompactionRecoverContextIgnoresNormalThreshold`：同上但 `MaxTokens` 很大、`KeepRecent` 很大；仍然恢复，证明恢复不看阈值/条数。
5. `TestCompactionRecoverContextRequestMode`：`Persist:false`；仍然恢复并发布 `HistoryCompacted`（用 bus 订阅）。
6. `TestCompactionRecoverContextRejectsNoShrink`：摘要内容很长或历史太短，使结果等长/估算不变；断言返回 `false` 且不发布事件。
7. `TestCompactionRecoverContextSummaryFailure`：摘要模型返回错误；断言返回 `false`。
8. `agent/overflow_test.go` 新增 `TestContextOverflowCompactionRecovery`：预置 8 条包含大文本的历史；在 `agent/overflow_test.go` 增加局部 helper `perMessageCounter(n int) middleware.TokenCounter`（返回 `len(msgs) * n`，包内可访问）；`middleware.Compaction(Model: summaryMock, MaxTokens: 1<<30, KeepRecent: 2, Counter: perMessageCounter(10))`；模型第一次超限（`Limit=100`）、第二次成功；断言：
   - 摘要模型调用 1 次；
   - 事件顺序 `HistoryCompacted` 在 `ContextOverflowRetried` 之前；
   - 第二次请求首条包含 `summaryMark`，消息数更少；
   - 运行结束后 `store.Latest().State.Messages` 以摘要开头（落盘）；
   - 没有 orphan tool result。

**实现要求：**

- `middleware/compaction.go`：新增 `compactionRecoveryTarget`、`forceCut`、`RecoverContext`；抽取 `publishCompacted`；`CompactHistory` 改为调用它；`var _ agent.ContextOverflowRecoverer = (*compaction)(nil)`。
- 恢复路径不调用 `compact`，不检查 `maxTokens`/`keep`；摘要失败、边界为 0、结果不严格更短都返回 `(history, false)`。
- 恢复结果的头条是 `RoleUser`、正文以 `summaryMark` 开头，后缀保持原样。
- 事件字段和持久模式一致：`Dropped=cut`、`Kept=len(out)-1`、`EstTokens=before`、`Step=lc.Step`。
- 不改正常 `ModifyRequest`/`CompactHistory` 的行为。

**验证命令：**

```bash
go test ./middleware -run 'Test(CompactionRecovery|ForceCut|CompactionRecoverContext)' -count=1
go test ./agent -run 'TestContextOverflowCompaction' -count=1
go test ./middleware ./agent -count=1
go vet ./middleware ./agent
```

**完成标准：**单元测试和端到端测试全过；正常压缩测试（`TestCompaction*`）继续通过；`go vet` 无输出。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 恢复仍检查 `MaxTokens` 阈值 | `TestCompactionRecoverContextIgnoresNormalThreshold` |
| 在长度/估算检查之前发布事件 | `TestCompactionRecoverContextRejectsNoShrink` |
| 去掉 `forceCut` 的 `target/2` 条件 | `TestForceCut` |
| 恢复结果不检查估算变小 | `TestCompactionRecoverContextRejectsNoShrink` |
| 请求模式直接返回 `false` | `TestCompactionRecoverContextRequestMode` |
| 端到端不落盘（去掉循环的中途检查点） | `TestContextOverflowCompactionRecovery` 的最新快照断言 |

### CO-05 Window 恢复、注释与全量门禁

**目标：**`Window` 实现 `ContextOverflowRecoverer`，超限时做紧急尾部淘汰；事件策略名为 `"overflow_tail"`；补齐注释并跑全量门禁。

**先写的测试：**

1. `middleware/window_internal_test.go`（包 `middleware`）新增 `TestWindowRecoveryTarget`：`before=100, limit=0 → 50`；`before=100, limit=200 → 50`；`before=100, limit=80 → 40`。
2. `TestKeepTailWithin`：构造 `user / assistant(tool call) / tool result / user / assistant` 历史；固定计数器；断言：
   - 目标足够大时返回原历史（`start <= 0`）；
   - 目标较紧时保留最新单元；最新单元自身超目标时仍然保留；
   - 返回值只含完整单元，不以 tool 结果开头。
3. `TestWindowRecoverContext`：溢出目标使结果变短；断言返回 `true`、消息更少、无 orphan、发布 `WindowTrimmed{Strategy:"overflow_tail"}` 且 `Dropped/Kept` 与修复后的结果一致。
4. `TestWindowRecoverContextRejectsNoShrink`：历史只有 2 条或最新单元已占满目标；断言 `false`、无事件。
5. `TestWindowRecoverContextRejectsEmptyAfterRepair`：历史只含孤立的 tool 结果；`repairPairing` 清空后断言 `false`、无事件。
6. `agent/overflow_test.go` 新增 `TestContextOverflowWindowRecovery`：预置包含 tool call/result 对的历史；`middleware.Window(Strategy: middleware.SlidingWindow(1<<30), Counter: perMessageCounter(10))`（正常路径永不裁剪）；模型第一次超限（`Limit=100`）、第二次成功；断言：
   - 事件顺序 `WindowTrimmed` 在 `ContextOverflowRetried` 之前；
   - `WindowTrimmed.Strategy=="overflow_tail"`；
   - 第二次请求更短、没有 orphan tool result；
   - 运行结束后最新检查点的消息是裁剪后的历史（加最终回复）。
7. `TestWindowRecoverIgnoresStrategyBudget`：`SlidingWindow(1<<30)` 或 `RecentN(100)`；恢复仍然发生，证明恢复不调用配置策略的预算。

**实现要求：**

- `middleware/window.go`：新增 `windowRecoveryTarget`、`estimate`、`keepTailWithin`、`RecoverContext`；`apply` 的计数闭包改为委托 `estimate`；`publishTrimmed` 拆成 `publishTrimmed`/`publishTrimmedAs`；`var _ agent.ContextOverflowRecoverer = (*window)(nil)`。
- 恢复不调用 `w.strategy.Select`，只复用 `groupUnits`/`keptMessages`/`repairPairing`。
- 空结果、等长、更长、估算不变都返回 `false`，且不发布事件。
- `WindowTrimmed` 的注释列出四个策略值；`HistoryCompacted` 的注释若 CO-02 没改，这里一并改完。
- 不改变正常 `ModifyRequest`/`CompactHistory` 的行为和三个策略的语义。

**验证命令：**

```bash
go test ./middleware -run 'Test(WindowRecovery|KeepTailWithin|WindowRecoverContext)' -count=1
go test ./agent -run 'TestContextOverflowWindow' -count=1
go test ./middleware ./agent ./core -count=1
go vet ./middleware ./agent ./core
```

**全量门禁（本卡最后一步）：**

```bash
gofmt -l agent/loop.go agent/middleware.go agent/options.go agent/agent.go agent/failure_test.go agent/overflow_test.go llm/error.go llm/error_test.go llm/openaicompat/openaicompat.go llm/openaicompat/openaicompat_test.go llm/anthropic/anthropic.go llm/anthropic/anthropic_test.go middleware/retry.go middleware/middleware_test.go middleware/compaction.go middleware/compaction_internal_test.go middleware/window.go middleware/window_internal_test.go core/event.go core/eventjson.go core/stuckevent_test.go
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./agent ./middleware ./llm ./core -count=1
git diff --check
git status --short
```

`gofmt -l` 只允许列出既有的 CRLF 文件；新增文件必须是 LF 且不出现在输出里。不要为了消除 CRLF 告警重排整份既有文件。`git status --short` 用来确认提交只包含本卡列出的文件。

**完成标准：**单元和端到端测试全过；正常窗口测试（`TestWindow*`）继续通过；全仓 build/vet/test/race 通过；`gofmt` 和 `git diff --check` 无本卡引入的问题。

**反向验证：**

| 改错 | 预期失败 |
| --- | --- |
| 恢复直接调用 `w.strategy.Select` | `TestWindowRecoverIgnoresStrategyBudget` |
| 去掉 `repairPairing` | `TestWindowRecoverContext`（orphan 断言）或 `TestWindowRecoverContextRejectsEmptyAfterRepair` |
| 允许空结果返回 `true` | `TestWindowRecoverContextRejectsEmptyAfterRepair` |
| 事件策略名写成 `"sliding_window"` | `TestWindowRecoverContext`、端到端策略名断言 |
| 不检查估算变小 | `TestWindowRecoverContextRejectsNoShrink` |
| 最新单元也受目标限制 | `TestKeepTailWithin` |

## 7. 验收矩阵

| 编号 | 验收点 | 卡片 | 证据 |
| --- | --- | --- | --- |
| A1 | 400/413 超限升级为 `*ContextLengthError`，400/429/500 无标记不升级 | CO-01 | `TestNewStatusErrorClassifiesContextLength` |
| A2 | 错误串不变，旧的 `errors.As(*StatusError)` 仍可用 | CO-01 | `TestContextLengthErrorUnwrapsStatusError` |
| A3 | 超限不被 `IsRetryable`/`RetryModel` 重试 | CO-01 | `TestIsRetryableExcludesContextLength`、`TestRetryModelDoesNotRetryContextLength` |
| A4 | 两个提供方实际走分类器 | CO-01 | 两个 `TestGenerateClassifiesContextLength` |
| A5 | 新事件往返和 `omitempty` | CO-02 | `TestContextOverflowRetriedEventRoundTrip`、`TestContextOverflowRetriedEventOmitsUnknownFields` |
| A6 | 默认一次、同一步骤内恢复并重试 | CO-03 | `TestContextOverflowRecoversAndRetries` |
| A7 | 0/负数关闭 | CO-03 | `TestContextOverflowDisableOption`、`TestContextOverflowOptionNegativeIsDisabled` |
| A8 | 已输出不重试 | CO-03 | `TestContextOverflowDoesNotRetryAfterPartial` |
| A9 | 中途检查点先于第二次调用 | CO-03 | `TestContextOverflowPersistsBeforeRetry` |
| A10 | 中途检查点不释放租约 | CO-03 | `TestContextOverflowMidStepKeepsClaim` |
| A11 | 请求从视图重建，不重跑改写 | CO-03 | `TestContextOverflowModifyRequestRunsOnce`、`TestContextOverflowDoesNotDuplicateSystemRewrite`、`TestContextOverflowRunBudgetWrapUp` |
| A12 | `lc.Request` 与 `req` 都更新 | CO-03 | `TestContextOverflowAfterModelSeesRetryRequest` |
| A13 | 冻结失败、检查点失败不重试且错误合并 | CO-03 | `TestContextOverflowFreezeFailureDoesNotRetry`、`TestContextOverflowCheckpointFailureDoesNotRetry` |
| A14 | 无恢复者/不缩短不重试 | CO-03 | `TestContextOverflowNoRecovererFails`、`TestContextOverflowRequiresShrink` |
| A15 | 多轮恢复与第二次失败语义 | CO-03 | `TestContextOverflowSecondFailureReturnsSecondError` |
| A16 | 取消/超时不恢复 | CO-03 | `TestContextOverflowCanceledContextDoesNotRecover` |
| A17 | Compaction 恢复、事件顺序、落盘 | CO-04 | `TestCompactionRecoverContext*`、`TestContextOverflowCompactionRecovery` |
| A18 | Window 恢复、配对修复、策略名、落盘 | CO-05 | `TestWindowRecoverContext*`、`TestContextOverflowWindowRecovery` |
| A19 | 全仓 build/vet/test/race 通过 | CO-05 | 全量门禁命令 |

## 8. 风险、兼容性与测试环境

### 8.1 风险

| 风险 | 影响 | 处理 |
| --- | --- | --- |
| 提供方改写错误文案 | 标记匹配不到，超限退化为普通 400 | 标记表集中在一个函数；新增文案只需加标记和用例；未识别时行为与今天一致 |
| 通用兼容层数字不可靠 | 不填数字，恢复目标只能用 `before/2` | 明确接受；第二次失败返回类型化错误，不做猜测 |
| 摘要请求本身超限 | `Compaction` 恢复失败 | 恢复返回 false；建议同时安装 `Window` 作为后备，注册顺序 `Compaction` 在前 |
| 请求模式的压缩/窗口与工作历史不一致 | 恢复后的请求可能仍比失败请求长 | 设计稿 §5.4 已说明；需要精确恢复的部署用持久模式或自定义恢复者 |
| 额外的摘要调用/延迟 | 每次恢复可能多一次模型调用 | 默认只恢复一次；`0` 关闭；恢复前先检查类型、取消、已输出和预算 |
| 事件消费者不认识新类型 | 跨进程反序列化失败 | 事件类型字符串新增；部署时同步升级消费者；旧事件编码不变 |
| 同一步骤两个检查点 | 外部观察者看到同一步骤号的第二个快照 | 这是「先落盘再重试」的要求；中途检查点没有 `Pending`/`Pause`，`Resume` 按普通快照恢复 |
| `OnError` 看到两次错误 | 依赖「一次失败只回调一次」的中间件可能误判 | 既有语义就是每次模型调用失败回调一次；不新增回调、不吞掉第一次 |
| 既有测试用 `newLoop` 绕过默认值 | 恢复测试永远不触发 | 用公开 `agent.New`；必要时显式设置字段 |
| Windows CRLF | `gofmt -l` 标记既有文件 | 只格式化本卡改动的文件；新文件用 LF |

### 8.2 兼容性

- 检查点结构、`StepSnapshot`、`CommitRequest` 都不变；不需要数据迁移。
- `StatusError` 仍是公开类型，直接构造仍然有效；新代码推荐 `NewStatusError`。
- `streamModel` 是未导出方法，签名变化不影响外部。
- `WithContextOverflowRetry` 是新增选项，默认行为是「恢复一次」，不设置该选项的现有程序会看到行为变化；这是本计划的默认值决定，发布说明需要写清楚。希望保持旧行为的部署显式传 `0`。
- 事件是新增类型，JSON 字段 `omitempty`；旧事件的字节编码不变。
- `Compaction`/`Window` 的正常路径不变；恢复只在类型化超限且满足条件时发生。
- `Resume` 语义不变：中途检查点是一个没有 `Pending`/`Pause` 的普通快照，恢复运行仍从新的 step 0 开始。

### 8.3 测试环境注意

- 恢复的集成测试必须预置历史，见 §2.1 第 7 条。
- `checkpoint.Memory` 会走 `BeginRun` 并持有租约；观察 `ReleaseClaim` 用只嵌入 `Durable` 的包装。
- `phase1FailOnSaveStore` 走 `Save`，可以用来数检查点次数。
- `scriptModel` 不能先 partial 后 error；用局部模型。
- `RunBudget` 收尾测试要设置上限并用 `WithMaxTurns(1)`。
- `ContextLengthError` 的测试夹具用 `NewStatusError` 或显式嵌入 `*StatusError`。
- Windows 上既有文件可能是 CRLF；只处理本卡新增/修改的文件。新文件用 LF。
- `-race` 在 Windows 需要可用的 C 工具链；如果本机不满足，先修环境，不要用跳过代替验证。

## 9. 明确不做的事

- 不在已经输出过任何增量或最终消息后重试；半条回复不修补。
- 不做跨提供方切换、模型降级或备用端点。
- 不做无限恢复；默认一次，选项上限由调用方设置。
- 不按错误字符串在 `agent`/`middleware` 里二次识别超限；分类只在 `llm.NewStatusError`。
- 不把系统提示和工具 schema 计入恢复目标；目标使用校准后的历史估算，系统提示/schema 的偏差由校准因子吸收。
- 不改正常 `Compaction` 的 `MaxTokens`/`KeepRecent` 语义，不改 `Window` 的三个策略。
- 不新增检查点字段、不把恢复信息写进 `StepSnapshot`、不改检查点 JSON。
- 不新增 `LoopContext` 的重试计数字段；计数是步骤局部变量。
- 不重跑 `BeforeModel`、`ModifyRequest`、`CompactHistory`、steering/inbox、工具执行。
- 不发布 `TurnStarted`/`TurnDone` 来表示恢复。
- 不改变 `Resume`、HITL、审批、租约和 `fail` 的既有语义。
- 不处理非 400/413 的超限编码；429/5xx 仍按普通可重试状态处理。
- 不新增真实提供方的网络测试；提供方测试使用 `httptest`。
- 不改 UI、示例、README；除非用户另行要求，不新增 ADR。

## 10. 执行记录与反向验证记录

实施时逐卡填写。没有实测的输出不要预先写；只记录实际命令和实际结果。

### 10.1 执行记录

| 任务卡 | 提交 | 验证命令 | 结果 | 备注 |
| --- | --- | --- | --- | --- |
| CO-01 | `3358a8c` | `go test ./llm -run 'Test(NewStatusError\|ContextLengthError\|IsRetryable)' -count=1`、`go test ./llm/... -count=1`、`go test ./middleware -run 'TestRetryModel' -count=1`、`go vet ./llm/... ./middleware` | 全过 | 另跑 `go build ./...`、`go vet ./...`、`go test ./... -count=1` 全绿；`gofmt -l` 对改动文件无输出 |
| CO-02 | `004941e` | `go test ./core -run 'Test(ContextOverflowRetried\|HistoryCompacted\|WindowTrimmed\|StepViewDrifted)' -count=1`、`go test ./core -count=1`、`gofmt -l core/event.go core/eventjson.go core/stuckevent_test.go` | 全过 | 另跑 `go build ./...`、`go vet ./...`、`go test ./... -count=1` 全绿；额外加未知类型回归用例 |
| CO-03 | `9d1f8b9` | `go test ./agent -run 'TestContextOverflow' -count=1`、`go test ./agent -count=1`、`go test -race ./agent -run 'TestContextOverflow' -count=1`、`go vet ./agent` | 全过：16 个顶层用例（18 个 RUN 含 2 个子用例） | 另跑 `go build ./...`、`go vet ./...`、`go test ./... -count=1` 全绿；`gofmt -l` 对 6 个改动文件无输出；`git diff --check` 无输出 |
| CO-04 | `4ba413a` | `go test ./middleware -run 'Test(CompactionRecovery\|ForceCut\|CompactionRecoverContext)' -count=1`、`go test ./agent -run 'TestContextOverflowCompaction' -count=1`、`go test ./middleware ./agent -count=1`、`go vet ./middleware ./agent` | 全过：middleware 7 个顶层用例（9 个 RUN 含 2 个子用例）+ agent 1 个端到端用例 | 另跑 `go build ./...`、`go vet ./...`、`go test ./... -count=1` 全绿；`gofmt -l` 对 3 个改动文件无输出；`git diff --check` 无输出；端到端用例在第二次模型调用内部读取最新检查点，使「恢复后先落盘再重试」可观测 |
| CO-05 | `9e5df25` | `go test ./middleware -run 'Test(WindowRecovery\|KeepTailWithin\|WindowRecoverContext)' -count=1`、`go test ./agent -run 'TestContextOverflowWindow' -count=1`、`go test ./middleware ./agent ./core -count=1`、`go vet ./middleware ./agent ./core` | 全过：middleware 7 个 RUN（5 个顶层用例 + 2 个子用例）+ agent 1 个端到端用例 | `TestWindowRecoverIgnoresStrategyBudget` 不在卡片正则内，另行单跑与包级运行均通过；`gofmt -l` 对 21 个文件无输出；`git diff --check` 无输出；CO-02 已更新 `HistoryCompacted`/`WindowTrimmed` 注释，本卡未再改 `core/event.go` |
| 全量门禁 | `9e5df25` | `gofmt -l agent/loop.go agent/middleware.go agent/options.go agent/agent.go agent/failure_test.go agent/overflow_test.go llm/error.go llm/error_test.go llm/openaicompat/openaicompat.go llm/openaicompat/openaicompat_test.go llm/anthropic/anthropic.go llm/anthropic/anthropic_test.go middleware/retry.go middleware/middleware_test.go middleware/compaction.go middleware/compaction_internal_test.go middleware/window.go middleware/window_internal_test.go core/event.go core/eventjson.go core/stuckevent_test.go`、`go build ./...`、`go vet ./...`、`go test ./... -count=1`、`go test -race ./agent ./middleware ./llm ./core -count=1`、`git diff --check`、`git status --short` | 全过（exit 0） | `gofmt -l` 无输出；全量测试 agent 13.628s、core 0.321s、llm 0.382s、middleware 0.732s；race 全绿（agent 18.673s、middleware 1.784s、llm 1.209s、core 1.227s）；提交只含本卡 3 个文件 |

### 10.2 反向验证记录

| 卡片 | 故意改错 | 预期失败的用例 | 实测结果 | 还原方式 |
| --- | --- | --- | --- | --- |
| CO-01 | 删掉 `IsRetryable` 的超限前置判断 | `TestIsRetryableExcludesContextLength` | FAIL：`context length error must not be retryable` | Edit |
| CO-01 | 删掉 `RetryModel` 的超限判断 | `TestRetryModelDoesNotRetryContextLength` | FAIL：`model calls = 2, want 1` | Edit |
| CO-01 | 删掉 `Unwrap` | `TestContextLengthErrorUnwrapsStatusError` | 改为 `return nil`（等效断链，避免编译失败）；FAIL：`errors.As` 找不到嵌入的 `*llm.StatusError` | Edit |
| CO-01 | 状态门改成 `code < 400` | 429/500 两个子用例 | FAIL：两个子用例均为 `typed = true, want false` | Edit |
| CO-01 | 交换 Anthropic 的 `Limit`/`Requested` 赋值 | Anthropic 两个用例 | FAIL：两组数值颠倒 | Edit |
| CO-02 | marshal 漏掉 `Provider` | `TestContextOverflowRetriedEventRoundTrip` | FAIL：解码后 `Provider` 为空 | Edit |
| CO-02 | 去掉一个 `omitempty` | `TestContextOverflowRetriedEventOmitsUnknownFields` | FAIL：JSON 出现 `"model":""` | Edit |
| CO-02 | decode 漏掉 `MessagesBefore` | `TestContextOverflowRetriedEventRoundTrip` | FAIL：解码后 `MessagesBefore` 为 0 | Edit |
| CO-02 | 类型字符串写成 `context_overflow_retry` | `TestContextOverflowRetriedEventRoundTrip` | FAIL：`unknown event type "context_overflow_retry"` | Edit |
| CO-03 | 去掉 `emitted` 条件 | `TestContextOverflowDoesNotRetryAfterPartial` | FAIL：`model calls = 2, want 1 after a partial was emitted`。同时把调用处的 `emitted` 改成 `_` 以保持可编译 | Edit |
| CO-03 | 去掉 `genCtx.Err() == nil` | `TestContextOverflowCanceledContextDoesNotRecover` | FAIL：`ContextOverflowRetried events = 1, want none` | Edit |
| CO-03 | 去掉 `req = retryReq` | `TestContextOverflowRecoversAndRetries` | FAIL：`retry request has 7 messages, first had 7` | Edit |
| CO-03 | 去掉 `lc.Request = retryReq` | `TestContextOverflowAfterModelSeesRetryRequest` | FAIL：`AfterModel saw "user=message-0;...", want the retried request "user=message-1;..."` | Edit |
| CO-03 | `Release:false` 改成 `true` | `TestContextOverflowMidStepKeepsClaim` | FAIL：`release sequence = [true true], want [false true]` | Edit |
| CO-03 | 跳过中途检查点 | `TestContextOverflowPersistsBeforeRetry` | FAIL：`the retry reused the seeded checkpoint instead of committing a new one` | Edit |
| CO-03 | 去掉 `attempts` 上限判断 | `TestContextOverflowSecondFailureReturnsSecondError` | FAIL：`model calls = 7, want 3`（继续恢复直到无法再缩短） | Edit |
| CO-03 | 恢复者结果不做严格更短检查 | `TestContextOverflowRequiresShrink` | FAIL：两个子用例均 `model calls = 2, want 1` | Edit |
| CO-03 | 重跑 `ModifyRequest` | `TestContextOverflowModifyRequestRunsOnce`、`TestContextOverflowDoesNotDuplicateSystemRewrite` | FAIL：`BeforeModel/ModifyRequest = 1/2, want 1/1`；第二次请求系统提示出现 2 个标记 | Edit |
| CO-03 | 中途检查点失败后仍重试 | `TestContextOverflowCheckpointFailureDoesNotRetry` | FAIL：`model calls = 2, want 1 when the mid-step checkpoint fails` | Edit |
| CO-04 | `RecoverContext` 增加 `before <= c.maxTokens` 提前返回 | `TestCompactionRecoverContext`、`TestCompactionRecoverContextIgnoresNormalThreshold`、`TestCompactionRecoverContextRequestMode` | FAIL：三个用例分别报 `recovery did not run`、`recovery must ignore MaxTokens and KeepRecent`、`request mode must still recover on overflow` | Edit |
| CO-04 | 把 `publishCompacted` 挪到长度/估算检查之前 | `TestCompactionRecoverContextRejectsNoShrink` | FAIL：两个子用例均 `a rejected recovery published 1 events` | Edit |
| CO-04 | 去掉 `forceCut` 的 `<= target/2` 估算条件 | `TestForceCut` | FAIL：`forceCut = 1, want 4` | Edit |
| CO-04 | 去掉结果估算必须变小的检查（保留长度检查） | `TestCompactionRecoverContextRejectsNoShrink` | FAIL：`summary_too_long` 报 `an oversized summary must be rejected`；`history_too_short` 仍由长度条件拦住 | Edit |
| CO-04 | 请求模式（`Persist:false`）直接返回 false | `TestCompactionRecoverContextRequestMode`、`TestContextOverflowCompactionRecovery` | FAIL：前者 `request mode must still recover on overflow`；后者以原始超限错误结束 | Edit |
| CO-04 | 跳过恢复后的中途检查点 | `TestContextOverflowCompactionRecovery` | FAIL：`the retry did not persist the recovered history before calling the model` | Edit |
| CO-05 | 恢复调用策略 `Select` | `TestWindowRecoverIgnoresStrategyBudget` | FAIL：`recovery must not consult the configured strategy budget` | Edit |
| CO-05 | 去掉 `repairPairing` | `TestWindowRecoverContext`（orphan 断言）或 `TestWindowRecoverContextRejectsEmptyAfterRepair` | FAIL（后者）：`an empty result after pairing repair must not be returned` | Edit |
| CO-05 | 事件策略名写成 `"sliding_window"` | `TestWindowRecoverContext`、端到端策略名断言 | FAIL：middleware 报 `event = {Strategy:sliding_window Dropped:3 Kept:2 EstTokens:50 Step:2}, want overflow_tail, Dropped 3, Kept 2, EstTokens 50, Step 2`；`TestContextOverflowWindowRecovery` 同时因事件结构不符失败 | Edit |
| CO-05 | 去掉 `len(out) == 0` 检查 | `TestWindowRecoverContextRejectsEmptyAfterRepair` | FAIL：`an empty result after pairing repair must not be returned` | Edit |
| CO-05 | 去掉 `after >= before` 检查 | `TestWindowRecoverContextRejectsNoShrink`（`estimate does not shrink` 子用例） | FAIL：`a history whose estimate does not shrink must not recover`。`after` 会因此未使用，补 `_ = after` 保持可编译后复跑 | Edit |
| CO-05 | 最新单元也受目标限制 | `TestKeepTailWithin` | FAIL：`target 5 kept [] with 0 messages, want only the newest unit` | Edit |

还原一律用 Edit 按原内容改回，不要用 `git checkout --` 抹掉同文件里其它未提交改动。还原后执行对应测试确认恢复绿色，再执行 `git diff --check` 确认没有残留的改错痕迹。
