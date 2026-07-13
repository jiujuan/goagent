# ADR 0008：LLM 驱动委派——transfer 作为自动注入的工具

状态：已接受

## 背景

adk-go 的 LLM 驱动委派不是静态注册的工具，而是当 agent 满足条件（有子 agent、允许
转移）时**自动注入**一个 `transfer_to_agent` 工具，其 `agent_name` 参数被 enum 约束
为合法目标；工具执行时设置 `Actions.TransferToAgent`，Flow 检测到后转交目标 agent。

## 决策

复用本框架已有的"事件Event 即 Actions"机制（ADR 0004）实现委派：

- 当 `LLMAgent` 有 `SubAgents` 且未 `DisableTransfer` 时，turn 引擎在广告工具列表里
  追加一个合成的 `transfer_to_agent` 工具（schema 的 `agent_name` 带 `enum`）。
- 该工具的 `Call` 把 `ctx.Actions.TransferToAgent = agent_name`。
- `execTools` 把各工具的 `Actions` 合并（`mergeActions`）；turn 引擎检测到
  `TransferToAgent` 后，定位目标子 agent，转交其 `Run` 流并结束本 agent 的 turn。

委派与确定性编排（`Sequential`/`Parallel`/`Loop`）、agent-as-tool 并存，统一在同一棵
Agent 树下。

## 理由

- **零新机制**：委派复用 Actions + 工具执行管线，没有为它发明独立通路。
- **类型约束**：enum 限定合法目标，减少模型乱填。
- **自然衔接**：转交后目标 agent 读取同一 session 历史（含转交前的上下文），无缝接管。

## 方向规则与返回父 agent（已实现）

委派目标不再局限于子 agent。每个 LLMAgent 通过 `InvocationContext.Root`（由 Runner 注入）
定位自己在树中的位置，按方向规则计算合法目标：

- **子 agent**：始终可转交（委派开启时）。
- **父 agent**：除非 `DisallowTransferToParent`——支持"把控制交还上级"（返回父 agent）。
- **同级 peer**：除非 `DisallowTransferToPeers`，且**父 agent 自身也具备委派能力**（否则
  peer 交接会悬空）。

合法目标既用于构建 `transfer_to_agent` 的 `enum`（附"上级/下级/同级"方向提示），也用于
**转交时的校验**——只接受落在允许集合内的目标名，模型填了越权目标则忽略、令其重试。

防环：`InvocationContext.transferDepth` 每次转交 +1，超过 `maxTransferDepth`（8）即停止，
避免模型诱导的 parent↔child 无限 ping-pong。

## 后果

- 转交仍是"接管式"：目标 agent 接手并产出后，控制经事件流向上返回；配合方向规则即可表达
  "交还父级"。
- `transfer_to_agent` 是保留工具名；用户工具不应重名。
- 方向规则依赖 agent 名全局唯一（树内按名定位父/同级）。

## 备选方案

- **静态注册 transfer 工具**：要求用户手动连线，易错且与"有子 agent 即可委派"的直觉
  不符。
- **独立的委派信令通道**：与事件/Actions 模型重复，增加复杂度。
