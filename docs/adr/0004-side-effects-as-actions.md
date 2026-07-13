# ADR 0004：事件Event 建模为 `Event.Actions`，由 runner 事务提交

状态：已接受

## 背景

adk-go 的核心设计：所有状态变更（state delta、artifact、agent 转移、循环升级）
都挂在 `Event.Actions` 上，由 Runner 在提交非 partial 事件时**事务性应用**。这让
运行可重放、可审计，且组件之间不通过共享可变状态耦合。

## 决策

`core.Event` 携带一个 `Actions` 字段，包含 `StateDelta`、`TransferToAgent`、
`Escalate`、`Stop` 等声明式事件Event。组件（agent、tool）不直接改 session 状态，而是
把意图写进 `Actions`。runner 在 `store.Append` 时统一应用 `StateDelta`。错误也作为
`Event.Err` 走同一条流（统一错误流，吸收自 pi）。

工具通过 `tool.Context.Actions` 累积事件Event；turn 引擎把它折叠进产生的事件。

## 理由

- **可重放/可审计**：事件日志 + 声明式 actions = 完整、确定的历史。
- **解耦**：组件不持有对 session 的写权限，避免竞态与隐式耦合。
- **统一控制信号**：`Escalate`（退出 Loop）、`TransferToAgent`（委派）、`Stop`
  都用同一机制，扩展新信号只是加字段。

## 后果

- 事件Event 是"延迟"的：写进 `Actions` 后要等 runner 提交才生效。组件内若需立即读到
  自己刚写的状态，需自行处理（当前 tool 直接拿到 `State` 可即时读写，actions 用于
  需要随事件持久化的语义）。
- partial 事件不提交，因此其 actions 不生效——这是有意的（增量不应产生事件Event）。

## 备选方案

- **组件直接改 session**：简单但难审计、易竞态、难重放。
