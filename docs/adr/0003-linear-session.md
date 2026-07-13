# ADR 0003：线性 append-only 会话（会话树作为后续可选扩展）

状态：已接受

## 背景

pi 把会话建模为带可移动 leaf 指针的**树**，支持分支、fork、重新摘要——非常强大，
但实现复杂（路径回溯、leaf 迁移、分支摘要）。adk-go 用**线性** append-only 的事件
列表 + `Branch` 字段做并行子 agent 的历史隔离——简单稳健。

本项目选择"线性优先"。

## 决策

`session.Session` 是一个有序的、append-only 的 `[]*core.Event` 加一个 key/value
`State`。`Store` 接口只有两个方法：`GetOrCreate` 和 `Append`。`Append` 提交事件时
事务性地应用 `Actions.StateDelta`（见 ADR 0004），并跳过 `temp:` 作用域的状态键。

并行场景下的历史隔离用 `Event.Branch` 字段表达（`ParallelAgent` 为每个子 agent
赋予 `parent.child` 分支名），而非真正的树。

## 理由

- **简单**：线性日志易于实现、推理、持久化（一个 JSONL 文件即可）。
- **够用**：绝大多数 agent 场景是线性对话；分支是高级需求。
- **可演进**：树可以作为另一个 `Store` 实现 + 会话视图后续引入，而不破坏现有
  `Store`/`Session` 契约。

## 后果

- 暂不支持"回到某个历史节点 fork 新分支"。需要时通过新增 `TreeStore` 扩展。
- 状态作用域前缀（`app:`/`user:`/`temp:`）已预留语义，但当前 `InMemoryStore`
  只实现了 `temp:` 的丢弃；`app:`/`user:` 的跨会话共享留待持久化后端实现。

## 备选方案

- **一开始就做会话树**：能力更强但前期复杂度高，违背"易用、先跑通"的目标。
