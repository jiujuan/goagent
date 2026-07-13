# ADR 0001：用 `iter.Seq2[*Event, error]` 作为唯一流式原语

状态：已接受

## 背景

三个参考框架各有不同的流式机制：pi 用事件订阅 + AsyncGenerator，langchaingo
用 streaming 回调，adk-go 用 Go 1.23 的 range-over-func 迭代器 `iter.Seq2`。
我们需要一个能贯穿 runner → agent → turn 引擎 → model 各层、且天然支持取消和
背压的统一原语。

## 决策

定义 `core.Stream = iter.Seq2[*core.Event, error]`，每一层都返回它。消费者用
`for ev, err := range stream { ... }` 驱动；`break` 或提前 `return` 会让 `yield`
返回 `false`，从而干净地停止上游生产。

## 理由

- **单一心智模型**：所有层同构，组合直接（`core.Pipe` 转发子流）。
- **背压与取消免费**：pull 模型下，消费者不取，生产者就阻塞；无需额外的 channel
  或 context 协调。
- **Go 1.23 原生**：无需第三方库，零依赖。
- 相比 channel：无需手动管理关闭、泄漏、select；相比回调：控制流是线性的、可读。

## 后果

- 要求 Go ≥ 1.23。
- 生产者必须正确处理 `yield` 返回 `false`（提前停止），否则会在消费者退出后继续
  跑。内建实现都遵守此约定。
- 错误与正常事件走同一条流（见 ADR 0004），订阅者无需为错误单独开通道。

## 备选方案

- **channel**：更灵活但样板多、易泄漏。
- **回调**：简单但控制流割裂，难以组合多层。
