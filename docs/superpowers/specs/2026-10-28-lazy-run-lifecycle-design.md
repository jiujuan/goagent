# 惰性 Run 的启动、观察、取消与终结设计

- 日期：2026-10-28
- 状态：Proposed（独立模型设计复核通过，待实施）；本文件是技术方案，不代表代码已经修复。
- 基线：5e7b603（以本轮读取的工作区实现为准）。
- 范围：3 个 P0、3 个 P1、1 个 P2。普通 Stream 保留惰性执行；运行事实、事件交付与队列收尾分别表达。

## 1. 问题和优先级映射

上一轮表格实际为 2 个 P0、3 个 P1、1 个 P2。本轮将“三个优先问题”中的队列执行权问题单独提升为 P0，形成以下 7 项；没有把同一修复重复计算为两项。

| 编号 | 问题 | 方案摘要 |
| --- | --- | --- |
| P0-1 | Lossless 满缓冲时 Publish 与 unsubscribe 互锁；运行取消不能中断发送 | 总线锁只保护订阅索引；每订阅发送登记、停止信号和关闭协议；框架发送使用 PublishContext |
| P0-2 | 已结束 Run 的 Iter 永久等待；终态发送可以阻塞运行完成 | 订阅与终结共用观察锁；运行保存终态尾部；FinishTopic 将终态交付交给订阅收尾者 |
| P0-3 | queue.Handle.Wait 可绕过 worker 执行任务 | Run 提供纯等待；worker 独占启动入口；队列 Wait 只调用 Await |
| P1-1 | 启动、等待、订阅职责混杂 | 定义 Start/Await/Wait/Iter/Events 的完整契约和执行权 |
| P1-2 | Wait 为等待结果额外建立 Lossless 订阅，capture 参与结算 | 执行方独占结算；Wait 只做 Start + Await；统一冻结结果 |
| P1-3 | 未启动 Run 取消后没有完整结算；Resume 可能持有 gate/claim | 明确状态机和唯一结算者；父 context 取消同样结算；完成资源交接后才安装取消回调 |
| P2-1 | 启动时点、迟到订阅、退出、超时及兼容行为不清楚 | 补文档、示例、ADR 变更说明与必要的时间指标 |

P0-3 需要先引入最小纯等待方法；这属于必要依赖。P0 优先描述和交付，并不要求所有 P1 基础代码都等到全部 P0 完成后才出现。

## 2. 现有实现依据

- `agent/agent.go`：Stream 冻结输入，newRunHandleWithID 创建执行 context；历史恢复在 drive 后进行。
- `agent/run.go`：Iter 消费时订阅后 drive；Wait 当前也订阅并捕获事件；drive 使用 sync.Once 启动后台 goroutine。
- `bus/bus.go`：Publish 持有全局 RLock 阻塞发送，取消订阅需要全局写锁。
- `agent/hitl.go`：Resume 返回前已获取线程名额，并可能持有 durable claim。
- `queue/agent_bridge.go`：Handle.Wait 委托 run.Wait；worker 闭包也通过 run.Wait 驱动执行。
- `agent/terminal.go`：RecordCompletion 使用冻结的本次提交边界；无本次边界返回 ErrNoCommittedBoundary。
- ADR-0033 D5 明确允许队列 Wait 驱动运行。P0-3 是有意修订这一行为，不应作为没有兼容影响的内部重构发布。

此前临时探针已复现：完成后 Iter 阻塞；Cancel 无法唤醒该迟到订阅；满缓冲下发布及取消订阅均阻塞；无 worker 的队列任务能被 Handle.Wait 启动。临时探针不计作已经建立的仓库回归测试。

## 3. 方案比较与选择

| 方案 | 收益 | 代价 | 结论 |
| --- | --- | --- | --- |
| 保留惰性入口，分离执行、等待与观察 | 可兼容常见 Run/Stream 用法；允许队列保有执行权 | 需要明确生命周期及订阅关闭协议 | 采用 |
| Stream 立即启动 | 调用后即执行，部分用法更直观 | 首批事件订阅竞态、额外缓存、旧代码副作用时点变化；不能单独修复总线互锁 | 本轮不采用 |
| 只支持 Prepare → Subscribe → Start → Await | 启动权最明确 | 所有调用方都承担显式启动步骤，迁移较大 | 为 Start/Await 提供这种用法，但保留便利入口 |

必须同时满足：

1. 运行结果只由执行或取消的结算者产生，不能依赖消费者捕获终态。
2. 普通 Lossless 事件允许背压；订阅取消和运行 context 取消必须能够打断这种等待。
3. 框架发送根 TaskDone 和 RunDone/RunFailed/Interrupted 的终态尾部不再等待订阅者腾出缓冲。workflow 分支和 plan 节点的 TaskDone 仍是普通、可取消的背压事件；不能对子任务封闭共享 topic。
4. 对没有取消订阅、且继续消费的观察者，已入队事件后仍能收到一次有序终态；Lossless 不因终结而静默丢掉已入队事件。
5. Await/Wait 返回不表示所有观察者已读到终态，也不表示 RecordCompletion 已完成。
6. 不引入无界事件日志；不提供历史事件回放，只缓存当前 Run 的终态尾部。

普通事件的背压仍可能使执行尚未完成；本方案只消除不可取消的等待，以及框架终态交付对结算的依赖。用户模型、工具、FinishRun hook 或自定义 store 若忽略 context，本方案不声称能强制抢占其执行。

## 4. P0-1：总线发送、取消与关闭协议

### 4.1 接口

保留现有调用，同时新增带 context 的发布方式：

```go
func (b *Bus) Publish(topic Topic, ev core.Event)
func (b *Bus) PublishContext(ctx context.Context, topic Topic, ev core.Event) error
```

Publish 是 Background context 的兼容包装。框架内所有可能背压的发布点迁移到 PublishContext，包括 RunStarted、TaskStarted、RunContext.publish 及其派生执行上下文。RecordApproval 的持久化不依赖观察事件成功；活动运行的通知使用调用方 context 和运行终结检查，已终结运行不重开旧事件流。

PublishContext 的约定：

- context 取消返回 ctx.Err()；不把一个观察者的退订升级为运行错误。
- Lossy 在满缓冲时仍允许丢普通事件。
- 某订阅已关闭、正在收尾，或快照取得后恰好退订时，跳过该订阅。
- fan-out 可能部分完成后遇到 context 取消，不承诺原子广播；调用方不能据此盲目重发并假设没有重复。
- 并发发布者的相对顺序不新增保证；既有按调用方串行发布的顺序继续保持。

### 4.2 发送与取消算法

Bus.mu 只保护 topic → subscribers 的索引。PublishContext 复制订阅指针快照后释放 Bus.mu，再逐订阅发送。

每个订阅有短临界区的状态锁、普通发送计数、sendStop 和 cancelSignal。普通发送必须在订阅状态锁内完成“检查仍为 active + normalWG.Add(1)”；随后解锁，执行可取消发送，最后 normalWG.Done。

阻塞发送同时观察：

```go
select {
case ch <- ev:
case <-ctx.Done():
case <-sendStop:
}
```

取消订阅的步骤：

1. 在订阅状态锁内禁止新发送，并关闭 sendStop 与 cancelSignal；不能在这里做 I/O 或等待。
2. 释放订阅锁，移除总线索引；不同时持有总线锁和订阅锁。
3. 在锁外等待已登记普通发送退出；若终态收尾者已安装，还要等待它退出。
4. 由唯一关闭者关闭数据 channel。取消函数返回时，该订阅不再有在途发送，channel 已关闭。

关键约束：禁止新发送与发送登记使用同一把订阅锁，因此开始 Wait 之后不会再有新的 normalWG.Add。不能仅对 ch 的 send/close 使用两个互不协调的 sync.Once，也不能用 recover 掩盖 send on closed channel。

sendStop、cancelSignal 和数据 channel 都只能各关闭一次。状态锁内只有 active → sealing/canceling 的第一次转换能关闭 sendStop；sealing → canceling 只补充关闭尚未关闭的 cancelSignal，不重复 close(sendStop)。信号的单次关闭由状态和同锁标记保证，不把 sync.Once 当成发送登记协议。取消请求与已有发送可能同时发生：已经登记的发送允许在取消函数返回前完成；取消返回后不再发送。缓冲里已有事件可继续被读取，取消不承诺清空历史缓冲。

### 4.3 验收

- 容量为 1、无人读取时，第二次发布阻塞，unsubscribe 能解除发布阻塞并返回。
- context 取消能够解除满缓冲中的 PublishContext。
- 一个 topic 的慢订阅不会持有总线锁阻止其他 topic 订阅或退订。
- Publish/Subscribe/unsubscribe 的并发压力测试无数据竞争、重复关闭或发送到已关闭 channel；特别覆盖 FinishTopic、unsubscribe、已登记普通发送的三方竞争。
- 保留 Lossy 丢弃和普通 Lossless 背压行为。

## 5. P0-2：结束后迭代与可靠终态交付

### 5.1 订阅语义

Iter 与 Events 都通过 Run 的内部 subscribeRun 方法建立订阅；subscribeRun 与 sealEvents 共用观察锁 observationMu。

- 未封闭：在 observationMu 内注册总线订阅；注册完成后再允许启动。
- 已封闭：返回装有缓存终态尾部的有限 channel，填入后关闭；不重新注册总线订阅。
- 活动运行的迟到订阅只接收加入后的事件及最终尾部，不补发此前进度。
- 已封闭运行只提供尾部，不重放 RunStarted、模型增量和工具进度。
- 相同或不同 Iter 多次消费都不会重新执行；每个订阅最终收到自己的终态副本并结束。

终态尾部由执行方构造。执行已进入根 Task 时为根 TaskDone + 单个 RunDone/RunFailed/Interrupted；准备阶段失败、未启动即取消等尚无 TaskStarted 的路径只提供 RunFailed，避免伪造 TaskDone。未启动取消不发布 RunStarted。

### 5.2 总线终结接口

```go
func (b *Bus) FinishTopic(topic Topic, tail ...core.Event)
```

只用于拥有唯一终结者的临时运行 topic。算法如下：

1. 在 Bus.mu 内摘除该 topic 当前的订阅集合，然后立即释放锁。
2. 对每个订阅，在其状态锁内转换 active → sealing，禁止新普通发送，关闭 sendStop，并安装唯一 tailDone。随后释放锁。
3. 启动该订阅的收尾者。收尾者先等待已登记的普通发送退出，再依序发送 tail，最后关闭数据 channel 和 tailDone。
4. 尾部发送监听 cancelSignal；取消订阅可打断收尾者，即使消费者从未继续读取，也能结束收尾。
5. FinishTopic 不等待普通发送、消费者接收或尾部发送完成。

订阅取消与 sealing 使用相同状态锁。取消先获胜时，不再安装收尾者；sealing 先获胜时，取消负责唤醒并等待已安装的收尾者。普通发送计数只覆盖普通发送；tailDone 单独协调尾部发送，避免收尾者等待包含自身的 WaitGroup。

数据 channel 的唯一关闭协议必须覆盖两条路径：无收尾者的取消路径，以及已安装收尾者的自然/取消退出路径。后者由收尾者关闭 channel，取消调用者等待 tailDone，不能并发直接 close(ch)。

这一设计最多为每个正在收尾的订阅增加一个 goroutine。普通运行期间不增加订阅转发 goroutine；尾部只有固定的 1～2 个事件，不复制整段历史。不读取也不取消的观察者可能保留自己的收尾 goroutine，这是保留有限缓冲及不丢终态的代价，必须在 Events 文档中要求 defer unsubscribe。

Lossy 只允许丢普通事件，终态尾部通过同一收尾协议交付。主动取消的订阅可以放弃尾部。事件内容应视为只读；Run 缓存与订阅得到的含可变消息/切片的数据必须隔离，不能让事件持有者改写冻结结果。

Bus 本身不存储终结 tombstone，以免每次运行永久增加一条 topic 记录。generic Bus.Subscribe 在 FinishTopic 后仍没有回放能力；Run 通过 observationMu 和自身缓存保证迟到观察者行为。

### 5.3 运行结算顺序

运行结算者执行以下顺序，任何资源 I/O 都不能放在生命周期锁或观察锁内：

1. 取得唯一结算权，关闭 steering；捕获本次运行已提交的 checkpoint boundary。
2. 停止并释放 durable claim，再释放 admission gate；释放错误进入最终 outcome。这一顺序也覆盖 prepare 失败，不能被 prepare 自己的 defer 提前释放 gate 绕过。
3. 完成所有可能失败的结果规范化、冻结所需深拷贝，以及 hook 参数副本准备；合并 cleanup/clone 错误，确定最终 outcome，再冻结结果、身份及 checkpoint boundary。构造终态尾部所需的可变数据也在此阶段准备；失败时转为零结果的失败结局，不能在 hook 已收到成功后再改判失败。
4. 按既有调用条件调用一次 FinishRun，参数来自最终 outcome 的独立副本。未发生错误的暂停仍不调用运行完成 hook，不给准备失败路径额外增加原先没有的 hook。hook 修改其参数不能改变冻结结果；不为每个 hook 重新执行可能改变 outcome 的判定。
5. 从已冻结的数据取得终态尾部，在 observationMu 内设置事件封闭标记和缓存尾部，并调用不等待消费的 FinishTopic。
6. 在生命周期锁内取出并清空已登记的父 context 停止函数，在锁外调用它并释放运行 context；然后在同一短临界区将状态置为 Settled、写结果并关闭 done。不得等待观察者收到尾部，也不得在关闭 done 前等待取消回调退出。

这样观察者即使先看到终态，也能读到已冻结的结果；Wait 则可以在慢观察者收到终态前返回。普通阶段的发布必须采用 P0-1 的可取消协议，否则取消仍无法让执行到达这里。尤其是子任务 TaskDone 满缓冲时，根 runnable 可以尚未返回；它必须能被运行取消唤醒，而不是提前封闭根 topic。

### 5.4 验收

- Wait 成功、失败或暂停后调用 Iter，均有限结束，并得到对应终态；没有第二次模型或工具执行。
- 订阅与封闭通过测试屏障竞态执行，每次都进入“正常注册后终结”或“直接返回缓存尾部”之一。
- 迭代在普通事件或终态事件处 break，都能完成退订。
- 缓冲满时，运行 done 可以关闭；消费者随后继续读取，能按顺序读到已入队事件和单个终态。
- 收尾者正等待缓冲时 unsubscribe 能退出，无残留收尾 goroutine。
- 验证 Lossy 进度可以缺失，但未退订且继续消费的观察者仍收到终态。
- Run 被释放后，Bus 不保留该 topic 的永久终结记录。
- workflow/plan 的子 TaskDone 可以继续背压，但取消后发布者能退出，根运行随后完成结算；子任务不得调用 FinishTopic。
- 注入结果冻结失败，以及在 FinishRun 中修改其结果参数，验证 hook、Await、FrozenOutcome 和终态事件采用相同结局；hook 参数修改不污染冻结结果。
- A 已持有 claim、prepare 失败、claim 释放尝试仍被屏障阻塞时，B 不得进入 BeginRun；释放尝试结束后才交接 gate。

## 6. P0-3：队列执行权归 worker

Handle.Wait 改为纯等待底层运行，worker 使用显式启动：

```go
// 示意契约，不是本轮已实现的代码。
func (h *Handle) Wait() (core.Result, error) {
    return h.run.Await(context.Background())
}

// 仅在 worker 已取得执行名额后进入。
h.run.Start()
_, runErr := h.run.Await(context.Background())
```

worker 使用 Background 等待真正的执行结算；workCtx 只控制执行。不能因等待 context 取消就提前进行持久化，把取消请求误当作执行已停止。收尾结果仍从 FrozenOutcome 获取。

保持现有两种完成含义：

- Handle.Wait：只报告运行的执行结算；不能 dequeue、执行模型、运行工具或写终态记录。
- Handle.WaitRecorded：仅在 worker 尝试 RecordCompletion 并保存收尾结果后返回。
- 提交 context 只控制提交；已接受的后台运行继续使用队列拥有的 workCtx。
- 未启动任务被 Cancel 后，Handle.Wait 可以返回取消结局；无 worker 时 WaitRecorded 仍等待，因为没有人尝试持久化。
- 之后 worker 消费已取消任务时，Start 不执行模型；它仍按既有规则尝试记录。没有本次提交边界时保留 ErrNoCommittedBoundary，不伪造完成 checkpoint。

兼容迁移：修订 ADR-0033 D5 中“Wait 可能驱动执行”这一条；补 changelog 和启动 worker 的示例。调用方若依赖 enqueue 后直接 Wait 启动任务，需选择正常启动 worker，或改为普通 Agent.Run。保留原 Wait 签名，不加一个可以绕过队列的 Handle.Start。

验收：无 worker 时 Handle.Wait 不执行任何工作；启动 worker 后才执行；并发大量 Handle.Wait 时模型实际并发不超过 worker 数；提交取消、执行取消、完成记录失败的既有契约继续通过。增加屏障用例：执行取消已经发生，但执行者或清理尚未退出时，RecordCompletion 的调用次数必须仍为零。

## 7. P1-1：启动、等待、观察接口契约

建议只增加两个 Run 方法：

```go
func (r *Run) Start()                          // 幂等、非阻塞；运行/参数错误通过 Await 报告
func (r *Run) Await(ctx context.Context) (core.Result, error) // 纯等待，不启动、不取消执行
```

暂不增加 Result、WaitContext、Done 等同义入口。内部仍可使用 done channel。

| 入口 | 启动 | 观察/等待 | 运行已结算时 |
| --- | --- | --- | --- |
| Stream | 否 | 创建 Run，冻结请求、绑定执行 context | 不适用 |
| Start | 是，仅第一次 | 不等待 | 无操作，不重启 |
| Await(waitCtx) | 否 | 等待执行结算 | 返回独立结果副本 |
| Wait | 是 | Start + Await(Background) | 返回独立结果副本 |
| Iter 的首次消费 | 是 | 先订阅，再 Start，消费至单个终态 | 返回缓存尾部后结束 |
| Events | 否 | 仅注册；调用方持有 unsubscribe | 返回有限终态尾部 channel |
| queue.Handle.Wait | 否 | 纯等待执行结算 | 返回独立结果副本 |
| queue.Handle.WaitRecorded | 否 | 等待 worker 完成收尾尝试 | 返回独立的收尾结果 |

Await 的 waitCtx 只控制这位等待者。它取消后返回 waitCtx.Err，不调用 Run.Cancel，也不改变原运行 outcome。Run 的执行 context 仍来自 Stream/Resume 创建时绑定的 context。

若 done 与 waitCtx.Done 同时可观察，采用结果优先的二次检查：先检查 done，再 select；取消分支返回前再次检查 done。最后一次检查时仍未完成，才返回等待 context 错误。不能承诺取消返回前后绝对没有并发结算。

需要订阅原始事件的用法为 Events → Start → Await；获取迭代流继续使用 for range Stream(...).Iter()。队列 Handle 不对外暴露启动能力。

## 8. P1-2：执行方独占结算，Wait 不消费事件

目标结构是：

```text
Start → 唯一执行者 → 冻结结果 → 封闭事件并安排尾部交付 → close(done)
                     ↓                                    ↓
              FrozenOutcome/DetailedResult              Await
```

- 删除 Iter/Wait 对 capture 的依赖；观察者不得调用 finish 来决定运行完成。
- Wait 只调用 Start 和 Await，任何数量的等待者都不建立总线订阅。
- done 关闭前完成结果冻结、身份和 boundary 的写入。结果发布与等待通过 channel 关闭建立同步关系。
- Await 复用冻结结果的深拷贝出口；不同等待者修改自己的消息不会改变运行记录或其他等待者的结果。
- freezeTerminal 不允许 clone 失败后回退为共享的原始可变对象；冻结失败应形成错误结局。对已规范化的冻结消息复制仍失败时，等待方法返回错误，不静默共享引用。
- 保留 Paused、FrozenOutcome、DetailedResult 和 RecordCompletion 的职责；暂停沿用零 Result + nil error，由 Paused/DetailedResult.Paused 区分。
- 不增加全历史事件回放，也不让运行中 RunStatus 冒充进程内实时状态；RunStatus 继续表达 checkpoint 存储事实。

验收：100 个并发等待者不增加 Bus.Subscribers；没有消费者也能结算；结果副本相互隔离；暂停、准备失败、执行失败、claim 释放失败都由单一执行路径结算。

## 9. P1-3：未启动取消、资源交接与唯一结算权

### 9.1 状态与竞争规则

内部阶段使用 Prepared → Starting → Running → Settling → Settled。Prepared 也可直接进入 Settling。Succeeded/Failed/Paused/Canceled 是冻结 outcome 的类别，而不是让阶段回退；Resume 创建新的 Run，不重启已暂停 Run。

- Start 与 Cancel 使用同一 lifecycleMu 争夺 Prepared。
- Start 获胜：设置 Starting，启动唯一执行 goroutine；Cancel 仅请求执行 context 取消，由执行者结算。Start 已赢但尚未调度时也不能由第二个结算者接管。
- Cancel 获胜：设置 Settling，自己负责未执行路径的结算；不发布 RunStarted，不访问模型、工具或普通历史恢复。
- 执行者进入 Settling 时锁定本次 outcome。此前记录的取消请求参与结局判定；此后 Cancel 不改写结局。cleanup 错误仍按 terminalOutcome 的现有规则合并。
- 同一 Run 只有一个结算者。startOnce/driveStarted 最终由单一阶段字段取代；不能长期保留互不约束的第二套状态。
- 已启动的 Cancel 是请求取消，不承诺方法返回时执行已停止；需要 Await。
- 未启动的 Cancel 同步尝试释放 claim/gate 并结算；多个并发取消者在锁外等同一结算完成，不能有调用方以为资源已清理却提前返回。

### 9.2 取消路径

1. 在短临界区取得结算权，随后释放锁。
2. 取消执行 context、关闭 steering，让未递送 ticket 结束。
3. 对 Resume 已持有的 claim 进行释放尝试，先 claim 后 admission gate；claim 释放失败进入 errors.Join(cancelCause, cleanupErr)。远端 claim 释放失败时不承诺它已消失，仍依赖 store 的 fencing/lease 规则。
4. 清理 I/O 使用脱离执行取消的有限 context。沿用已有可用的清理超时；无统一值时新增内部 runCleanupTimeout = 5s，并可在测试中注入短期限。忽略 context 的 store 不在强制终止承诺内。
5. 按 P0-2 冻结失败 outcome、封闭事件、安排 RunFailed 尾部并关闭 done。没有本次提交时不新增持久化 checkpoint。

成功取得 admission 后，其所有权交给 Run 的唯一结算者。明确修改 agent/run_prepare.go：移除或改造当前“任意准备错误即 releaseAdmission”的 defer，使 prepare 返回错误时仍由结算者先清理 claim、后释放 gate；未取得 gate 的路径保持无资源可释放。不要只修正常执行完成的分支。claim 释放尝试失败时，在记录清理错误后才交接 gate，后续运行仍受 store 的 claim/fencing 检查。

资源 I/O 不得在 lifecycleMu 内执行。prepared Cancel 的同步含义是完成清理尝试并发布结局，不保证外部 store 一定成功释放。

### 9.3 父 context 取消与构造安全

仅显式调用 Cancel 不够，父 context deadline 也可能在首次消费前到期。使用 context.AfterFunc 安排同一套取消逻辑，并在结算时停止关联。不能依赖 stop 函数等待已开始的回调结束。

取消回调必须在完整构造后安装，不能直接放到 newRunHandleWithID 的前半段：Resume 当前在 newRunHandle 返回后才附加 gate、claim、恢复批次和 preparation。若取消回调先结算、资源后附加，会造成泄漏。

采用统一的内部 activateLifecycle 最后步骤：

- streamConfigured、streamManaged、Resume 所有成功返回分支和 resumeInFlight，先完成资源交接和字段初始化，再调用 activateLifecycle，最后才对外返回句柄。
- 已取消的 context 在激活后可以立即触发取消；此时完整资源已经归 Run 所有。
- activateLifecycle 持有 lifecycleMu，登记 AfterFunc、保存返回的 stop 函数、发布 activated 标记，然后解锁；回调首先获取同一 lifecycleMu 再进入取消状态机。AfterFunc 在 context 已取消时也只异步启动回调，不会同步等待回调完成，因此该握手不会因回调取锁形成登记死锁。即使回调被立即调度，它也不能先于 stop 函数和激活标记的发布进入结算。
- 激活后禁止再附加 claim/gate/preparation；Start 仅操作已经激活的公开句柄。
- 对直接调用内部构造器的测试，明确它们是尚未激活的测试夹具，或改为使用完整构造入口。
- 结算者在 lifecycleMu 下取出并清空 stop 函数，随后在锁外调用；stop 返回 false 不表示应同步等待回调。特别是另一个取消调用可能正在等待 done，结算者必须先关闭 done，不能在这之前 join 回调。需要确认回调完全退出的测试在 done 关闭后等待独立的 callbackDone 屏障。
- 结算时停止 AfterFunc 关联并释放运行 context，避免已完成运行被父 context 继续保留。

验收覆盖 Start/Cancel 竞争、父 deadline 先于消费、激活前 context 已取消、激活期间取消、stop 函数保存与立即回调之间的握手、Resume claim/gate 同步清理、释放失败、并发重复取消，以及完成后父 context 再取消不改写 outcome。callbackDone 的等待不得成为关闭运行 done 的前置条件。

## 10. P2-1：契约、兼容与可观测性

必须更新：Run/Stream/Iter/Events/Wait/Start/Await/Cancel 的 GoDoc、queue.Handle 文档、README 的流式与后台示例、ADR-0033 D5 的替代说明，以及发布说明。

明确列出以下行为变更：

1. queue.Handle.Wait 不再驱动运行，必须有 worker；普通 Run.Wait 仍然惰性启动。
2. 已结束 Run 的 Iter/Events 返回有限终态尾部，不再无限等待，也不重放进度。
3. Wait 返回后终态可能尚未进入慢消费者的 channel；禁止用“Wait 返回后非阻塞排空 Events”判断终态是否存在。
4. Events 在终态尾部交付后自动关闭；提前退出仍必须调用 unsubscribe，以释放可能阻塞的收尾者。
5. 未启动取消会产生完整的内存结局，Await 可以返回；无 worker 不意味着终态记录已经写入。
6. 输入在创建时冻结，历史在获得 admission 后恢复；创建先后不构成执行顺序承诺。
7. RecordApproval 的权威结果是 durable receipt；已终结 Run 的观察流不会为之后的审批重新打开。

队列已有 terminalEvent 测试助手假设“Wait 返回后立即排空即能看到终态”，应改为带测试 deadline 的持续接收，验证最终交付及顺序，不把原假设偷偷保留成新的运行屏障。

观测上区分 CreatedAt、StartRequestedAt、AdmittedAt、SettledAt 和首次模型响应时间。优先使用项目已有 tracing/metrics，不为此另加公开实时状态接口；指标只用于区分等待消费、等待线程准入和执行耗时，不改变调度行为。

## 11. 实施顺序与文件范围

| 批次 | 内容 | 主要文件 | 必须一起完成的契约 |
| --- | --- | --- | --- |
| A | P0-1 发送与取消协议 | bus/bus.go、bus/bus_test.go；agent/runtime.go、agent/run.go 的框架发布点 | 不持有总线锁等待；send/close 互斥协议；取消可唤醒 |
| B | P0-2 终态尾部和迟到订阅 | bus/bus.go、agent/run.go、agent/run_prepare.go；相关 agent 和 queue 测试 | FinishTopic + observationMu；冻结先于 hook/终态；所有路径先清理 claim 后交接 gate |
| C | P0-3 与 P1-1 的最小 Await/Start 基础 | agent/run.go、queue/agent_bridge.go；队列并发测试 | worker 保有执行权；worker 等到真正结算后才记录 |
| D | P1-2/P1-3 状态和取消统一 | agent/run.go、agent/agent.go、agent/managed_run.go、agent/hitl.go、agent/run_prepare.go、必要的清理 helper | 构造激活握手、唯一结算者、父 context 取消、结果副本 |
| E | P2 文档和示例 | README、相关 GoDoc、ADR 替代说明、examples | 所有行为变更和迁移方式准确对应实现 |

B 可先沿用当前执行者的完成入口，但观察者不再能抢先结算；D 再将取消纳入统一状态机。C 引入的方法在 D 中收敛实现，不能产生两种公开 Start/Await 语义。C 可独立验证“等待不启动”和“worker 并发限制”；“未启动 Cancel 后 Wait/Await 返回”依赖 D 的完整取消结算，必须在 D 完成后联合验收，不能在 C 提前宣称通过。

建议回归命令：

```text
go test ./bus ./agent ./queue -count=1
go test -race ./bus ./agent ./queue -count=1
go test ./... -count=1
```

竞态用例使用可控 channel 屏障定位“已登记发送”“已摘除订阅”“已安装尾部”“进入结算”和“资源已交接”；timeout 只做失败保护，不用 sleep 推断正确性。不以单次 goroutine 总数作为泄漏证明，而是直接等待对应发送者、收尾者和取消回调退出。

本轮仅交付技术设计，以上命令是后续实施的验收要求，不代表这些新测试已经存在或已经通过。

## 12. 独立模型审核

2026-10-28 使用独立上下文模型，按 code-review-and-quality 技能完成只读审核。首轮结论为 Request changes，确认 3 个 P1 修订点，并要求补齐 1 个激活握手条件；未据此确认新增 P0。审核基于源码推演，没有把设计判断当作新协议的实测结果。

| 审核发现 | 性质 | 正文修订 |
| --- | --- | --- |
| prepare 失败提前释放 gate | 现有代码路径未被原规格覆盖 | 5.3、5.4、9.2、11：移交统一结算者，明确修改 run_prepare.go，添加 claim/gate 屏障用例 |
| hook 先于可失败冻结，造成结局不一致 | 初稿引入的顺序问题 | 5.3、5.4：规范化与冻结先于 hook，hook 获取独立副本，新增冻结失败及参数修改用例 |
| TaskDone 的非阻塞承诺范围过宽 | 初稿契约与算法范围冲突 | 3、5：限定根 TaskDone 与 Run 终态；子任务事件保持可取消背压，禁止提前封闭共享 topic |
| AfterFunc 返回前回调可能已开始 | 激活协议缺少登记握手 | 5.3、9.3：同锁登记、发布 stop 与激活标记；锁外停止，关闭 done 前不等待回调退出 |

另补 FinishTopic/unsubscribe/普通发送三方竞争、取消但尚未结算时不调用 RecordCompletion，以及批次 C 对 D 的取消验收依赖。修订后由原独立审核者进行定向复核，确认四个阻塞项全部闭合、补充约束完整，结论为“设计复核通过，可以作为实施规格”；指定章节未发现修订引入的实质新增矛盾。复核只读取修订章节，没有修改文件或运行测试；该结论不代表实现验证通过。

## 13. 标准语义依据

本方案依赖的 Go 语义应以官方资料为准：

- Go Memory Model：channel close 与观察到关闭的接收建立同步；不能用 goroutine 退出代替结果发布屏障。`https://go.dev/ref/mem`
- context 包：CancelFunc 不等待工作退出；AfterFunc 在独立 goroutine 执行，停止关联不会等待已开始回调。`https://pkg.go.dev/context`
- Go Language Specification：缓冲 channel、关闭后读取以及对已关闭 channel 发送的规则。`https://go.dev/ref/spec`
- Go Race Detector：并发 send/close 本身需要同步，不能仅以没有立即 panic 作为证明。`https://go.dev/doc/articles/race_detector`


## 14. 本轮验证范围

2026-10-28 执行 `go test ./bus ./agent ./queue -count=1 -timeout=90s`，三个包均通过。该结果只建立现有实现的测试基线，不验证本文尚未实现的协议。

文档检查结果：7 项优先级齐全，所有列出的主要源文件存在，代码围栏成对，无未解决占位项或行尾空格。本轮仅新增本技术方案文件，未修改生产代码、测试或已有 ADR，未创建提交。
