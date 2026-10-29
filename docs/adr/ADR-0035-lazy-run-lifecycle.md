# ADR-0035: 惰性 Run 的启动、等待、观察与结算

- 状态: Accepted
- 日期: 2026-10-28
- 替代: [ADR-0033 D5](ADR-0033-background-run-context-and-status.md#d5-等待与查询分别表达执行结果记录结果和存储事实) 中允许 queue.Handle.Wait 驱动运行的旧决定；D1–D4 及 D5 的记录、查询职责继续保留。
- 依据: [批准设计](../superpowers/specs/2026-10-28-lazy-run-lifecycle-design.md)、[迁移说明](../changes/2026-10-28-lazy-run-lifecycle.md)。

## 背景

惰性运行需要分别表达启动执行、等待结果和观察事件。原先用等待者订阅并捕获终态来决定结果，会让慢观察者影响结算，也让完成后的迭代器无法自然结束。队列的 Handle.Wait 若可以驱动底层 Run，则提交方能绕过 worker 的并发名额。取消尚未启动的运行还必须有明确的内存结果，不能要求消费者或 worker 先启动模型才能结束等待。

保留普通 Stream 的副作用时点，可以兼容先订阅再执行的用法。运行事实由唯一结算者产生，事件交付与 worker 的持久化尝试各自有独立的完成条件。

## 决策

### D1. 保留惰性入口，显式区分启动、等待和订阅

| 入口 | 启动执行 | 观察或等待 |
| --- | --- | --- |
| Agent.Stream | 否 | 创建句柄并冻结输入；普通运行获得 admission 后才恢复 checkpoint 历史 |
| Run.Start | 是，幂等、非阻塞 | 不订阅，不等待；启动或执行错误由等待方法报告 |
| Run.Await(ctx) | 否 | 只等结算；ctx 只结束这位等待者，不取消运行 |
| Run.Wait | 是 | 等于 Start + Await(context.Background())，不建立事件订阅 |
| Run.Iter 的消费 | 是 | 每次消费先订阅，再 Start；创建 iterator 本身不启动 |
| Run.Events | 否 | 只订阅，返回 channel 与 unsubscribe |
| queue.Handle.Wait | 否 | 纯 Await，正常执行依赖 worker |
| queue.Handle.WaitRecorded | 否 | 等 worker 完成执行结算并尝试记录终态 |

Start 的并发重复调用只允许一个启动者；已结算后调用也不重跑。Await/Wait 返回独立结果副本。等待 context 与结算同时可见时优先已结算结果；取消分支在返回前再检查结算，不能保证检查之后不会并发完成。

普通 Stream 在创建时不做 checkpoint 恢复、模型或工具调用。Start 请求之后可能仍在等待 admission。Resume 有既有的同步准备和预占 gate 行为，不套用普通 Stream 的全部构造时序。

### D2. 只保留有限终态尾部

活动运行的晚订阅只收到订阅之后的事件及尾部，不回放此前的 RunStarted、模型增量或工具进度。已封闭运行的 Iter/Events 只取得缓存尾部并结束，不注册新的活动订阅，也不重新执行：

- 已进入根 Task：根 TaskDone，随后恰好一个 RunDone、RunFailed 或 Interrupted。
- 尚未进入根 Task 的准备失败或 prepared 取消：尾部只有 RunFailed，不伪造根 TaskDone。prepared 取消也不发布 RunStarted。

持续消费且未退订的观察者，先读已入队的普通事件，再读有序尾部，最后看到 channel 关闭。Lossy 可以丢普通进度，不能静默丢掉这个尾部；主动退订可以放弃剩余交付。事件应视为只读，尾部与结果副本相互隔离。

Wait/Await 返回只说明内存结算完成，不保证观察者已读到尾部，甚至不保证尾部已经进入观察 channel。需要完整观察时读到 channel 关闭；不能在 Wait 后用 select/default 的非阻塞 drain 判断尾部已经齐全。Events 调用方应立即 defer unsubscribe()，尤其在提前退出时；否则停止读取的订阅可能保留等待交付尾部的收尾 goroutine。Iter 自己管理退订，break 会退出该订阅。

普通 Lossless 进度仍会背压执行，订阅取消和运行 context 取消可以打断框架发送。只有根 TaskDone 与 Run 终态尾部交付不等待消费者腾出缓冲；workflow 分支、plan 节点的 child TaskDone 仍是普通、可取消的背压事件，不能提前封闭共享 topic。

### D3. 执行或取消的唯一结算者决定结果

结算顺序是：停止并释放 durable claim，再释放 admission；完成规范化和深拷贝，确定冻结 outcome、身份与本次提交边界；按既有条件调用 FinishRun；封闭观察流并安排尾部交付；最后发布结算完成。hook、事件消费者或等待者不能改写冻结结果，也不能抢占结算。

Prepared 状态显式 Cancel 同步完成内存结算与既有资源清理，没有模型调用，仅留下 RunFailed 尾部。已启动状态的 Cancel 只提出取消请求；实际执行者退出和清理后才结算。父执行 context 取消也走同一生命周期协议。忽略 context 的模型、工具、hook 或 store 必须自己协作退出，取消不提供强制抢占。需要确定已结束时 Cancel 后再 Await；仅限制等待时间时使用 Await(waitCtx)。

不新增公开 Done、Result、WaitContext，也不把 RunStatus 当作进程内实时运行状态。

### D4. 队列 worker 独占启动权

worker 取得 Pool 名额并消费 job 后，显式 Start，再以 Background Await 等实际结算；不能用可能取消的等待 context 提前开始 RecordCompletion。Handle 不新增 Start/Await，Wait 不 dequeue、不执行模型或工具、不写终态。

- 无 worker 的普通任务保持 prepared，Handle.Wait 等待；迁移时启动 Pool/worker，或者改用直接执行的 Agent.Run。
- 未消费任务被 Handle.Cancel 后，内存 Wait 可以返回取消结果；无 worker 时 WaitRecorded 仍等待，因为尚无人尝试记录。
- 后续 worker 消费该任务时不得执行模型，但仍按既有规则尝试记录。没有本次成功提交边界时保留 agent.ErrNoCommittedBoundary，不伪造 Completion 或借用旧 head。
- WaitRecorded 返回独立结果与 errors.Join(runErr, recordErr)。它等待的是记录尝试；completed 且返回 nil 才确认记录成功。暂停跳过终态记录。失败可能同时包含运行与记录错误。

提交 context 只控制提交，入队成功后的 workCtx 归队列所有；Pool context 停止取任务并等待在途任务。停止某项工作使用 Handle.Cancel。RunStatus 继续只报告存储事实，执行结算、事件消费和终态记录互不等价。

### D5. 审批与已封闭流分别表达持久事实

RecordApproval 的持久 decision receipt 是权威事实。对 sealed Run 再次提交相同审批不会重开事件流或补发进度；以 receipt 判断持久结果，需要继续执行时使用 Resume。不能依赖在旧 Run 上重新订阅 ApprovalDecided 来确认持久成功。

### D6. 生命周期观测不改变执行语义

记录 CreatedAt、StartRequestedAt、AdmittedAt、FirstModelResponseAt、SettledAt，只附加实际经历的时点。生命周期 events 加到 context 已有 span，不创建新的 root/child span，不改变现有 obs/otel 模型、工具 span 的父子关系。

生命周期指标从全局 otel.GetMeterProvider() 取 meter。独立的 obs/otel.WithMeterProvider 只影响该 middleware，不能代替设置应用全局 MeterProvider。四个 histogram 的单位均为秒：

| 指标 | 区间 |
| --- | --- |
| agent.run.creation_to_start | 创建到首次启动请求 |
| agent.run.start_to_admission | 首次启动请求到真实准入 |
| agent.run.admission_to_first_response | 真实准入到首个有效模型 response |
| agent.run.start_to_settlement | 首次启动请求到结算 |

只有区间两端都存在且差值非负时才记录；缺失阶段不填假零值。Resume 可先取得 admission 再创建句柄，AdmittedAt 可以早于 CreatedAt/StartRequestedAt：保留真实时间，省略负的 start_to_admission，不能改成启动时刻。首个有效 response 指 err=nil 且 response 非 nil，可为 partial 或最终响应，不代表首 token。

指标不带 runID、threadID、prompt、tool args 等高基数或内容 labels；运行身份仅放 span event 属性。

## 备选方案

| 方案 | 未采用的原因 |
| --- | --- |
| Stream 创建后立即启动 | 改变既有副作用时点，增加首事件订阅竞态 |
| 全部调用方强制 Prepare → Subscribe → Start → Await | 迁移成本高；显式方法与 Wait/Iter 便利入口可以并存 |
| 保留 queue.Handle.Wait 的启动副作用 | 绕过 worker 并发上限；Handle.Start 有相同问题 |
| 缓存并回放所有进度事件 | 引入无界历史和新的持久化契约；有限尾部已满足终结观察 |
| Wait 同时等待所有观察者或持久化 | 慢观察者或 store 决定执行等待时长，混淆三种完成条件 |

## 后果与验证边界

普通 Run.Wait 的启动能力保留；依赖 queue.Handle.Wait 启动的调用方必须迁移。停止读流的调用方要退订，普通 Lossless 仍可能阻塞进度。prepared 取消后能读取内存结果，不意味着有 worker 或持久终态。

[离线示例](../../examples/lazy-run-lifecycle/main.go) 用 mock、channel 与 context 展示单次启动、Await 超时隔离、有限尾部和两个队列等待终点；[background](../../examples/background/main.go) 展示实际 Pool、WaitRecorded 与 worker 退出。实现与跨层并发验证的具体命令及结果见[执行计划](../superpowers/plans/2026-10-28-lazy-run-lifecycle-implementation.md#9-执行证据执行时填写)。
