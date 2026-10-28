# 2026-10-28：惰性 Run 生命周期与队列等待迁移

日期：2026-10-28。此变更保留普通 Stream 的惰性行为，并调整队列等待与终态观察契约。

决策见 [ADR-0035](../adr/ADR-0035-lazy-run-lifecycle.md)，它替代 [ADR-0033 D5](../adr/ADR-0033-background-run-context-and-status.md#d5-等待与查询分别表达执行结果记录结果和存储事实) 中 Handle.Wait 可以驱动执行的旧决定。

## 行为与兼容边界

- 普通 Stream 仍在创建时冻结输入且保持惰性；启动并获得 admission 后才恢复历史。Start 幂等、非阻塞；Await(ctx) 只等待、不启动或取消执行，返回独立结果副本；Wait = Start + Await(Background)，无事件订阅。
- Iter 在消费时先订阅再 Start。Events 只订阅，提前退出仍需 defer unsubscribe。活动晚订阅不补历史进度；sealed Run 只给根 TaskDone + 一个 Run 终态的有限尾部；未进入根 Task 的失败只有 RunFailed。尾部后 channel 关闭，重复观察不重跑。
- Wait 返回不保证观察者已收到尾部；尾部可能还没进入 channel。普通 Lossless 进度保持背压，child TaskDone 仍为普通可取消事件，只有根 TaskDone 与 Run 终态尾部交付不阻塞结算。
- Prepared Cancel 同步内存结算，不发布 RunStarted/root TaskDone，仅 RunFailed。Started Cancel 只提出请求，等待实际执行者与资源清理完成才结算；外部模型、工具、hook 和 store 必须协作响应 context。
- queue.Handle.Wait 改为纯 Await，正常执行必须有 worker。WaitRecorded 等 worker 的记录尝试。prepared 取消且无 worker 时 Wait 可返回，WaitRecorded 仍等；后续 worker 不跑模型，无本次成功提交边界保留 ErrNoCommittedBoundary。
- RecordApproval 以持久 decision receipt 为权威；已封闭 Run 不因重复记录审批而重开流，继续执行使用 Resume。

## 调用方迁移

### 1. 入队后启动 worker

旧代码若仅靠 Handle.Wait 启动运行，改为运行 Pool/worker。不要寻找 Handle.Start 或 Handle.Await，这两个方法不新增。

```go
q := queue.NewMemQueue(1)
pool := queue.NewPool(q, 1)
workersDone := make(chan struct{})
go func() {
    defer close(workersDone)
    pool.Run(ctx)
}()
h, err := queue.EnqueueAgent(ctx, q, a, "hello")
if err != nil {
    q.Close()
    <-workersDone
    return err
}
q.Close()
_, runErr := h.Wait()
_, recordErr := h.WaitRecorded()
<-workersDone
return errors.Join(runErr, recordErr)
```

需要直接执行则用普通 Agent.Run。提交 context 仅控制入队；已接受任务的执行 context 归队列。Pool context 取消只停止取新任务并等在途任务，停止指定工作用 Handle.Cancel。

WaitRecorded 表达“worker 已尝试记录并保存收尾结果”，返回 errors.Join(runErr, recordErr)；不是无条件持久成功。completed 且 nil error 才确认记录成功，暂停不记终态，失败可能同时包含运行与记录错误。RunStatus 只读存储事实。

### 2. 分开等待超时与执行取消

普通 Run 可显式启动后限制某一位等待者：

```go
run := a.Stream(execCtx, "hello")
run.Start()
waitCtx, stopWaiting := context.WithTimeout(context.Background(), time.Second)
defer stopWaiting()
_, err := run.Await(waitCtx) // DeadlineExceeded 不取消 execCtx 或运行。
// 业务确实需要停止工作时：
run.Cancel()
_, settledErr := run.Await(context.Background())
_ = err
_ = settledErr
```

queue.Handle.Wait 没有等待 context，不要把它当成超时或取消 API；停止队列工作用 Handle.Cancel，之后 Wait 等内存结算，WaitRecorded 等 worker 记录尝试。

### 3. 清理订阅，读取到 channel 关闭

原始事件用 Events → Start → 持续消费 → Await，注册后立即 defer unsubscribe()。Iter 的 break 会自动退订，但不会自动停止运行；需要停止时显式 Cancel，再等待结算。不要把普通 Lossless 订阅留在既不读也不退订的状态。

移除“Wait 返回后用 select/default 非阻塞 drain，暂时没事件便算流结束”的模式。需要尾部的消费者持续接收至 channel 关闭；不再观察的消费者退订。Wait 只是内存结果的完成屏障，不是消费者排空屏障。

### 4. 使用 receipt 确认审批

以 RecordApproval 返回的持久 receipt 确认幂等提交，不等待 sealed Run 补发审批事件。旧流保持封闭，恢复执行由 Resume 返回的新句柄负责。

## 生命周期观测

生命周期向 context 中已有 span 添加 events，不增加 span 或改变模型/工具的 span 父子关系。记录 CreatedAt、StartRequestedAt、AdmittedAt、FirstModelResponseAt、SettledAt；未经历的阶段不伪造。

以下 histogram 单位为秒，使用全局 otel.GetMeterProvider()：

- agent.run.creation_to_start
- agent.run.start_to_admission
- agent.run.admission_to_first_response
- agent.run.start_to_settlement

应用通过 otel.SetMeterProvider(...) 配置；单独配置 obs/otel.WithMeterProvider 不影响这些生命周期 metrics。区间两端缺失或差值为负时省略。Resume 可以先 admission 后创建，保留真实时间，不把早到的 admission 改成 Start。首次有效模型 response 是非 nil 且 err=nil 的 partial 或最终响应，不是首 token。指标不加 runID/threadID/prompt/tool args 等高基数或内容 labels。

## 示例与验证边界

- [lazy-run-lifecycle](../../examples/lazy-run-lifecycle/main.go)：全 mock 离线断言，使用 channel/context，无 sleep；覆盖创建零模型调用、Start/Iter 单次执行、晚订阅有限尾部、Await 超时不取消、无 worker 的纯等待及 prepared 取消、启动 Pool 后 Wait/WaitRecorded 的差异。
- [background](../../examples/background/main.go)：离线 mock，运行 Pool、分别检查 Wait/WaitRecorded 并等待 worker 退出。
- [quickstart-stream](../../examples/quickstart-stream/main.go)：真实 provider 模板，显式持有 Run 并说明 break 后退订与取消；编译检查不发送 provider 请求。

编译和离线验证：

```sh
go test ./examples/... -run '^$'
go run ./examples/lazy-run-lifecycle
git diff --check
```

跨包功能、race、观测与示例的实际验证证据见[执行计划](../superpowers/plans/2026-10-28-lazy-run-lifecycle-implementation.md)。真实 provider 示例只编译，离线验证使用 mock。
