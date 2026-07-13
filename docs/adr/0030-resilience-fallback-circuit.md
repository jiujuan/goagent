# ADR 0030：弹性系统 —— Provider Fallback + 熔断（Circuit Breaker）

状态：提议

> 本文给 v2 增加**企业级弹性**：两个纯 `llm.Model` 装饰器——`FallbackModel`（provider 故障
> 转移）与 `CircuitBreaker`（熔断），配合既有 `RetryModel`，让单一 LLM 供应商的抖动或宕机
> 不再拖垮整个 Agent 服务。同时引入轻量类型化错误 `llm.StatusError`，让弹性判定基于状态码而
> 非字符串匹配。**不改 agent / loop / core**。

---

## 背景

可观测性（ADR-0029）解决"看得见"；本文解决"扛得住"。现状只有
[middleware/retry.go](../../middleware/retry.go) 的 `RetryModel`（瞬时重试**同一** provider），
企业生产缺两样关键能力：

1. **Provider 故障转移**：主 provider 持续 5xx/限流/超时时，自动切到备用模型，避免单点供应商
   宕机即全站不可用。
2. **熔断**：对持续失败的 provider 快速失败（fail-fast），不再每次空等超时，避免雪崩；冷却后
   半开探测自动恢复。

且 provider 当前返回无类型 `fmt.Errorf("…: status %d: …")`，状态码只在字符串里，弹性逻辑无法
可靠区分 429/5xx（值得切换/熔断）与 4xx（换地方也一样失败）。

## 决策

retry / circuit / fallback 三者职责正交，均实现为 `llm.Model` 装饰器（与 `RetryModel` 同包
`middleware`、同范式），可自由叠放：

### 一、类型化错误 `llm.StatusError`（[llm/error.go](../../llm/error.go)）

```go
type StatusError struct { Provider string; Code int; Body string }
func (e *StatusError) Error() string   // "<provider>: status <code>: <body>"，与旧格式一致（向后兼容）
func (e *StatusError) Retryable() bool // 408 / 429 / 5xx
func IsRetryable(err error) bool       // 共享默认判定：StatusError 看 Retryable；context 取消/超时不重试；其余默认可重试
```

两个 provider（[anthropic](../../llm/anthropic/anthropic.go)、
[openaicompat](../../llm/openaicompat/openaicompat.go)）的状态码错误改回传 `*llm.StatusError`
（各 1 行，`Error()` 文本不变）。

### 二、`FallbackModel`（[middleware/fallback.go](../../middleware/fallback.go)）

```go
func FallbackModel(o FallbackOptions, models ...llm.Model) llm.Model // 首个为主，余者按序备份
type FallbackOptions struct {
    ShouldFallback func(error) bool                 // 默认 llm.IsRetryable
    OnFallback     func(from, to string, err error) // 观测钩子
}
```

依次尝试：**pre-stream 失败**且谓词通过且有后备 → 切下一个；**mid-stream 失败**（已吐 token）
→ 无法切换、原样上抛；谓词拒绝 / 最后一个 → 返回该错误。

### 三、`CircuitBreaker`（[middleware/circuit.go](../../middleware/circuit.go)）

经典三态机（`StateClosed/Open/HalfOpen`），`sync.Mutex` 保护，状态回调在锁外触发：

- **Closed**：连续失败达 `FailureThreshold` → Open。
- **Open**：`Now()-openedAt >= OpenTimeout` → 转 HalfOpen 放行单个探测；否则**直接 `ErrCircuitOpen`，不触达 inner**（fail-fast）。
- **HalfOpen**：同一时刻仅一个探测；连续成功达 `SuccessThreshold` → Closed；失败 → 重新 Open。

`IsFailure`（默认 `llm.IsRetryable`）决定哪些错误计入熔断健康度——**400 等非失败错误既不计失败
也不计成功**，不污染熔断。`Now` 可注入便于测试。

### 四、推荐叠放顺序

```go
primary := middleware.CircuitBreaker(
    middleware.RetryModel(real, middleware.RetryOptions{MaxAttempts: 2}),
    middleware.CircuitOptions{FailureThreshold: 5, OpenTimeout: 30 * time.Second})
model := middleware.FallbackModel(middleware.FallbackOptions{}, primary, backup)
```

里→外：**retry**（吸收瞬时抖动）→ **circuit**（持续故障 fail-fast）→ **fallback**（换 provider）。
`ErrCircuitOpen` 默认被 `IsRetryable` 判为可切换，故熔断打开时 Fallback 立即转备用、**不空等超时**
（见 [examples/resilience](../../examples/resilience)：主模型被触达 3 次后熔断，后续调用 fail-fast）。

## 影响

- 新增：`llm/error.go`、`middleware/fallback.go`、`middleware/circuit.go`（各带测试）、
  `examples/resilience/`、本 ADR。
- 修改：两个 provider 各 1 行（回传 `*llm.StatusError`）。
- **不改** agent / loop / core；不引入新依赖（纯标准库）。

## 已知限制（未来增强）

1. **mid-stream 不可切换/不可重试**：与 `RetryModel` 同源约束，token 一旦流出，故障只能原样上抛。
   未来可"缓冲首块再提交"以支持 mid-stream 切换（代价是首 token 延迟）。
2. **熔断为进程内、按装饰器实例**：多副本服务各自独立计数，不共享熔断状态。如需全局熔断需
   外部协调（如 Redis），属未来增强。
3. **网络层错误无状态码**：默认按可重试处理，可能对永久性网络错误做无谓切换；用户可用
   `ShouldFallback` / `IsFailure` 谓词精调。

## 替代方案（未采用）

- **做成 loop 中间件**：模型重试/切换属于"裸模型调用"层，放 `llm.Model` 装饰器更内聚，且
   与既有 `RetryModel` 一致。
- **引入第三方熔断库**（sony/gobreaker 等）：违反"核心零外部依赖"，且三态机本身很轻。
- **字符串匹配状态码**：脆弱；类型化 `StatusError` 一次到位且向后兼容。
