# goagent 架构设计综述

> 一句话理念:**一切皆事件流;事件即数据;能力即中间件;接口尽量小。**

本文把 goagent 各核心机制的深入分析汇总成一份连贯的架构综述,自底向上贯穿:
流式原语 → 事件系统 → provider 隔离 → 决策层(并发/委派)→ 横切能力(工具/中间件/记忆)。
配套阅读:[ARCHITECTURE.md](ARCHITECTURE.md)(分层总图)、[DESIGN.md](DESIGN.md)、各 [ADR](adr/)。

---

## 0. 设计基因

goagent 把整个 agent 框架建立在**两条正交的主轴**上:

- **纵轴(传输)**:`iter.Seq2` —— 唯一的流式原语,贯穿每一层。
- **横轴(数据)**:`Event` + `Actions` —— 流里流动的唯一数据单元,副作用建模成数据。

其余所有机制(provider 隔离、并发归并、委派、工具、中间件、记忆)都是这两条主轴上的**最小增量**。
全局贯穿一种工程品味:**对静默错误的偏执**——把协议/契约正确性钉死在最容易被忽略的细节上
(CallID 配对、embeddings Index 排序、retry 的 yielded 标志、Parallel 的 `<-done`、transfer 的深度上限),
少了任一个,系统会**无声地**产生错误结果,而非显式报错。

---

## 1. 流式原语:`iter.Seq2[*Event, error]`

### 1.1 唯一的流类型

```go
// core/stream.go
type Stream = iter.Seq2[*Event, error]   // 类型别名,不是新类型
```

它是 Go 1.23 的 range-over-func 迭代器,消费者一行驱动:

```go
for ev, err := range stream {
    if err != nil { /* 错误走同一条流 */ }
    if done { break }   // break/return 让 yield 返回 false,干净停止上游
}
```

### 1.2 为什么是它(ADR 0001)

| 诉求 | `iter.Seq2` 如何免费提供 |
|---|---|
| 统一心智模型 | runner→agent→turn 引擎→model 每层同构,组合直接 |
| 背压 | pull 模型:消费者不取,生产者阻塞,无需 channel 缓冲管理 |
| 取消 | 消费者 break → yield 返回 false → 生产者停止,无需 context 协调 |
| 零依赖 | Go 1.23 原生 |

### 1.3 全栈同构

同一迭代器签名在每层重复出现,只是 payload 不同:

```
internal/sse.Scan   → iter.Seq2[sse.Event, error]    (SSE 字节解析)
llm.Model.Generate  → iter.Seq2[*llm.Response, error] (模型层 + 中间件,同签名)
agent.Run           → iter.Seq2[*core.Event, error]   (决策层)
runner.Run          → iter.Seq2[*core.Event, error]   (编排层)
```

一个 `break` 能从最外层用户代码一路传导回 SSE 读取器,中途每层都正确收手。

构造/组合工具极少(小接口哲学):`Once` / `Fail` / `Empty` / `Pipe`。
`Pipe(src, yield)` 是组合关键——任何"包一层子流"的实现都用它转发并保留早停语义。

---

## 2. 事件系统:Event + Actions

### 2.1 数据结构

```go
// core/event.go
type Event struct {
    ID, InvocationID, Author, Branch string
    Message *Message    // 本步内容
    Partial bool        // 流式增量标记 —— 提交语义支点
    Actions Actions     // 声明式副作用 —— 事件即数据支点
    Usage   *Usage
    Err     error       // 错误走同一条流 (json:"-")
}

type Actions struct {
    StateDelta      map[string]any // 合并进 session state
    TransferToAgent string         // 委派目标
    Escalate        bool           // 通知外层 LoopAgent 停止
    Stop            bool           // 本 turn 结束
}
```

### 2.2 支点一:Partial —— 渲染 vs 提交

- **partial 事件**:流式文本增量,投递订阅者实时渲染,**永不落库**。
- **final 事件**:一步的聚合结果,进 history、被 Runner 提交。

这让 turn 引擎对"流式 vs 非流式"**零特判**(ADR 0006):
流式 = "yield 多个 partial 后跟一个 final",非流式 = "yield 一个 final",同一套代码。

### 2.3 支点二:Actions —— 事件即数据

组件**不直接改共享状态**,而是把副作用作为声明式数据挂在事件上,
由 Runner 在**提交非 partial 事件时事务性应用**。收益:**可重放、可审计、可持久化**。

---

## 3. 一次 turn 的完整数据流

```
① Provider:SSE 字节 → streamAgg 聚合 → Response 流(partial→final)
② 中间件:同签名装饰 Response 流(改请求 / 控调用)
③ Agent:Response → Event(透传 Partial),跑 tool-use 循环,按需委派
④ Runner:非 partial 事件先 Append 落库再 yield → 早停也留一致日志
⑤ 用户:for ev := range r.Run(...) 一行消费
```

**事务性三要点**:
- 提交边界 = 非 partial 事件(partial 永不触发 commit)。
- 先持久化,后 yield(落库失败则错误走流;消费者此刻 break,日志已一致)。
- 决策/执行分离(Agent 只声明 Actions,只有 Runner/Store 应用)。

---

## 4. Provider SSE 聚合器(ADR 0005/0006)

### 4.1 三层切分

```
io.Reader → sse.Scan (通用 SSE 协议) → streamAgg.handle (provider 专属累积) → Response
```

`sse.Scan` 只懂 SSE 协议本身,两 provider 共享;差异只在如何解释 data 负载。

### 4.2 两种 wire 模型收敛到同一序列

| 维度 | Anthropic(content-block) | OpenAI(delta) | 统一结果 |
|---|---|---|---|
| 文本载体 | `map[int]*string`(多块) | 单个 `content string` | `core.Text` |
| tool 累积 | content block index | `tool_calls[].index` | `core.ToolCall` |
| tool JSON | `input_json_delta` 拼接 | `function.arguments` 拼接 | `json.RawMessage` |
| 结束 | `message_stop` 事件 | `finish_reason`/`[DONE]` | `StopReason` |

OpenAI delta 的 id/name 通常只在首 chunk 出现,故需"判空保留"(`if tc.ID != "" {...}`),
否则后续只带 arguments 的 chunk 会清空 id/name。

### 4.3 三个不变量

- **tool call 绝不在 partial 出现**:snapshot 只含 text;tool args 默默累积,只在 final 打包。
  理由:绝不把残缺 JSON 暴露给消费者。
- **顺序严格保留**:`order []int` 记录出现次序,final 按序重建 parts。
- **容错收尾**:流正常退出后再 yield 一次 final 兜底;空 args 兜底成 `"{}"`。

可测性:`ParseStream(io.Reader)` 可喂 canned SSE 字符串,无需网络。

---

## 5. Actions 提交与合并:两级折叠

```
多工具并发产生 Actions → ① mergeActions(turn 内扇入) → 单 Event.Actions
                       → ② Session.commit(跨 commit 应用 + 持久化)
```

### 5.1 第一级:mergeActions(turn 内跨工具)

工具并发执行(各写独占的 `actions[i]`),`wg.Wait()` 后单线程合并:

```
StateDelta      → 逐 key 合并(后写覆盖同名 key)
TransferToAgent → last-wins(互斥)
Escalate / Stop → OR(单调)
```

并发安全靠"分而治之 + 屏障后合并",非锁。

### 5.2 第二级:Session.commit(状态应用 + scope 过滤)

```go
for k, v := range e.Actions.StateDelta {
    if strings.HasPrefix(k, "temp:") { continue } // temp: 不持久化
    s.state.Set(k, v)
}
s.events = append(s.events, e)
```

State 作用域前缀:`app:`(全局)/`user:`(用户级)/`temp:`(invocation 内,不落库)/无前缀(session 级)。

### 5.3 事件溯源

`FileStore.load` 逐行读 JSONL **重放同一个 commit 路径**——
**重建状态 = 折叠事件日志的 StateDelta**。写入和加载共用 `commit`,无独立快照:
日志即真相。这是"把 mutation 降格成 data"的全部价值来源。

---

## 6. 决策层并发:Parallel 的背压与取消

`ParallelAgent` 是全框架唯一需要手动并发协调处(pull 迭代器无法表达 fan-in)。
用三 channel 结构把并发归并重新塞回 `iter.Seq2` 语义:

```go
merged := make(chan parallelItem)  // 无缓冲 → 背压核心
done := make(chan struct{})        // 取消信号
// N 个生产者 goroutine:select{ merged<- ; <-done }
// 1 个 closer:wg.Wait(); close(merged)
// 消费者:defer close(done); for it := range merged { ... }
```

- **背压**:无缓冲 channel 是 rendezvous,生产者必须等消费者取走才继续 → 内存有界(最多 N 个在途),复刻 pull 背压。
- **取消**:消费者 break → `defer close(done)` → 生产者经 `<-done` 退出 → 逐层穿透到子 agent 的 model 调用。
- **正确性**:无 send-on-closed(merged 只在 wg.Wait 后关);无泄漏(阻塞的生产者总有 `<-done` 逃生路);closer 独立(消费者忙于读,不能自己 wg.Wait)。
- **归属**:Branch = 父branch + "." + sub.Name(),事件交错也能按分支还原来源。

---

## 7. 决策层委派:transfer 方向规则与防环(ADR 0008)

LLM 驱动委派由合成工具 `transfer_to_agent` 实现,`Call` 只写 `Actions.TransferToAgent`(复用 Actions 机制)。

### 7.1 方向规则(transferableTargets)

| 方向 | 规则 | 开关 |
|---|---|---|
| 下级(sub) | 总是允许 | DisableTransfer 全关 |
| 上级(parent) | 默认允许返回父 | DisallowTransferToParent |
| 同级(peer) | 默认允许,**但前提父 agent 自身可委派** | DisallowTransferToPeers |
| 自己 | 永不允许 | `delete(out, self)` |

同级需"父可委派"的原因:父若不能委派(DisableTransfer 或 workflow agent),
横向交接会"搁浅",故框架直接不列同级为目标。

### 7.2 双层强制

- **schema enum(引导)**:工具 schema 的 `enum` 只列合法目标,模型只被告知有效选项。
- **运行时校验(兜底)**:`transferTargets[name]` 查不到则 fall through,模型重试。
  软约束 + 硬约束缺一不可。

### 7.3 防环

- **深度计数**:`transferDepth` 每跳 +1,`>= maxTransferDepth(8)` 即 return。
  防 A↔B 乒乓,同时防递归栈溢出(委派是真递归 `target.Run`)。
- **返回父级**:父是合法目标,控制权能冒泡回去;靠**共享 Session 历史**这个隐式信道传递上下文。

---

## 8. 工具系统:泛型 + reflect schema(249 行整包)

### 8.1 解决的张力

模型世界无类型(JSON),Go 世界强类型。工具用泛型 + reflect 自动缝合:
开发者只写带类型的函数,框架**自动派生 schema**(给模型)+ **自动编解码**(运行时)。
结构体是唯一真相源,schema 永不漂移。

### 8.2 类型擦除的桥

```go
type Tool interface { Name; Description; Schema; Call }  // 接口无泛型(异构 dispatch)
func New[In, Out any](...) Tool { ... SchemaFor[In]() ... }  // 构造侧有泛型
```

类型只活在 `New` 那一刻(生成 schema),之后在 `funcTool.Call` 内部由 `var in In` 局部复活解码。

```go
func (t *funcTool[In,Out]) Call(ctx, args) (*Result, error) {
    var in In; json.Unmarshal(args, &in)   // JSON → 类型
    out, err := t.fn(ctx, in)              // 类型安全处理器
    if err != nil { return ErrorResult(err.Error()), nil }  // 业务错误回模型,不上抛
    return &Result{Content: renderOutput(out)}, nil          // Out → 文本/JSON
}
```

**错误分流**:工具业务错误 → ErrorResult(进对话,模型可恢复);基础设施错误 → panic/别的通道。

### 8.3 reflect schema 生成

`SchemaFor[T]` → `reflect.TypeFor[T]()` → 递归映射。tag 约定全复用 Go 既有惯例:

| 来源 | 作用 |
|---|---|
| `json:"name"` | wire 名 |
| `json:",omitempty"` | **有=可选;无=required**(语义复用) |
| `json:"-"` | 排除 |
| `desc:"..."` | 字段说明(唯一自定义 tag) |
| 首字母大写 | 仅导出字段入 schema |

reflect 只在建工具时跑一次(存入 funcTool.schema),不进 advertise 热路径。

**局限**:无 enum/min/max/format(需要则手写 schema,如 transferTool);Map 不展开;无自引用保护。

---

## 9. 中间件:能力即中间件

### 9.1 装饰器模型

```go
type Middleware func(next llm.Model) llm.Model
```

turn 引擎只 `middleware.Chain(model, mws...)`,对插了哪些能力**完全无感**。
能力只看 Request/Response,看不到 Event/Session/Actions —— 关注点最小。

两个构造基元:
- **BeforeModel**(请求转换器):改 req 后透传;返回 error 则短路。→ Compaction / RAG / Steering
- **原始 Wrap**(调用控制器):掌控 Generate 调用循环。→ Retry / RateLimit / HITL

### 9.2 Chain 组合与顺序

```go
for i := len(mws)-1; i >= 0; i-- { base = mws[i](base) }  // 逆序包裹
```

`Chain(m, A, B, C)` → `A(B(C(m)))`,**列表第一个 = 最外层**。
推荐顺序 **RateLimit → Compaction → RAG → Retry**,每层位置都有正确性论证:
- RateLimit 最外:门控一切(含重试)。
- Retry 最内:只重跑裸 model 调用,不重跑 compaction 摘要 / RAG 检索。

### 9.3 各中间件要点

| 中间件 | 基元 | 要点 |
|---|---|---|
| **Compaction** | BeforeModel | 超阈值则摘要旧历史;findCut **绝不切断 tool_result 与 tool_call**;独立 summarizer model |
| **RAG** | BeforeModel | 每次调用自动检索注入 system(push);放 memory 包(依赖方向 memory→middleware) |
| **Steering** | BeforeModel | 外部 goroutine 注入消息,下次调用前 drain;不入会话日志 |
| **Retry** | Wrap | **仅 yielded=false 才重试**(已流过则重试会重复内容);ctx 感知退避 |
| **RateLimit** | Wrap | 信号量(并发)+ 虚拟时钟预约(RPS);槽 `defer release` 覆盖整段流 |

---

## 10. HITL:最复杂的中间件

纯作为 model 装饰器在 `Generate` 里**截获并改写**携带 tool call 的 final 响应,turn 引擎无感。

### 10.1 主循环

```go
work := *req; work.Messages = slices.Clone(...)  // 本地副本,不碰引擎 history
work.Messages = append(work.Messages, h.drain()...)  // 注入上轮拒绝反馈
for {
    final := streamCapture(...)        // 转发 partial,扣留 final
    if 无门控 call { yield(final); return }
    rewritten, denials := adjudicate(final)  // 逐 call 审批,重建消息
    if 有 call 存活 {
        stash(denialNote(denials))     // 混合:暂存拒绝提示给引擎下一步
        yield(rewritten); return
    }
    work.Messages = append(work.Messages, final, denialResults(denials))  // 全拒:内部重调
}
```

### 10.2 关键设计

- **借力"call 只在 final"**:partial(纯文本)无审批实时流;tool call 只藏 final,在 final 点裁决。
- **三裁决**:批准(保留)/ 改参批准(替换 Args)/ 拒绝(移除 + 记 denial)。
- **孤儿 tool_use 根除**:
  - 混合回合:被拒 call 移除(不发),denialNote 经 stash/drain 注入引擎下一步。
  - 全拒回合:直接结束会 dead-end 模型 → HITL 自己重调,denialResults **按 CallID 配对**喂回(协议合法)。
- **不持久化**:全程操作 req 克隆,只 yield rewritten;引擎 history 不动。

---

## 11. 记忆与嵌入:向量检索

### 11.1 三层

```
embeddings.Embedder (文本→向量) ← memory.Store (Add+Search) ← SearchTool(pull) / RAG(push)
```

### 11.2 Embedder 契约(检索正确性基石)

```go
Embed(ctx, texts) ([][]float32, error)  // 同方法嵌文档与查询;一文本一向量,顺序对应;同维
```

provider 隔离同 llm 款:核心包零依赖,实现在子包(mock / openaicompat)。

### 11.3 InMemoryStore(暴力库)

- **Add**:一次批量 Embed 所有文档;`len(vecs)==len(docs)` 强校验;位置配对。
- **Search**:嵌查询 → 对**每个**文档算余弦(O(n) 全扫)→ 降序 → top-k。
- 大规模换真向量库(Store 接口隔离),上层不动。

### 11.4 两个 Embedder

- **Mock**:确定性哈希词袋,token→FNV→桶+1→L2 归一化(余弦退化点积);切 ASCII 词 + 逐 CJK 字符(中英双语);无网络可复现。
- **openaicompat**:`POST /embeddings`;**关键按响应 Index 排序**恢复顺序(API 不保证同序,不排会无声错配)。

---

## 12. 全局不变量速查

| 不变量 | 实现处 | 删掉的后果 |
|---|---|---|
| 错误走同一条流 | Event.Err / 各层 yield(nil, err) | 订阅者要为错误单开通道 |
| partial 不落库 | runner `if !ev.Partial` | 流式增量污染历史 |
| 事件即数据 | Actions + commit | 不可重放/审计 |
| tool call 只在 final | streamAgg.snapshot | 残缺 JSON 暴露 |
| 切点不断 tool 配对 | compactor.findCut | provider 报孤儿 tool_result |
| retry 仅 yielded=false | retry.go | 重试重复已流内容 |
| Parallel `<-done` 逃生 | workflow.go select | 消费者早停 → goroutine 泄漏 |
| transfer 深度上限 | maxTransferDepth=8 | A↔B 无限委派 / 栈溢出 |
| HITL CallID 配对 | denialResults | provider 协议非法 |
| embeddings Index 排序 | openaicompat | 检索无声错配 |

---

## 13. 设计来源对照

| 精华 | 来源 |
|---|---|
| `iter.Seq2` 全栈流式 · 事件驱动(Actions)· workflow/transfer · 泛型工具 | google/adk-go |
| 内部消息↔wire 边界转换 · 上下文压缩 · JSONL 持久化 · 统一错误流 | earendil-works/pi |
| 小接口 · functional options · 中心 schema · 决策/执行分离 · provider 子包 | tmc/langchaingo |

---

## 14. 分层职责一句话总结

| 层 | 包 | 职责 | 关键不变量 |
|---|---|---|---|
| L4 编排 | runner | 解析会话·提交事件·选 agent | 只有它持久化;先落库后 yield |
| L3 决策 | agent | LLMAgent turn 引擎 / workflow / transfer | 只决策产事件,不持久化 |
| L2 模型 | llm + middleware | 模型调用 + 横切能力装饰 | 能力即中间件,引擎无感 |
| L1 原语 | core | Message / Event / Stream | 一切皆事件流;事件即数据 |
| 横切 | tool/session/memory/embeddings/callbacks | 可插拔小接口 | 换实现不动核心 |
| provider | anthropic/openaicompat/mock | wire 边界转换 | 收敛到统一 Response/向量 |
