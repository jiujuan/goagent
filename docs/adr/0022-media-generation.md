# ADR 0022：图片/视频生成——能力接口 + 模型即 Agent + 独立后台队列

状态：已接受

## 背景

需要支持图片与视频生成（Agnes Image 2.1 / Video 2.0，后续 gpt-image、Gemini
image、Flux 等）。它们与文本对话有本质差异：

- 请求/响应形态不同（prompt + 可选输入图 → 若干图/一段视频），不进 turn 循环。
- 视频几乎都是**异步长任务**（提交 → 轮询 → 取回），耗时分钟级，需要**进度反馈**。
- 采样参数不同（size/seed/aspect、frames/fps）。

第一版曾把生成做成 `tool.Tool`，但 `tool.Call` 只能返回**单个 `*Result`、无法流式**，
为补回进度被迫加了一层队列/总线——这是抽象错配的信号。

## 决策

分三层，职责单向：

1. **能力接口（接口隔离）**——`llm` 包并列三个接口，互不耦合：
   - `Model`（文本，原有，不动）
   - `ImageModel.GenerateImage(...) iter.Seq2[*ImageResponse, error]`
   - `VideoModel.GenerateVideo(...) iter.Seq2[*VideoProgress, error]`，可选
     `ResumableVideoModel.ResumeVideo(jobID)`（崩溃后凭 JobID 重连，不重复付费）。

   三者都返回 `iter.Seq2`：同步图片只 yield 一次，异步视频把**轮询建模成流**，
   取消/背压/早停复用框架唯一的流原语。provider 子包隔离（`llm/agnes`），延续
   [ADR 0005](0005-provider-isolation.md)。生成结果用新增的 `core.Video` part 承载
   （`core.Image` 已存在），进度用新增的 `core.Event.Progress` 承载。

2. **模型即 Agent（不是 tool）**——`agent.Image(name, m)` / `agent.Video(name, m)`
   包 `llm.ImageModel`/`VideoModel`，正如 `LLMAgent` 包 `llm.Model`，实现 `Agent`
   接口。`Agent.Run` 返回 `core.Stream`，进度流式是**接口白送**的：媒体 Agent 直接
   yield「running 进度事件」+「最终媒体事件」。它能进 `Sequential/Parallel/Loop`、
   作 transfer 目标或 root，与文本 Agent 完全同构。

3. **独立后台队列**——`queue` 包做**通用后台执行器**，只依赖 `core`：
   - `Job{ID, Key string, Run 闭包}`、`Queue`/`MemQueue`、`Bus`/`MemBus`、`Worker`。
   - Worker 只 run + 把进度（Partial）和最终事件 publish 到 Bus，**不碰持久化**。
   - `runner.EnqueueAgent` 桥接 agent↔queue：把一次 agent 运行包成 `Job`，Partial
     事件转发到 Bus、最终事件落 session。这层耦合放在 runner（本就联结 agent 与
     store），queue 自身保持纯净、可复用。

两种用法：**直接流式**（root 或 workflow，进度经 runner 流实时交付，无需 queue）；
**后台 fire-and-forget**（`EnqueueAgent` 入队即返回，前端 `Bus.Subscribe(BusKey)`
收进度直至成品）。

## 理由

- **接口隔离**：文本/图片/视频请求响应形态不同，硬塞一个接口会产生永远为 nil 的字段。
- **agent 优于 tool**：tool 返回单值、无法流式，正是它逼出队列 hack；`Agent.Run` 天生
  流式，长任务进度自然交付。媒体模型与文本模型同为「模型驱动的 Agent」，对称。
- **queue 独立**：只依赖 `core`，不知「媒体」为何物，可承载任意后台工作；持久化经
  job 闭包外置，换 Redis/DB 后端不动 worker。

## 后果

- 放弃了 tool 唯一的长处——**让聊天 LLM 自主决定生成并自拟 prompt**。本设计中生成由
  代码/workflow/前端触发；若将来需要 LLM 自主触发，可用 `transfer_to_agent` 委派给
  媒体 Agent，或另加一个薄 tool，不影响本结构。
- 媒体 Agent 的最终事件为 `RoleAssistant` + 媒体 part。若其后**紧接** Anthropic 文本
  调用，可能出现连续 assistant 消息触碰角色交替——纯媒体流水线无此问题，混合场景可在
  provider 转换层合并同角色消息。
- `MemBus` 对慢订阅者**丢弃**进度（保护 worker 不被卡死）；进度是 advisory，最终结果以
  session 落库为准。
- in-process 队列随进程退出而丢失未完成 job；`ResumableVideoModel` 为持久化后端预留了
  重连能力。

## 备选方案

- **塞进 `llm.Model`**：请求/响应差异大，产生大量空字段与 `if modality` 分支，否决。
- **保留 tool 作主路径**：单值返回与长任务流式进度错配，否决（降级为可选委派/薄 tool）。
- **queue 依赖 session 直接落库**：耦合持久化，丧失通用性；改为 job 闭包外置持久化。
