# ADR 0028：独立日志系统 —— zerolog + 薄 facade,接入 config 与 middleware

状态：提议

> 本文给 v2 增加一个**独立的日志系统**(顶层 `logger` 包):以 rs/zerolog 为底层,
> 对外只暴露 `*Logger` / `*Event` 两个自有薄封装类型 + 逃生口,把核心库里零散的
> `fmt.Print*` / 标准库 `log` 收敛成统一、结构化、可配置的日志出口,并通过 config 的
> `Log` 段驱动、用适配器无侵入接入既有的 `middleware.Tracing`。

---

## 背景

到目前为止 goagent 没有日志系统,输出手段零散且不可控:

1. **散落且无结构**:`examples/` 里 ~190 处 `fmt.Print*` + 83 处标准库 `log.`,没有级别、
   没有结构化字段、没有统一格式。核心库(agent/core/llm/...)几乎不打日志,出问题只能靠
   `fmt` 临时插桩。
2. **唯一的框架级日志点**是 `middleware/tracing.go`:它定义了一个极简接口
   `Logger interface { Printf(format string, args ...any) }`,默认用 `log.Default()`——
   能换 sink,但仍是无结构的 `Printf`。
3. **无级别 / 无格式切换**:开发想要彩色可读、生产想要 JSON 采集,当前都做不到;也没法
   按级别过滤噪声。

业界参考:`rs/zerolog` 是 Go 生态零分配、结构化日志的事实标准之一(API 链式、性能优于
logrus,定位与 zap 相近但更轻)。本项目直接复用,不自研 logger。

## 决策

新建顶层包 `logger`,**只依赖 zerolog + 标准库**,对外暴露最小 API。

### 一、薄 facade + 逃生口(可替换)

不直接暴露 zerolog 类型,而是包两层薄封装:

```go
type Logger struct { z zerolog.Logger }   // 构造/派生/全局
type Event  struct { e *zerolog.Event }   // 链式字段 + Msg/Send 收尾
```

调用方代码只依赖 `logger.*`:

```go
logger.L().Info().Str("run_id", id).Int("turn", n).Msg("model replied")
```

`*Event` 只转发最常用字段方法(Str/Int/Bool/Dur/Err/Any/Fields...),冷门能力走逃生口
`Event.Z() *zerolog.Event` / `Logger.Zerolog() *zerolog.Logger`。**价值**:日后若要替换
底层实现,改动收敛在 logger 包内,调用点不动。代价是少量转发样板——可接受。

### 二、全局单例 + 显式注入并存

对齐 `config.Default()` 的懒加载单例风格:

```go
logger.L()                       // 懒初始化的全局,最简调用
logger.SetDefault(l)             // main() 里用 config 值构造后设一次
logger.Info().Msg(...)           // 等价 logger.L().Info()
l := logger.New(opts...)         // 也可纯显式,不碰全局(测试友好)
```

### 三、config 驱动(两层 config,与 queue 同款)

顶层 `config` 包新增 `Log` 段(`level`/`format`/`output`/`time_format`/`caller`),
负责"值的**来源**";`logger` 包的函数式 `Option`(`WithLevel`/`WithFormat`...)负责
"值的**装配**"。两层在 `main()` 汇合,值**单向流动**:`config` → `main()` →
`logger.Option`。**关键约束:两层互不 import**——`config` 不依赖 `logger`(否则破坏
叶子性),`logger` 不依赖 `config`(否则失去干净 Option API 与可测试性)。由调用方
(`examples/logger` 的 `loggerOpts(cfg)`)负责映射,字符串 `output` →`io.Writer` 的
解析(stderr/stdout/文件)也留在调用方。

```
内置默认值  <  config.yaml  <  config.local.yaml  <  环境变量(GOAGENT_LOG_*)  <  WithXxx
```

### 四、无侵入接入 middleware.Tracing

`logger.Printf(l)` 返回一个满足 `middleware.Logger`(`Printf`)的适配器,把 Tracing 的
格式化日志桥接到 zerolog(Info 级):

```go
mw := middleware.Tracing(logger.Printf(logger.L()))
```

**`middleware/tracing.go` 一字不改**:适配器靠结构化接口匹配,`logger` 不 import
`middleware`、`middleware` 也不 import `logger`,避免双向耦合。Tracing 的 `nil` 默认仍是
`log.Default()`,接入与否由调用方决定。

### 五、context 流转

zerolog 原生支持把 logger 存入 context,本包包一层 facade:

```go
ctx = logger.Into(ctx, logger.L().WithStr("run_id", id))
logger.From(ctx).Info().Msg("...")   // 深层自动带上 run_id;ctx 无 logger 时返回禁用态
```

项目里 `context.Context` 已贯穿全栈(RunContext 内嵌 Context),这让"一次运行的字段随
ctx 下沉"成为零成本的自然写法。

### 六、DAG 不变

`logger` 不 import `core`/`agent`/`llm`/`config`/`middleware`,是近叶子包,可被任意上层
引用而不形成环。

## 被否方案

- **继续 `fmt.Print*` / 标准库 `log`**:无级别、无结构、无格式切换,问题照旧。
- **直接暴露 `*zerolog.Logger`(零封装)**:最省事,但全项目硬绑定 zerolog 类型,日后
  替换要改所有调用点。取舍后保留薄 facade,把替换风险收敛进 logger 包。
- **让 `middleware.Tracing(nil)` 默认走 zerolog**:调用更短,但会让 middleware 反向依赖
  logger。取舍后用结构化接口适配器,保持两包零耦合。
- **自研 logger**:级别/结构化/采样/零分配都要自己实现,zerolog 已是成熟标准,无必要。
- **用标准库 `log/slog`**:可行且无第三方依赖,但本项目优先 zerolog 的链式 API 与
  console/JSON 双格式开箱体验;若团队倾向零依赖,slog 可作为后续替换目标(facade 正为此留口)。

## 影响

- 新增 `logger/`(`logger.go` + `event.go` + `config.go` + `context.go` + `adapter.go` +
  `logger_test.go`)、`examples/logger/`、本 ADR;go.mod 增加 `github.com/rs/zerolog`
  (及其传递依赖 go-colorable / go-isatty)。
- `config.Config` 增 `Log` 段(level/format/output/time_format/caller)+ `setDefaults` 默认值;
  `config.example.yaml` 增 `log:` 示例。`GOAGENT_LOG_*` 环境变量经 AutomaticEnv 自动生效。
- 现有代码**零改动**,向后兼容;核心库与 examples 的 `fmt`/`log` 迁移是**后续可选项**,
  逐处替换,不在本 ADR 范围。
