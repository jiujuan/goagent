# ADR 0027：配置管理系统 —— viper + 强类型 schema，向后兼容旧 env

状态：提议

> 本文给 v2 增加一个**独立的配置管理系统**(顶层 `config` 包)：把散落在各处的
> `os.Getenv` 收敛成"一次加载、强类型字段访问"的单一来源，支持配置文件 + 环境变量 +
> 默认值的优先级合并，并**零破坏**兼容现有的 `AGNES_*` 环境变量。

---

## 背景

到目前为止 goagent 没有配置系统，配置读取散落且重复：

1. **散落**：~15 个 example 里到处是 `os.Getenv("AGNES_API_KEY")`，外加一份**复制粘贴**
   的 `envOr(k, def)` 辅助函数（见 `examples/agent-tutorial/main.go:290`、
   `examples/middleware/main.go:103` 等）。在用的键有 `AGNES_API_KEY` /
   `AGNES_BASE_URL` / `AGNES_MODEL` / `EVAL_LIVE` / redis url。
2. **无文件能力**：无法用配置文件,所有值只能靠环境变量逐个注入。
3. **默认值散落**：`gemini-2.5-flash`、`https://apihub.agnes-ai.com/v1` 这类默认值
   在每个 example 里各写一遍，改一处要改十几处。

值得注意：`.gitignore` 已预留 `config.local.yaml`、`.env`、`secret.json`，说明项目本就
预期一套"committed 默认 + 本地 secret 覆盖"的文件约定——只是一直没落地。

业界参考：viper 是 Go 生态事实标准的配置库，统一处理文件(yaml/json/toml)、环境变量、
默认值、远程配置，并定义清晰的优先级。本项目直接复用它，不自研 loader。

## 决策

新建顶层包 `config`，**只依赖 viper + 标准库**，对外暴露最小 API。

### 一、强类型 schema + 简洁 API

把"配置长什么样"用 Go struct 固定下来，`mapstructure` tag 对应 yaml 键：

```go
type Config struct {
    LLM        LLM        `mapstructure:"llm"`
    Embeddings Embeddings `mapstructure:"embeddings"`
    Redis      Redis      `mapstructure:"redis"`
    Eval       Eval       `mapstructure:"eval"`
}
```

API 贴合本仓库已有风格（`llm/agnes` 的函数式 Option）：

```go
cfg := config.MustLoad()                 // 一行加载
cfg := config.Default()                  // 懒加载全局单例,最简调用
cfg, err := config.Load(config.WithFile("config.yaml"))
cfg.GetString("llm.model")               // 原始 key 透传(struct 未覆盖的临时键)
```

### 二、优先级合并（低 → 高）

```
内置默认值  <  config.yaml  <  config.local.yaml  <  环境变量  <  WithXxx 显式覆盖
```

- 默认值集中在 `setDefaults`,对齐现有 example 取值,改一处即可。
- `config.yaml` committed,`config.local.yaml` 放本地 secret（已 gitignored）。
- 环境变量前缀 `GOAGENT_`（点号→下划线,如 `llm.api_key` → `GOAGENT_LLM_API_KEY`）。
- 缺配置文件不报错——纯 env / 默认值也能运行。

### 三、向后兼容旧 env（零改动迁移）

用 `viper.BindEnv("llm.api_key", "GOAGENT_LLM_API_KEY", "AGNES_API_KEY")` 给每个键绑定
"新前缀键 + 旧键",viper 按序取首个非空。于是现有 example 一行不改,旧的 `AGNES_*` /
`EVAL_LIVE` 继续生效；想迁移的，把 `envOr` 三行换成 `config.Default().LLM.*` 一行即可。

### 四、DAG 不变

`config` 不 import `llm`/`agent`/`eval`，是近叶子包，可被任意上层引用而不形成环。
**模型构造留在调用方**（example 里 `openaicompat.Agnes(cfg.LLM.BaseURL, ...)`），
config 只产出强类型值——避免 config 反向依赖 provider 包。

### 五、两层 config：来源层 vs 装配层

项目里**已有**一类 config：每个包私有的 `type config struct` + 函数式 `Option`
（如 `queue`/factory.go 的 `WithRedis`/`WithStream`/`WithIdleThreshold`，`llm/agnes` 的
`WithBaseURL`）。本 ADR 的 `config` 包**不取代也不接管**它们,二者是**两层**：

| | 顶层 `config` 包（本 ADR） | 各包私有 `config` + `Option`（既有） |
|---|---|---|
| 职责 | 值的**来源**：默认值/yaml/env 按优先级合并 | 值的**装配**：构造参数 + 代码默认 |
| 形态 | exported 强类型字段 | unexported，只通过 `With*` 访问 |
| 调用点 | `main()` 一次 `MustLoad` | 调用方传 `queue.WithRedis(url)` |

两层在 **`main()` 处汇合**，值**单向流动**：`config` → `main()` → `queue.Option`。
关键约束:**两层互不 import**——`config` 不依赖 `queue`（否则被迫依赖每个子系统、破坏
叶子性），`queue` 不依赖 `config`（否则失去干净的 Option API 与可测试性）。由调用方
（如 `examples/redis` 的 `queueOpts(cfg)`）负责把 config 的值映射成 Option。

**哪些值进 config？** 只收"随环境/部署变化、是 secret、或需集中开关"的值：
`redis.url`、各 `api_key`、`eval.live`、queue 的 `stream`/`idle_threshold`/`max_deliveries`
等。纯代码调优默认值留在 `Option`,需要时再提升进 config（加字段 + `setDefaults` 一行,
queue 一字不改）。

## 被否方案

- **继续裸 `os.Getenv`**：无文件能力、默认值散落、无优先级，问题照旧。
- **自研 loader**：要自己实现文件解析 + env 合并 + 优先级,viper 已是成熟标准,无必要。
- **config 内置 `BuildModel()` 直接返回 `llm.Model`**：调用更短，但会让 config 依赖
  `llm/openaicompat`，破坏 DAG 叶子性。取舍后保留"config 只产出值"，构造交给调用方。

## 影响

- 新增 `config/`（`config.go` + `config_test.go` + `README.md`）、`config.example.yaml`、
  `examples/config/`、本 ADR；go.mod 增加 `github.com/spf13/viper`。
- `config.Config` 含 `LLM`/`Embeddings`/`Redis`/`Queue`/`Eval` 五段;`Queue` 段把队列里
  "会随环境变化"的旋钮纳入配置（stream/group/idle_threshold/max_deliveries/max_len）。
- `examples/redis` 改用 config:`os.Getenv("REDIS_URL")` → `config.MustLoad()`,并新增
  `queueOpts(cfg)` 演示"config 值 → queue.Option"的调用方映射(范本)。
- 其余现有代码**零改动**，向后兼容；迁移是可选项，逐处一行。
