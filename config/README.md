# config —— 配置管理系统(基于 spf13/viper)

把项目里散落、重复的 `os.Getenv("AGNES_API_KEY")` + 复制粘贴的 `envOr(k, def)`,
收敛成**一次加载、强类型字段访问**的单一配置源。

```go
cfg := config.MustLoad()
model := openaicompat.Agnes(cfg.LLM.BaseURL, cfg.LLM.Model, cfg.LLM.APIKey)
```

## 优先级(低 → 高)

| 来源 | 说明 |
|------|------|
| 内置默认值 | 代码里的 `SetDefault`,对齐现有 example 取值 |
| `config.yaml` | committed 默认配置(搜索 `.`、`./config`、`$HOME/.goagent`) |
| `config.local.yaml` | 本地覆盖,放 secret(已在 `.gitignore`) |
| 环境变量 | `GOAGENT_*`,并兼容旧 `AGNES_*` / `EVAL_LIVE` / `REDIS_URL` |
| `WithXxx` 选项 | 代码里显式覆盖(优先级最高) |

缺配置文件不报错——纯环境变量 / 默认值也能跑。

## 键名映射

struct 字段、yaml 键、环境变量三者一一对应(点号 → 下划线,加前缀)。
带 `*` 的列出了向后兼容的旧环境变量,**无需改动现有代码**即继续生效。

| struct 字段 | yaml 键 | 环境变量(`GOAGENT_` 前缀) | 兼容旧变量 |
|------------|---------|--------------------------|-----------|
| `LLM.Provider` | `llm.provider` | `GOAGENT_LLM_PROVIDER` | |
| `LLM.BaseURL` | `llm.base_url` | `GOAGENT_LLM_BASE_URL` | `AGNES_BASE_URL` |
| `LLM.APIKey` | `llm.api_key` | `GOAGENT_LLM_API_KEY` | `AGNES_API_KEY` |
| `LLM.Model` | `llm.model` | `GOAGENT_LLM_MODEL` | `AGNES_MODEL` |
| `Embeddings.APIKey` | `embeddings.api_key` | `GOAGENT_EMBEDDINGS_API_KEY` | `AGNES_API_KEY` |
| `Redis.URL` | `redis.url` | `GOAGENT_REDIS_URL` | `REDIS_URL` |
| `Queue.Stream` | `queue.stream` | `GOAGENT_QUEUE_STREAM` | |
| `Queue.Group` | `queue.group` | `GOAGENT_QUEUE_GROUP` | |
| `Queue.IdleThreshold` | `queue.idle_threshold` | `GOAGENT_QUEUE_IDLE_THRESHOLD` | |
| `Queue.MaxDeliveries` | `queue.max_deliveries` | `GOAGENT_QUEUE_MAX_DELIVERIES` | |
| `Queue.MaxLen` | `queue.max_len` | `GOAGENT_QUEUE_MAX_LEN` | |
| `Eval.Live` | `eval.live` | `GOAGENT_EVAL_LIVE` | `EVAL_LIVE` |

> 新旧变量同时存在时,`GOAGENT_` 前缀键优先。
> `idle_threshold` 是时长,写 `"5m"` / `"90s"`(yaml、env、默认值都用字符串,自动解析为 `time.Duration`)。

## 两层 config:本包 vs 各包的 Option

项目里有**两种 config,是两层,职责不同,刻意不互相 import**:

| | **本包(顶层 `config`)** | **各包私有的 `config` + `Option`**(如 `queue`、`llm/agnes`) |
|---|---|---|
| 管什么 | 值的**来源**:默认值 / yaml / env,按优先级合并 | 值的**装配**:构造参数 + 代码默认值 |
| 形态 | exported 强类型字段 | unexported struct,只能通过 `With*` 碰 |
| 谁调 | `main()` 里 `config.MustLoad()` | 调用方传 `queue.WithRedis(url)` 等 |

两层在 **`main()` 处汇合**,而不是互相依赖——值**单向流动**:

```
config(产出强类型值) → main()(粘合) → queue.Option(装配)
```

本包**不** import `queue`(否则就被迫依赖每个子系统,破坏 DAG 叶子性);
`queue` 也**不** import 本包(否则失去干净的 Option API 与可测试性)。
调用方负责把 config 的值映射成 Option,例如 [`examples/redis`](../examples/redis/main.go) 里的
`queueOpts(cfg)`:

```go
cfg := config.MustLoad()
q, c, _ := queue.New(append(queueOpts(cfg), queue.WithGroup("demo-queue"))...)
```

**什么值才进本包?** 只收"随环境/部署变化、是 secret、或需集中开关"的值
(URL、key、`eval.live`、queue 的 stream/重试阈值)。纯代码调优(如进程内队列 `mem_size`
的极少改动场景)留在 `Option` 的默认值即可。

## API

```go
func Load(opts ...Option) (*Config, error) // 主入口
func MustLoad(opts ...Option) *Config       // main() 里用,出错 panic
func Default() *Config                       // 懒加载全局单例,最简调用

func (c *Config) GetString/GetInt/GetBool/Get(key string) // 原始 key 透传

// Options
func WithFile(path string) Option        // 指定文件,跳过搜索
func WithName(name string) Option         // 文件基名,默认 "config"
func WithEnvPrefix(prefix string) Option  // 默认 "GOAGENT"
func WithPath(paths ...string) Option     // 追加搜索目录
func WithDefault(key string, val any) Option
```

## 扩展新配置域

在 `Config` 上加一个带 `mapstructure` tag 的字段,需要默认值就在 `setDefaults`
里加一行,需要兼容旧 env 就在 `bindLegacyEnv` 里加一行 `BindEnv`。无需改调用方。

## DAG 位置

本包只依赖 `viper` + 标准库,不 import `llm`/`agent`/`eval`,因此可被任意上层包引用
而不破坏现有分层。模型构造留在调用方,config 只产出强类型值。

## 演示

见 [`examples/config`](../examples/config),覆盖默认值 / 文件 / 环境变量 / 旧变量兼容
四条优先级路径。设计动机与取舍见 [ADR 0027](../docs/adr/0027-config-system.md)。
