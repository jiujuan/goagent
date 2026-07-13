# 配置管理系统（config 包，基于 spf13/viper）

## Context（为什么做这件事）

当前 goagent 的配置读取是**散落且重复**的：~15 个 example 里到处是
`os.Getenv("AGNES_API_KEY")` 加一份复制粘贴的 `envOr(k, def)` 辅助函数
（见 `examples/agent-tutorial/main.go:290`、`examples/middleware/main.go:103` 等）。
在用的键有 `AGNES_API_KEY` / `AGNES_BASE_URL` / `AGNES_MODEL` / `EVAL_LIVE` / redis url。

问题：没有单一配置源、没有配置文件能力、默认值散落在各处、无法集中覆盖。
`.gitignore` 已预留 `config.local.yaml`、`.env`、`secret.json`，说明项目本就预期一套
"committed 默认 + 本地 secret 覆盖"的文件约定。

目标：新增一个**独立 `config` 顶层包**，基于 viper，提供简洁 API；
让读配置从"到处 os.Getenv"变成"一次 Load、强类型字段访问"，且**向后兼容**——
旧的 `AGNES_*` 环境变量零破坏继续生效。

## 设计要点

**DAG 位置**：`config` 是近叶子包，**只依赖 viper + 标准库**（不 import llm/agent/eval），
所以可被任意上层包引用而不破坏现有分层。模型构造仍留在调用方（example），config 只产出强类型值。

**加载优先级（低→高）**：
`代码内置默认值` < `config.yaml` < `config.local.yaml`(gitignored 本地覆盖) < `环境变量`
（`GOAGENT_*`，并兼容旧 `AGNES_*` / `EVAL_LIVE`） < `WithXxx` 显式覆盖。

**文件搜索路径**：`.`、`./config`、`$HOME/.goagent`；缺文件不报错（纯 env/默认值也能跑）。

## 实现

### 1. go.mod：新增依赖
`go get github.com/spf13/viper@latest`（go 1.24 兼容）。

### 2. 新文件 `config/config.go`（核心，~150 行）

强类型 schema（按项目实际用到的域，可扩展）：
```go
type Config struct {
    v          *viper.Viper // 底层实例，供原始 key 访问
    LLM        LLM        `mapstructure:"llm"`
    Embeddings Embeddings `mapstructure:"embeddings"`
    Redis      Redis      `mapstructure:"redis"`
    Eval       Eval       `mapstructure:"eval"`
}
type LLM struct {
    Provider, BaseURL, APIKey, Model string `mapstructure:"..."`
}
// Embeddings{BaseURL,APIKey,Model} / Redis{URL} / Eval{Live bool}
```

简洁 API：
```go
func Load(opts ...Option) (*Config, error) // 主入口
func MustLoad(opts ...Option) *Config       // main() 里用，出错 panic
func Default() *Config                       // 懒加载全局单例(sync.Once)，最简调用

// 原始 key 透传（struct 没覆盖的临时键）
func (c *Config) GetString/GetInt/GetBool(key string) ...

// Options（函数式选项，贴合本仓库 llm/agnes 的 Option 风格）
func WithFile(path string) Option        // 指定文件，跳过搜索
func WithName(name string) Option         // 文件基名，默认 "config"
func WithEnvPrefix(prefix string) Option  // 默认 "GOAGENT"
func WithPath(paths ...string) Option     // 追加搜索目录
func WithDefault(key string, val any) Option
```

`Load` 内部 viper 装配顺序：
1. `setDefaults(v)` —— 内置默认值，对齐现有 example：
   `llm.base_url=https://apihub.agnes-ai.com/v1`、`llm.model=gemini-2.5-flash`、
   `llm.provider=agnes`、`redis.url=redis://localhost:6379`、`eval.live=false`。
2. 读 `config.yaml`（`ConfigFileNotFoundError` 忽略），再 `MergeConfigMap` 合并 `config.local.yaml`。
3. `SetEnvPrefix("GOAGENT")` + `SetEnvKeyReplacer(.→_)` + `AutomaticEnv()`；
   再用 `v.BindEnv` 显式绑定**向后兼容**旧键（多名按序取首个非空）：
   `llm.api_key ← GOAGENT_LLM_API_KEY, AGNES_API_KEY`；
   `llm.base_url ← ..., AGNES_BASE_URL`；`llm.model ← ..., AGNES_MODEL`；
   `eval.live ← ..., EVAL_LIVE`；`redis.url ← ..., REDIS_URL`。
4. `v.Unmarshal(&c)`，存 `c.v = v` 供原始访问。

### 3. `config/config_test.go`
覆盖：默认值生效；`GOAGENT_LLM_MODEL` 覆盖（`t.Setenv`）；旧 `AGNES_MODEL` 仍生效；
`WithFile` 读临时 yaml；优先级 env > file。

### 4. `config/README.md`
包文档（对齐 `eval/README.md` 约定）：优先级表、键名映射表（struct 字段 ↔ yaml key ↔ env）、最小用法。

### 5. `config.example.yaml`（仓库根）
带注释的样例配置，提示用户复制为 `config.local.yaml` 填 secret。

### 6. demo `examples/config/main.go` + `examples/config/config.yaml`
演示：
- `cfg := config.MustLoad()` 一行加载；
- 打印解析结果与来源（默认/文件/env 哪个生效）；
- **"少量改造"对照**：旧三行 `envOr` → 新一行
  `model := openaicompat.Agnes(cfg.LLM.BaseURL, cfg.LLM.Model, cfg.LLM.APIKey)`；
- 原始访问 `cfg.GetString("llm.model")`；
- `config.Load(config.WithFile("examples/config/config.yaml"))` 演示文件加载。

### 7. ADR `docs/adr/0027-config-system.md`
中文 ADR（对齐仓库每子系统一篇的约定）：背景、决策（viper + 强类型 + 向后兼容）、
DAG 不变性、优先级、被否方案（裸 env / 自研 loader）。

## 现有代码改动
**零改动**（向后兼容）。现有 example 的 `AGNES_*` env 经 `BindEnv` 继续生效；
迁移是可选项，调用方把 `envOr` 三行换成 `config.Default().LLM.*` 即可，一处一行。

## 验证
1. `go build ./config/... && go test ./config/...` —— 单测过（默认值/env 覆盖/文件/优先级/旧键兼容）。
2. `go run ./examples/config` —— 无文件无 env 时打印内置默认值；
   `GOAGENT_LLM_MODEL=foo go run ./examples/config` 显示被覆盖；
   `AGNES_MODEL=bar go run ./examples/config` 验证旧键仍生效。
3. `go build ./...` —— 确认未破坏任何包（DAG 不变）。