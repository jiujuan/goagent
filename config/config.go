// Package config 是 goagent 的独立配置管理系统，基于 spf13/viper。
//
// 设计目标:一次加载、强类型字段访问,替代项目里散落的 os.Getenv +
// 复制粘贴的 envOr 辅助函数。配置按优先级(低→高)合并:
//
//	内置默认值  <  config.yaml  <  config.local.yaml  <  环境变量  <  WithXxx 显式覆盖
//
// config.local.yaml 与 .env 已在 .gitignore 中,用于放本地 secret。环境变量
// 默认前缀 GOAGENT_(键名点号映射为下划线,如 llm.api_key → GOAGENT_LLM_API_KEY);
// 为向后兼容,旧的 AGNES_API_KEY / AGNES_BASE_URL / AGNES_MODEL / EVAL_LIVE /
// REDIS_URL 仍然生效,无需改动现有代码。
//
// 本包是近叶子包,只依赖 viper 与标准库,不 import llm/agent/eval,因此可被任意
// 上层包引用而不破坏现有分层(DAG)。模型构造留在调用方,config 只产出强类型值。
//
// 最简用法:
//
//	cfg := config.MustLoad()
//	model := openaicompat.Agnes(cfg.LLM.BaseURL, cfg.LLM.Model, cfg.LLM.APIKey)
package config

import (
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
)

// Config 是解析后的强类型配置。按项目实际用到的域分组,新增域时在此追加字段即可。
// 内嵌的 viper 实例供 GetString 等原始访问(读 struct 未覆盖的临时键)。
type Config struct {
	v *viper.Viper

	LLM        LLM        `mapstructure:"llm"`
	Embeddings Embeddings `mapstructure:"embeddings"`
	Redis      Redis      `mapstructure:"redis"`
	Queue      Queue      `mapstructure:"queue"`
	Eval       Eval       `mapstructure:"eval"`
	Log        Log        `mapstructure:"log"`
}

// LLM 是聊天/推理模型的配置。Provider 仅作标识,实际选择哪个 provider 由调用方决定。
type LLM struct {
	Provider string `mapstructure:"provider"` // openai | deepseek | agnes | anthropic
	BaseURL  string `mapstructure:"base_url"`
	APIKey   string `mapstructure:"api_key"`
	Model    string `mapstructure:"model"`
}

// Embeddings 是向量化模型的配置(可与 LLM 用不同 key/host)。
type Embeddings struct {
	BaseURL string `mapstructure:"base_url"`
	APIKey  string `mapstructure:"api_key"`
	Model   string `mapstructure:"model"`
}

// Redis 是 queue / bus 等用到的 Redis 连接配置。
type Redis struct {
	URL string `mapstructure:"url"` // redis://host:port/db
}

// Queue 是任务队列(queue 包)的可调旋钮——只收纳"会随环境/部署变化"的那部分,
// 默认值与 queue 包内的 defaults() 对齐。调用方在 main() 里把这些值映射到 queue 的
// 函数式 Option(queue.WithStream 等)注入;config 不 import queue,保持单向依赖。
// 留空的字段(如 DeadStream)交给 queue 自己推导,不在此强制覆盖。
type Queue struct {
	Stream        string        `mapstructure:"stream"`         // Redis stream key
	Group         string        `mapstructure:"group"`         // 消费组
	DeadStream    string        `mapstructure:"dead_stream"`    // 死信流,空则 queue 用 <stream>:dead
	IdleThreshold time.Duration `mapstructure:"idle_threshold"` // 未 ack 多久后重投,如 "5m"
	MaxDeliveries int           `mapstructure:"max_deliveries"` // 超过则进死信
	MaxLen        int64         `mapstructure:"max_len"`        // MAXLEN ~ 近似上限
	MemSize       int           `mapstructure:"mem_size"`       // 进程内队列缓冲(非 Redis 时)
}

// Eval 是评估系统的开关。Live 为 true 时用真实模型,否则离线 mock。
type Eval struct {
	Live bool `mapstructure:"live"`
}

// Log 是日志系统(logger 包)的配置。调用方在 main() 里把这些值映射成 logger 的
// 函数式 Option(logger.WithLevel 等)注入;config 不 import logger,保持单向依赖。
type Log struct {
	Level      string `mapstructure:"level"`       // debug | info | warn | error
	Format     string `mapstructure:"format"`      // console(开发) | json(生产)
	Output     string `mapstructure:"output"`      // stderr | stdout | 文件路径
	TimeFormat string `mapstructure:"time_format"` // 时间戳布局,空则 logger 用 RFC3339
	Caller     bool   `mapstructure:"caller"`      // 是否附带调用方文件:行号
}

// Option 配置 Load 的加载行为(贴合本仓库 llm/agnes 的函数式 Option 风格)。
type Option func(*options)

type options struct {
	file      string         // 显式文件路径,设了就跳过搜索
	name      string         // 配置文件基名,默认 "config"
	envPrefix string         // 环境变量前缀,默认 "GOAGENT"
	paths     []string       // 额外搜索目录
	defaults  map[string]any // 额外默认值
}

// WithFile 指定一个确切的配置文件路径,跳过按名搜索(也跳过 .local 合并)。
func WithFile(path string) Option { return func(o *options) { o.file = path } }

// WithName 设置配置文件基名(不含扩展名),默认 "config"。
func WithName(name string) Option { return func(o *options) { o.name = name } }

// WithEnvPrefix 设置环境变量前缀,默认 "GOAGENT"。
func WithEnvPrefix(prefix string) Option { return func(o *options) { o.envPrefix = prefix } }

// WithPath 追加配置文件搜索目录(默认已含 ".", "./config", "$HOME/.goagent")。
func WithPath(paths ...string) Option {
	return func(o *options) { o.paths = append(o.paths, paths...) }
}

// WithDefault 注入一个默认值(优先级最低,会被文件/env 覆盖)。key 用点号分隔,
// 如 "llm.model"。
func WithDefault(key string, val any) Option {
	return func(o *options) {
		if o.defaults == nil {
			o.defaults = map[string]any{}
		}
		o.defaults[key] = val
	}
}

// Load 按优先级合并默认值、配置文件与环境变量,返回强类型 Config。
// 配置文件缺失不是错误(纯 env / 默认值也能运行)。
func Load(opts ...Option) (*Config, error) {
	o := options{
		name:      "config",
		envPrefix: "GOAGENT",
		paths:     []string{".", "./config", "$HOME/.goagent"},
	}
	for _, opt := range opts {
		opt(&o)
	}

	v := viper.New()

	// 1) 内置默认值(对齐现有 example 的取值),再叠加 WithDefault。
	setDefaults(v)
	for k, val := range o.defaults {
		v.SetDefault(k, val)
	}

	// 2) 配置文件:config.yaml,然后合并 config.local.yaml(本地覆盖)。
	if o.file != "" {
		v.SetConfigFile(o.file)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	} else {
		v.SetConfigName(o.name)
		v.SetConfigType("yaml")
		for _, p := range o.paths {
			v.AddConfigPath(p)
		}
		if err := v.ReadInConfig(); err != nil {
			if _, notFound := err.(viper.ConfigFileNotFoundError); !notFound {
				return nil, err
			}
		}
		mergeLocal(v, o)
	}

	// 3) 环境变量:GOAGENT_ 前缀(点号→下划线)自动绑定,再显式绑定向后兼容的旧键。
	v.SetEnvPrefix(o.envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	bindLegacyEnv(v, o.envPrefix)

	var c Config
	if err := v.Unmarshal(&c); err != nil {
		return nil, err
	}
	c.v = v
	return &c, nil
}

// MustLoad 同 Load,出错时 panic。适合在 main() 里一行加载。
func MustLoad(opts ...Option) *Config {
	c, err := Load(opts...)
	if err != nil {
		panic("config: " + err.Error())
	}
	return c
}

var (
	defaultOnce sync.Once
	defaultCfg  *Config
)

// Default 返回懒加载的全局单例(用默认 Option)。最简调用方式:
//
//	model := config.Default().LLM.Model
//
// 首次调用解析失败会 panic;需要自定义 Option 或处理错误时改用 Load。
func Default() *Config {
	defaultOnce.Do(func() { defaultCfg = MustLoad() })
	return defaultCfg
}

// GetString 读取任意点号分隔键的字符串值(struct 未覆盖的临时键用)。
func (c *Config) GetString(key string) string { return c.v.GetString(key) }

// GetInt 读取任意键的整数值。
func (c *Config) GetInt(key string) int { return c.v.GetInt(key) }

// GetBool 读取任意键的布尔值。
func (c *Config) GetBool(key string) bool { return c.v.GetBool(key) }

// Get 读取任意键的原始值。
func (c *Config) Get(key string) any { return c.v.Get(key) }

// setDefaults 写入内置默认值,取值对齐现有 example(buildModel 里的 envOr 默认)。
func setDefaults(v *viper.Viper) {
	v.SetDefault("llm.provider", "agnes")
	v.SetDefault("llm.base_url", "https://apihub.agnes-ai.com/v1")
	v.SetDefault("llm.model", "gemini-2.5-flash")
	v.SetDefault("redis.url", "redis://localhost:6379")
	v.SetDefault("eval.live", false)

	// 日志默认值,与 logger 包内 defaults() 对齐(logger/config.go)。
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "console")
	v.SetDefault("log.output", "stderr")
	v.SetDefault("log.caller", false)

	// queue 旋钮默认值,与 queue 包内 defaults() 一致(queue/factory.go)。
	v.SetDefault("queue.stream", "goagent:jobs")
	v.SetDefault("queue.group", "workers")
	v.SetDefault("queue.idle_threshold", "5m")
	v.SetDefault("queue.max_deliveries", 3)
	v.SetDefault("queue.max_len", 100_000)
	v.SetDefault("queue.mem_size", 16)
}

// mergeLocal 在搜索路径中查找 <name>.local.yaml 并合并(优先级高于主文件)。缺失忽略。
func mergeLocal(v *viper.Viper, o options) {
	lv := viper.New()
	lv.SetConfigName(o.name + ".local")
	lv.SetConfigType("yaml")
	for _, p := range o.paths {
		lv.AddConfigPath(p)
	}
	if err := lv.ReadInConfig(); err == nil {
		_ = v.MergeConfigMap(lv.AllSettings())
	}
}

// bindLegacyEnv 把 struct 键显式绑定到 [前缀键, 旧键],viper 按序取首个非空,
// 从而在不改动现有代码的前提下兼容旧的 AGNES_* / EVAL_LIVE / REDIS_URL。
func bindLegacyEnv(v *viper.Viper, prefix string) {
	p := prefix + "_"
	_ = v.BindEnv("llm.api_key", p+"LLM_API_KEY", "AGNES_API_KEY")
	_ = v.BindEnv("llm.base_url", p+"LLM_BASE_URL", "AGNES_BASE_URL")
	_ = v.BindEnv("llm.model", p+"LLM_MODEL", "AGNES_MODEL")
	_ = v.BindEnv("embeddings.api_key", p+"EMBEDDINGS_API_KEY", "AGNES_API_KEY")
	_ = v.BindEnv("eval.live", p+"EVAL_LIVE", "EVAL_LIVE")
	_ = v.BindEnv("redis.url", p+"REDIS_URL", "REDIS_URL")
}
