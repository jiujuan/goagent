// Package logger 是 goagent 的独立日志系统,基于 rs/zerolog。
//
// 设计目标:给框架一个统一、结构化、可配置的日志出口,替代核心库里零散的
// fmt.Print* 与标准库 log。本包是**薄 facade**:对外暴露 *Logger / *Event 两个
// 自有类型(只转发到 zerolog),并保留 Zerolog() / Z() 逃生口拿原生对象。好处是
// 调用方代码只依赖本包类型,日后若换底层实现,改动收敛在 logger 包内。
//
// 本包是近叶子包,只依赖 zerolog 与标准库,**不 import** core/agent/llm/config,
// 因此可被任意上层引用而不破坏分层(DAG)。配置值的装配遵循项目的"两层 config":
// 顶层 config 包产出 cfg.Log 强类型值,调用方在 main() 里映射成 logger 的函数式
// Option(WithLevel/WithFormat...);logger 与 config 互不 import。
//
// 最简用法:
//
//	logger.L().Info().Str("run_id", id).Int("turn", n).Msg("model replied")
//
// 显式构造并设为全局:
//
//	logger.SetDefault(logger.New(logger.WithLevel("debug"), logger.WithFormat("json")))
package logger

import (
	"io"
	"sync"

	"github.com/rs/zerolog"
)

// Logger 是对 zerolog.Logger 的薄封装。方法返回 *Event(同样是薄封装),链式追加
// 字段后以 Msg/Send 收尾。零值不可用,请用 New 构造。
type Logger struct {
	z zerolog.Logger
}

// New 按 Option 构造一个 Logger。无 Option 时:info 级、console 格式、写 stderr。
func New(opts ...Option) *Logger {
	o := defaults()
	for _, opt := range opts {
		opt(&o)
	}

	var w io.Writer = o.out
	switch o.format {
	case FormatConsole:
		w = zerolog.ConsoleWriter{Out: o.out, TimeFormat: o.timeFormat, NoColor: o.noColor}
	case FormatJSON:
		// json 模式下时间字段格式由 zerolog 的包级变量控制(全局,仅影响时间戳渲染)。
		zerolog.TimeFieldFormat = o.timeFormat
	}

	zc := zerolog.New(w).Level(o.level).With().Timestamp()
	if o.caller {
		zc = zc.Caller()
	}
	return &Logger{z: zc.Logger()}
}

// Debug/Info/Warn/Error 开启对应级别的一条日志事件。若该级别被当前 Logger 过滤,
// 返回的 *Event 为禁用态,后续链式调用与 Msg 都是廉价 no-op(zerolog 保证)。
func (l *Logger) Debug() *Event { return &Event{l.z.Debug()} }
func (l *Logger) Info() *Event  { return &Event{l.z.Info()} }
func (l *Logger) Warn() *Event  { return &Event{l.z.Warn()} }
func (l *Logger) Error() *Event { return &Event{l.z.Error()} }

// WithFields 返回一个派生 Logger,其后每条日志都带上这些固定字段(如 run_id)。
func (l *Logger) WithFields(fields map[string]any) *Logger {
	return &Logger{z: l.z.With().Fields(fields).Logger()}
}

// WithStr 是单字段版 WithFields 的便捷写法。
func (l *Logger) WithStr(key, val string) *Logger {
	return &Logger{z: l.z.With().Str(key, val).Logger()}
}

// Zerolog 是逃生口:拿到底层 *zerolog.Logger 用 facade 未转发的高级能力。
func (l *Logger) Zerolog() *zerolog.Logger { return &l.z }

var (
	mu  sync.RWMutex
	def *Logger
)

// L 返回全局默认 Logger。首次调用且未 SetDefault 时,懒初始化为 New()(对齐
// config.Default 的懒加载单例风格)。并发安全。
func L() *Logger {
	mu.RLock()
	if d := def; d != nil {
		mu.RUnlock()
		return d
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()
	if def == nil {
		def = New()
	}
	return def
}

// SetDefault 替换全局默认 Logger(通常在 main() 里用 config 值构造后调用一次)。
func SetDefault(l *Logger) {
	mu.Lock()
	def = l
	mu.Unlock()
}

// 便捷透传:logger.Info() 等价 logger.L().Info(),省去显式取全局。
func Debug() *Event { return L().Debug() }
func Info() *Event  { return L().Info() }
func Warn() *Event  { return L().Warn() }
func Error() *Event { return L().Error() }

// Discard 返回一个丢弃所有输出的 Logger,测试里替换全局以静音日志很方便。
func Discard() *Logger { return &Logger{z: zerolog.New(io.Discard)} }
