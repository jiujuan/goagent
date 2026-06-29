package logger

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Format 是日志的输出格式。
type Format int

const (
	// FormatConsole 是人类可读的彩色行格式,适合本地开发。
	FormatConsole Format = iota
	// FormatJSON 是每行一条 JSON,适合生产采集/检索。
	FormatJSON
)

// Option 配置 New 的构造行为(贴合本仓库 llm/agnes、queue 的函数式 Option 风格)。
type Option func(*options)

type options struct {
	level      zerolog.Level
	format     Format
	out        io.Writer
	timeFormat string
	noColor    bool
	caller     bool
}

func defaults() options {
	return options{
		level:      zerolog.InfoLevel,
		format:     FormatConsole,
		out:        os.Stderr,
		timeFormat: time.RFC3339,
	}
}

// WithLevel 设置最低输出级别:debug|info|warn|error|fatal|panic|trace(大小写不敏感)。
// 无法识别的字符串被忽略,保持默认 info。
func WithLevel(level string) Option {
	return func(o *options) {
		if lvl, err := zerolog.ParseLevel(strings.ToLower(strings.TrimSpace(level))); err == nil {
			o.level = lvl
		}
	}
}

// WithFormat 设置输出格式:"console"(默认)或 "json"。其它值保持默认。
func WithFormat(format string) Option {
	return func(o *options) {
		switch strings.ToLower(strings.TrimSpace(format)) {
		case "json":
			o.format = FormatJSON
		case "console", "":
			o.format = FormatConsole
		}
	}
}

// WithOutput 设置日志写入目标(默认 os.Stderr)。字符串 "stderr"/"stdout"/文件路径到
// io.Writer 的解析交给调用方(见 examples/logger 的 resolveOutput)。
func WithOutput(w io.Writer) Option {
	return func(o *options) {
		if w != nil {
			o.out = w
		}
	}
}

// WithTimeFormat 设置时间戳格式(time 包的布局字符串,默认 time.RFC3339)。
func WithTimeFormat(layout string) Option {
	return func(o *options) {
		if layout != "" {
			o.timeFormat = layout
		}
	}
}

// WithNoColor 在 console 格式下关闭 ANSI 颜色(输出到文件或不支持颜色的终端时用)。
func WithNoColor(noColor bool) Option {
	return func(o *options) { o.noColor = noColor }
}

// WithCaller 在每条日志附带调用方文件:行号(有性能开销,默认关闭)。
func WithCaller(enable bool) Option {
	return func(o *options) { o.caller = enable }
}
