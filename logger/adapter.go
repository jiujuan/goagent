package logger

// PrintfLogger 把 logger 适配成只有 Printf 的极简日志接口,用来对接
// middleware.Tracing(它接受 interface{ Printf(string, ...any) })。这样无需改动
// middleware 包,就能让 Tracing 的日志走 zerolog:
//
//	middleware.Tracing(logger.Printf(logger.L()))
//
// 适配器以 Info 级输出;Tracing 本身是纯观察者,这些是常规追踪信息。
type PrintfLogger struct {
	l *Logger
}

// Printf 返回一个 *PrintfLogger,把 fmt 风格日志桥接到 zerolog 的 Info 级。
func Printf(l *Logger) *PrintfLogger {
	if l == nil {
		l = L()
	}
	return &PrintfLogger{l: l}
}

// Printf 满足 middleware.Logger 接口。
func (p *PrintfLogger) Printf(format string, args ...any) {
	p.l.Info().Msgf(format, args...)
}
