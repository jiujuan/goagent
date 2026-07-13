// 本示例演示 logger 包:基于 rs/zerolog 的独立日志系统。
//
// 运行:
//
//	go run ./examples/logger                                  # console 格式(默认)
//	GOAGENT_LOG_FORMAT=json go run ./examples/logger          # JSON 格式
//	GOAGENT_LOG_LEVEL=debug go run ./examples/logger          # 放开 debug
//
// 重点:logger 是近叶子包,只依赖 zerolog;配置值由顶层 config 包产出,在 main()
// 里映射成 logger 的函数式 Option(loggerOpts),两层互不 import——与 queue 的
// queueOpts 同款"装配层"范式。
package main

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/jiujuan/goagent/config"
	"github.com/jiujuan/goagent/logger"
)

func main() {
	cfg := config.MustLoad()

	// ---- 1) 用 config 值构造 logger,并设为全局(main 里一次)。----
	logger.SetDefault(logger.New(loggerOpts(cfg)...))

	// ---- 2) 结构化日志:链式追加强类型字段,Msg 收尾。----
	logger.Info().
		Str("provider", cfg.LLM.Provider).
		Str("model", cfg.LLM.Model).
		Msg("logger 已初始化")

	logger.Debug().Int("retries", 3).Msg("这条只有 level=debug 时才出现")

	logger.Warn().
		Dur("latency", 1200*time.Millisecond).
		Bool("slow", true).
		Msg("一次较慢的调用")

	logger.Error().
		Err(errors.New("connection refused")).
		Str("endpoint", cfg.LLM.BaseURL).
		Msg("模型调用失败")

	// ---- 3) 派生子 logger:固定字段随每条日志带出(如一次运行的 run_id)。----
	run := logger.L().WithStr("run_id", "run-42")
	run.Info().Int("turn", 1).Msg("开始第一步")

	// ---- 4) context 流转:深层函数无需显式接收 *Logger。----
	ctx := logger.Into(context.Background(), run)
	doWork(ctx)

	// ---- 5) 接入 middleware.Tracing:logger.Printf 适配器满足其 Logger 接口。----
	//   mw := middleware.Tracing(logger.Printf(logger.L()))
	//   agent.New(model, agent.WithMiddleware(mw))
	logger.Info().Msg("Tracing 可用 middleware.Tracing(logger.Printf(logger.L())) 接入")
}

// doWork 演示从 context 取回 logger,自动带上上游注入的 run_id。
func doWork(ctx context.Context) {
	logger.From(ctx).Info().Str("stage", "doWork").Msg("从 context 取回 logger")
}

// loggerOpts 把 config 的 Log 段映射成 logger 的函数式 Option(装配层范本)。
// config 不 import logger,这层映射由调用方持有,保持单向依赖。
func loggerOpts(cfg *config.Config) []logger.Option {
	return []logger.Option{
		logger.WithLevel(cfg.Log.Level),
		logger.WithFormat(cfg.Log.Format),
		logger.WithOutput(resolveOutput(cfg.Log.Output)),
		logger.WithTimeFormat(cfg.Log.TimeFormat),
		logger.WithCaller(cfg.Log.Caller),
	}
}

// resolveOutput 把配置里的字符串 output 解析成 io.Writer。文件路径打开失败时回退 stderr。
func resolveOutput(out string) *os.File {
	switch out {
	case "", "stderr":
		return os.Stderr
	case "stdout":
		return os.Stdout
	default:
		f, err := os.OpenFile(out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return os.Stderr
		}
		return f
	}
}
