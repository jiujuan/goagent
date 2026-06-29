package logger

import (
	"context"

	"github.com/rs/zerolog"
)

// Into 把 Logger 存入 context,沿调用链向下传递。配合 From 在深层取回,避免到处显式
// 传 *Logger。zerolog 原生支持,这里只做 facade 包装。
//
//	ctx = logger.Into(ctx, logger.L().WithStr("run_id", id))
//	logger.From(ctx).Info().Msg("...")   // 自动带上 run_id
func Into(ctx context.Context, l *Logger) context.Context {
	return l.z.WithContext(ctx)
}

// From 取回 Into 存入的 Logger。若 ctx 中没有,返回禁用态 Logger(所有输出被丢弃),
// 调用方无需判空。
func From(ctx context.Context) *Logger {
	return &Logger{z: *zerolog.Ctx(ctx)}
}
