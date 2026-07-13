package logger

import (
	"time"

	"github.com/rs/zerolog"
)

// Event 是对 zerolog.Event 的薄封装:链式追加字段,最后 Msg/Msgf/Send 收尾发送。
// 只转发最常用的字段方法;更冷门的能力用 Z() 拿原生 *zerolog.Event。
// 一个 Event 只能发送一次。禁用态(级别被过滤)下所有方法都是 no-op。
type Event struct {
	e *zerolog.Event
}

func (e *Event) Str(key, val string) *Event             { e.e.Str(key, val); return e }
func (e *Event) Strs(key string, vals []string) *Event  { e.e.Strs(key, vals); return e }
func (e *Event) Int(key string, val int) *Event         { e.e.Int(key, val); return e }
func (e *Event) Int64(key string, val int64) *Event     { e.e.Int64(key, val); return e }
func (e *Event) Float64(key string, val float64) *Event { e.e.Float64(key, val); return e }
func (e *Event) Bool(key string, val bool) *Event       { e.e.Bool(key, val); return e }
func (e *Event) Dur(key string, d time.Duration) *Event { e.e.Dur(key, d); return e }
func (e *Event) Time(key string, t time.Time) *Event    { e.e.Time(key, t); return e }

// Err 追加 error 字段(键名 "error");nil 时 zerolog 自动跳过。
func (e *Event) Err(err error) *Event { e.e.Err(err); return e }

// Any 追加任意类型字段(反射序列化,比定型方法略慢)。
func (e *Event) Any(key string, val any) *Event { e.e.Interface(key, val); return e }

// Fields 批量追加 map 中的字段。
func (e *Event) Fields(fields map[string]any) *Event { e.e.Fields(fields); return e }

// Msg 写入消息并发送这条日志。之后不可再用该 Event。
func (e *Event) Msg(msg string) { e.e.Msg(msg) }

// Msgf 同 Msg,但消息支持 fmt 格式化。
func (e *Event) Msgf(format string, args ...any) { e.e.Msgf(format, args...) }

// Send 不带消息直接发送(字段已通过链式追加)。
func (e *Event) Send() { e.e.Send() }

// Z 是逃生口:拿底层 *zerolog.Event 用 facade 未转发的字段方法。
func (e *Event) Z() *zerolog.Event { return e.e }
