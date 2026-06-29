package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// parse 解析单行 JSON 日志为 map,方便断言字段。
func parse(t *testing.T, line []byte) map[string]any {
	t.Helper()
	m := map[string]any{}
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("日志不是合法 JSON: %v\n原文: %s", err, line)
	}
	return m
}

func TestJSONFieldsAndMessage(t *testing.T) {
	var buf bytes.Buffer
	l := New(WithFormat("json"), WithLevel("debug"), WithOutput(&buf))

	l.Info().Str("run_id", "r1").Int("turn", 3).Bool("ok", true).Msg("model replied")

	m := parse(t, buf.Bytes())
	if m["level"] != "info" {
		t.Errorf("level = %v, 期望 info", m["level"])
	}
	if m["message"] != "model replied" {
		t.Errorf("message = %v", m["message"])
	}
	if m["run_id"] != "r1" {
		t.Errorf("run_id = %v", m["run_id"])
	}
	if m["turn"].(float64) != 3 {
		t.Errorf("turn = %v, 期望 3", m["turn"])
	}
	if m["ok"] != true {
		t.Errorf("ok = %v", m["ok"])
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	l := New(WithFormat("json"), WithLevel("warn"), WithOutput(&buf))

	l.Info().Msg("应被过滤")
	l.Debug().Msg("也被过滤")
	if buf.Len() != 0 {
		t.Fatalf("warn 级别下 info/debug 不应输出,却得到: %s", buf.String())
	}

	l.Error().Msg("应输出")
	if !strings.Contains(buf.String(), "应输出") {
		t.Errorf("error 级别应输出,缓冲为空")
	}
}

func TestWithFieldsDerivation(t *testing.T) {
	var buf bytes.Buffer
	base := New(WithFormat("json"), WithOutput(&buf))
	child := base.WithStr("run_id", "abc").WithFields(map[string]any{"agent": "main"})

	child.Info().Msg("hi")
	m := parse(t, buf.Bytes())
	if m["run_id"] != "abc" || m["agent"] != "main" {
		t.Errorf("派生字段缺失: %v", m)
	}
}

func TestContextRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	l := New(WithFormat("json"), WithOutput(&buf)).WithStr("run_id", "ctx1")

	ctx := Into(context.Background(), l)
	From(ctx).Info().Msg("from ctx")

	m := parse(t, buf.Bytes())
	if m["run_id"] != "ctx1" || m["message"] != "from ctx" {
		t.Errorf("context 往返丢字段: %v", m)
	}
}

func TestFromEmptyContextIsSilent(t *testing.T) {
	// 没存过 Logger 的 ctx,From 应返回禁用态,不 panic 也不输出。
	From(context.Background()).Info().Msg("should be discarded")
}

func TestPrintfAdapter(t *testing.T) {
	var buf bytes.Buffer
	l := New(WithFormat("json"), WithOutput(&buf))
	p := Printf(l)

	p.Printf("turn %d: %s", 2, "done")
	m := parse(t, buf.Bytes())
	if m["message"] != "turn 2: done" {
		t.Errorf("适配器 message = %v", m["message"])
	}
	if m["level"] != "info" {
		t.Errorf("适配器应以 info 级输出, 得到 %v", m["level"])
	}
}

func TestSetDefaultAndGlobal(t *testing.T) {
	var buf bytes.Buffer
	prev := def // 保存并在结束后恢复全局,避免污染其它测试
	t.Cleanup(func() { SetDefault(prev) })

	SetDefault(New(WithFormat("json"), WithOutput(&buf)))
	Info().Str("k", "v").Msg("global")

	m := parse(t, buf.Bytes())
	if m["message"] != "global" || m["k"] != "v" {
		t.Errorf("全局透传失败: %v", m)
	}
}
