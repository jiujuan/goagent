package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.LLM.Provider != "agnes" {
		t.Errorf("provider = %q, want agnes", c.LLM.Provider)
	}
	if c.LLM.BaseURL != "https://apihub.agnes-ai.com/v1" {
		t.Errorf("base_url = %q", c.LLM.BaseURL)
	}
	if c.LLM.Model != "gemini-2.5-flash" {
		t.Errorf("model = %q", c.LLM.Model)
	}
	if c.Eval.Live {
		t.Error("eval.live should default false")
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("GOAGENT_LLM_MODEL", "gpt-4o")
	t.Setenv("GOAGENT_LLM_API_KEY", "sk-new")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.LLM.Model != "gpt-4o" {
		t.Errorf("model = %q, want gpt-4o (GOAGENT_ env override)", c.LLM.Model)
	}
	if c.LLM.APIKey != "sk-new" {
		t.Errorf("api_key = %q, want sk-new", c.LLM.APIKey)
	}
}

func TestLegacyEnvCompat(t *testing.T) {
	// 旧的 AGNES_* / EVAL_LIVE 在零改动下仍生效。
	t.Setenv("AGNES_API_KEY", "sk-legacy")
	t.Setenv("AGNES_MODEL", "deepseek-chat")
	t.Setenv("EVAL_LIVE", "1")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.LLM.APIKey != "sk-legacy" {
		t.Errorf("api_key = %q, want sk-legacy (AGNES_API_KEY compat)", c.LLM.APIKey)
	}
	if c.LLM.Model != "deepseek-chat" {
		t.Errorf("model = %q, want deepseek-chat (AGNES_MODEL compat)", c.LLM.Model)
	}
	if !c.Eval.Live {
		t.Error("eval.live should be true (EVAL_LIVE=1 compat)")
	}
}

func TestPrefixedWinsOverLegacy(t *testing.T) {
	// 同时设新旧键时,GOAGENT_ 前缀键优先(BindEnv 顺序在前)。
	t.Setenv("GOAGENT_LLM_MODEL", "new-model")
	t.Setenv("AGNES_MODEL", "old-model")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.LLM.Model != "new-model" {
		t.Errorf("model = %q, want new-model (prefixed beats legacy)", c.LLM.Model)
	}
}

func TestWithFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	yaml := "" +
		"llm:\n" +
		"  provider: deepseek\n" +
		"  base_url: https://api.deepseek.com/v1\n" +
		"  model: deepseek-chat\n" +
		"redis:\n" +
		"  url: redis://example:6380/1\n"
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(WithFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if c.LLM.Provider != "deepseek" || c.LLM.Model != "deepseek-chat" {
		t.Errorf("file not applied: %+v", c.LLM)
	}
	if c.Redis.URL != "redis://example:6380/1" {
		t.Errorf("redis.url = %q", c.Redis.URL)
	}
}

func TestEnvBeatsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("llm:\n  model: from-file\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOAGENT_LLM_MODEL", "from-env")
	c, err := Load(WithFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if c.LLM.Model != "from-env" {
		t.Errorf("model = %q, want from-env (env should beat file)", c.LLM.Model)
	}
}

func TestRawAccessors(t *testing.T) {
	t.Setenv("GOAGENT_CUSTOM_TIMEOUT", "30")
	c, err := Load(WithDefault("custom.retries", 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.GetInt("custom.timeout"); got != 30 {
		t.Errorf("custom.timeout = %d, want 30", got)
	}
	if got := c.GetInt("custom.retries"); got != 3 {
		t.Errorf("custom.retries = %d, want 3 (WithDefault)", got)
	}
}

func TestQueueDefaults(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Queue.Stream != "goagent:jobs" {
		t.Errorf("queue.stream = %q", c.Queue.Stream)
	}
	if c.Queue.Group != "workers" {
		t.Errorf("queue.group = %q", c.Queue.Group)
	}
	if c.Queue.IdleThreshold != 5*time.Minute {
		t.Errorf("queue.idle_threshold = %v, want 5m", c.Queue.IdleThreshold)
	}
	if c.Queue.MaxDeliveries != 3 {
		t.Errorf("queue.max_deliveries = %d", c.Queue.MaxDeliveries)
	}
	if c.Queue.MaxLen != 100_000 {
		t.Errorf("queue.max_len = %d", c.Queue.MaxLen)
	}
}

func TestQueueEnvOverride(t *testing.T) {
	t.Setenv("GOAGENT_QUEUE_STREAM", "myapp:jobs")
	t.Setenv("GOAGENT_QUEUE_IDLE_THRESHOLD", "90s") // 字符串 → time.Duration
	t.Setenv("GOAGENT_QUEUE_MAX_DELIVERIES", "5")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Queue.Stream != "myapp:jobs" {
		t.Errorf("queue.stream = %q, want myapp:jobs", c.Queue.Stream)
	}
	if c.Queue.IdleThreshold != 90*time.Second {
		t.Errorf("queue.idle_threshold = %v, want 90s", c.Queue.IdleThreshold)
	}
	if c.Queue.MaxDeliveries != 5 {
		t.Errorf("queue.max_deliveries = %d, want 5", c.Queue.MaxDeliveries)
	}
}

func TestMissingFileIsOK(t *testing.T) {
	// 没有任何 config.yaml 时,Load 不应报错,返回默认值。
	c, err := Load(WithPath(t.TempDir()))
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if c.LLM.Model == "" {
		t.Error("expected default model when no file present")
	}
}
