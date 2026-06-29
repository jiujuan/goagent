// 本示例演示 config 包:基于 viper 的独立配置管理系统。
//
// 运行(优先级从低到高,后者覆盖前者):
//
//	go run ./examples/config                                  # 内置默认值
//	go run ./examples/config -file examples/config/config.yaml # + 配置文件
//	GOAGENT_LLM_MODEL=gpt-4o go run ./examples/config          # + GOAGENT_ 环境变量
//	AGNES_MODEL=deepseek-chat go run ./examples/config         # + 兼容旧环境变量
//
// 重点:配置从"到处 os.Getenv + 复制粘贴 envOr"变成"一次 Load、强类型字段访问",
// 且旧的 AGNES_* 环境变量零改动继续生效。
package main

import (
	"flag"
	"fmt"

	"github.com/jiujuan/goagent/config"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/openaicompat"
)

func main() {
	file := flag.String("file", "", "可选:指定 config.yaml 路径")
	flag.Parse()

	// ---- 1) 一行加载。无文件无 env 也能跑(用内置默认值)。----
	var opts []config.Option
	if *file != "" {
		opts = append(opts, config.WithFile(*file))
	}
	cfg := config.MustLoad(opts...)

	section("解析后的配置(强类型字段)")
	fmt.Printf("  llm.provider = %s\n", cfg.LLM.Provider)
	fmt.Printf("  llm.base_url = %s\n", cfg.LLM.BaseURL)
	fmt.Printf("  llm.model    = %s\n", cfg.LLM.Model)
	fmt.Printf("  llm.api_key  = %s\n", mask(cfg.LLM.APIKey))
	fmt.Printf("  redis.url    = %s\n", cfg.Redis.URL)
	fmt.Printf("  eval.live    = %v\n", cfg.Eval.Live)

	// ---- 2) "少量改造"对照:旧三行 envOr → 新一行从 cfg 取值构造模型。----
	//
	// 改造前(现有 example 里的写法):
	//   key   := os.Getenv("AGNES_API_KEY")
	//   base  := envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1")
	//   model := envOr("AGNES_MODEL", "gemini-2.5-flash")
	//
	// 改造后:默认值集中在 config,env 仍可覆盖,调用方只剩一行。
	var model llm.Model = openaicompat.Agnes(cfg.LLM.BaseURL, cfg.LLM.Model, cfg.LLM.APIKey)
	section("从配置构造模型")
	fmt.Printf("  model.Name() = %s\n", model.Name())
	if cfg.LLM.APIKey == "" {
		fmt.Println("  (未设置 api_key,仅演示构造;设 AGNES_API_KEY 或 GOAGENT_LLM_API_KEY 即可真实调用)")
	}

	// ---- 3) 原始 key 访问:struct 没覆盖的临时键也能读。----
	section("原始 key 访问(struct 未覆盖的键)")
	fmt.Printf("  GetString(\"llm.model\")     = %s\n", cfg.GetString("llm.model"))
	fmt.Printf("  GetBool(\"eval.live\")        = %v\n", cfg.GetBool("eval.live"))
	fmt.Printf("  GetInt(\"custom.timeout\")    = %d (未配置 → 零值)\n", cfg.GetInt("custom.timeout"))

	section("优先级(低 → 高)")
	fmt.Println("  内置默认值 < config.yaml < config.local.yaml < 环境变量 < WithXxx")
}

func section(title string) { fmt.Printf("\n========== %s ==========\n", title) }

// mask 隐藏 api_key 中段,避免演示输出泄露 secret。
func mask(s string) string {
	if s == "" {
		return "(空)"
	}
	if len(s) <= 6 {
		return "******"
	}
	return s[:3] + "****" + s[len(s)-2:]
}
