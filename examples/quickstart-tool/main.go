// Command quickstart-tool is the smallest real-model starter WITH a tool: the
// model decides to call get_time, and the loop feeds the result back for the
// final answer. Copy and add your own tools.
//
//	export AGNES_API_KEY=sk-...
//	go run ./examples/quickstart-tool
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/llm/openaicompat"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		fmt.Println("请先设置 AGNES_API_KEY(和可选 AGNES_MODEL)。")
		return
	}
	model := openaicompat.Agnes(envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1"),
		envOr("AGNES_MODEL", "gemini-2.5-flash"), key)

	// A typed tool: parameter struct → JSON schema by reflection.
	getTime := tool.New("get_time", "获取某时区当前时间",
		func(_ *tool.Context, in struct {
			Zone string `json:"zone" desc:"IANA 时区,如 Asia/Shanghai"`
		}) (string, error) {
			loc, err := time.LoadLocation(in.Zone)
			if err != nil {
				return "", err
			}
			return time.Now().In(loc).Format("2006-01-02 15:04:05"), nil
		})

	a, err := agent.New(
		agent.WithModel(model),
		agent.WithInstruction("需要当前时间时调用 get_time 工具,再用中文作答。"),
		agent.WithTools(getTime),
	)
	if err != nil {
		log.Fatal(err)
	}

	answer, err := a.Run(context.Background(), "上海现在几点?")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(answer)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
