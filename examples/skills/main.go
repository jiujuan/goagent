// Command skills is a tutorial for filesystem skill packs with progressive
// (three-level) loading:
//
//	Level 1  metadata     —— skill.PromptSection 把可用技能列进 system prompt(总在)
//	Level 2  SKILL.md body —— 模型按需调 use_skill(name) 读正文
//	Level 3  resources/脚本 —— use_skill(name, resource) 读文件;run_skill_script 经 sandbox 执行
//
// 技能以目录形式存在(含一个 SKILL.md);可从磁盘(LoadDir)或编进二进制(Load + embed.FS)
// 加载。本例用 embed.FS + mock 模型,离线、确定性。
//
//	go run ./examples/skills
package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/prompt"
	"github.com/jiujuan/goagent/skills"
)

//go:embed skills
var skillsFS embed.FS

func main() {
	// 把技能编进二进制:fs.Sub 把根定位到 skills/,使 Load 能扫到 greet/SKILL.md。
	sub, err := fs.Sub(skillsFS, "skills")
	if err != nil {
		log.Fatal(err)
	}
	lib, err := skills.Load(sub)
	if err != nil {
		log.Fatal(err)
	}

	// mock:第一轮按 prompt 里列出的技能调 use_skill 读正文,第二轮据正文作答。
	model := mock.New("mock", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			_ = tr // 这里能看到 SKILL.md 正文
			return mock.Text("(已加载 greet 技能)你好,小明!见到你很开心。")
		}
		return mock.CallTool("c1", "use_skill", `{"name":"greet"}`)
	})

	a, err := agent.New(
		agent.WithModel(model),
		// Level 1:skill.PromptSection 把可用技能列进 system prompt。
		agent.WithPrompt(prompt.New().
			Add(prompt.Identity("你是一个助手。需要某种能力时,先用 use_skill 加载对应技能再行动。")).
			Add(skills.PromptSection(lib))),
		// Level 2/3 的入口工具(本例不跑脚本,故只挂 use_skill;脚本用 skills.ScriptTool(lib, sandbox))。
		agent.WithTools(skills.Tool(lib)),
	)
	if err != nil {
		log.Fatal(err)
	}

	for ev, err := range a.Stream(context.Background(), "请问候一下小明").Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.ToolDone:
			if e.Result.Name == "use_skill" {
				fmt.Println("📖 use_skill 返回(SKILL.md 正文节选):")
				fmt.Println(indent(firstN(e.Result.Content[0].(core.Text).Text, 120)))
			}
		case core.MessageDone:
			if t := e.Message.Text(); t != "" {
				fmt.Println("🤖", t)
			}
		}
	}
}

func firstN(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + " …"
}

func indent(s string) string {
	out := ""
	for _, line := range splitLines(s) {
		out += "   " + line + "\n"
	}
	return out
}

func splitLines(s string) []string {
	var lines []string
	cur := ""
	for _, r := range s {
		if r == '\n' {
			lines = append(lines, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	return lines
}
