// Command task-runner 演示「用户给一个 task,程序执行到完成」的一次性任务运行器。
//
// goagent 的 Stream/Run 天然就是「执行到完成」语义:模型每轮发出 tool call,
// 循环执行并把结果回喂,直到模型返回一条不带 tool call 的收尾回复才结束。
// task 能否做完,关键在两点:给够干活趁手的工具(这里是 run_command +
// write_file/read_file),以及用系统提示约束「先拆解、以工具结果为准、做完验证」。
//
// 运行时事件被逐条打印(回合 / 工具调用 / 工具结果 / 模型文本),最终答案打在
// 标准输出,过程信息打在标准错误,方便把答案重定向到文件。
//
// 命令通过 sandbox/process 沙箱执行:限定工作目录、30 秒超时、输出上限。
// AllowedCommands 留空表示不限命令,生产环境建议改成白名单(如 go/git/ls)。
//
//	export AGNES_API_KEY=sk-...
//	go run ./examples/task-runner "统计工作目录里有多少个 .go 文件,把数字写进 result/count.txt,然后告诉我结果"
//
// 工作目录默认当前目录,可用 TASK_RUNNER_WORKDIR 覆盖;模型可用 AGNES_MODEL /
// AGNES_BASE_URL 切换。Windows 下命令入口请让模型使用 cmd /c 或 powershell 能
// 识别的可执行程序。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm/openaicompat"
	"github.com/jiujuan/goagent/sandbox"
	"github.com/jiujuan/goagent/sandbox/process"
	"github.com/jiujuan/goagent/tool"
	"github.com/jiujuan/goagent/tool/exec"
)

// taskInstruction 是任务执行器的系统提示:拆解 → 执行 → 验证 → 收尾。
const taskInstruction = `你是任务执行器,负责把用户交代的 task 真正做完。规则:
1. 先把任务拆成几个小步骤,再逐步调用工具执行;同一轮可以并行发起互不依赖的调用。
2. 一切结论以工具的真实返回为准,绝不臆造命令输出或文件内容。
3. 需要产出文件时用 write_file 写到工作目录内,必要时用 read_file 或 run_command 复核。
4. 确认任务完成后,停止调用工具,用中文给出收尾总结:做了哪些步骤、依据是什么、产出了哪些文件。`

func main() {
	task := strings.TrimSpace(strings.Join(os.Args[1:], " "))
	if task == "" {
		fmt.Println(`用法: go run ./examples/task-runner "<任务描述>"`)
		fmt.Println(`示例: go run ./examples/task-runner "统计工作目录里的 .go 文件数量,写进 result/count.txt 并报告"`)
		return
	}
	key := os.Getenv("AGNES_API_KEY")
	if key == "" {
		fmt.Println("请先设置 AGNES_API_KEY(和可选 AGNES_MODEL)。")
		return
	}
	work, err := filepath.Abs(envOr("TASK_RUNNER_WORKDIR", "."))
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(work, 0o755); err != nil {
		log.Fatal(err)
	}

	sb, err := process.New(sandbox.Policy{
		WorkDir:        work,
		Timeout:        30 * time.Second,
		MaxOutputBytes: 16 << 10,
	})
	if err != nil {
		log.Fatal(err)
	}

	model := openaicompat.Agnes(envOr("AGNES_BASE_URL", "https://apihub.agnes-ai.com/v1"),
		envOr("AGNES_MODEL", "gemini-2.5-flash"), key)

	a, err := agent.New(
		agent.WithName("task-runner"),
		agent.WithModel(model),
		agent.WithInstruction(taskInstruction),
		agent.WithTools(
			exec.RunCommand(sb),
			writeFileTool(work),
			readFileTool(work),
		),
		agent.WithMaxTurns(32), // 单次任务的安全上限,防止循环失控
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(os.Stderr, "工作目录: %s\n任务: %s\n", work, task)
	run := a.Stream(context.Background(), task)
	for ev, err := range run.Iter() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := ev.(type) {
		case core.TurnStarted:
			fmt.Fprintf(os.Stderr, "\n── 第 %d 轮 ──\n", e.Step)
		case core.MessageDone:
			if t := strings.TrimSpace(e.Message.Text()); t != "" {
				fmt.Fprintln(os.Stderr, t)
			}
		case core.ToolStarted:
			fmt.Fprintf(os.Stderr, "→ 调用 %s %s\n", e.Call.Name, clip(string(e.Call.Args), 200))
		case core.ToolDone:
			fmt.Fprintf(os.Stderr, "← %s 结果: %s\n", e.Result.Name, clip(resultText(e.Result), 300))
		}
	}

	res, err := run.Wait()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.Message.Text())
}

// writeFileTool 构造只允许写工作目录内路径的 write_file 工具。
func writeFileTool(work string) tool.Tool {
	return tool.New("write_file", "在工作目录内写入一个 UTF-8 文本文件(覆盖已有内容)。",
		func(_ *tool.Context, in struct {
			Path    string `json:"path" desc:"相对工作目录的文件路径,如 result/count.txt"`
			Content string `json:"content" desc:"要写入的完整文件内容"`
		}) (string, error) {
			abs, ok := inside(work, in.Path)
			if !ok {
				return "", fmt.Errorf("路径 %q 越出工作目录,只允许写其内的相对路径", in.Path)
			}
			if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
				return "", err
			}
			if err := os.WriteFile(abs, []byte(in.Content), 0o644); err != nil {
				return "", err
			}
			return fmt.Sprintf("已写入 %s(%d 字节)", in.Path, len(in.Content)), nil
		})
}

// readFileTool 构造只允许读工作目录内文件的 read_file 工具。
func readFileTool(work string) tool.Tool {
	return tool.New("read_file", "读取工作目录内一个文本文件的完整内容。",
		func(_ *tool.Context, in struct {
			Path string `json:"path" desc:"相对工作目录的文件路径"`
		}) (string, error) {
			abs, ok := inside(work, in.Path)
			if !ok {
				return "", fmt.Errorf("路径 %q 越出工作目录", in.Path)
			}
			b, err := os.ReadFile(abs)
			if err != nil {
				return "", err
			}
			return string(b), nil
		})
}

// inside 把相对路径解析到 work 之下的绝对路径;越界(如 ../)时返回 false。
func inside(work, p string) (string, bool) {
	abs, err := filepath.Abs(filepath.Join(work, filepath.Clean("/"+p)))
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(work, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return abs, true
}

// resultText 取出工具结果里的全部文本部分。
func resultText(r core.ToolResult) string {
	var b strings.Builder
	for _, part := range r.Content {
		if t, ok := part.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	if r.IsError {
		return "错误: " + b.String()
	}
	return b.String()
}

// clip 压掉换行并截到 n 个字符,供过程日志单行展示。
func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
