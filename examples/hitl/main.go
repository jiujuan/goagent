// Command hitl demonstrates the Human-in-the-Loop middleware: every call to a
// dangerous tool is shown to a human "approver" before the turn engine executes
// it. The approver can APPROVE it, APPROVE with edited arguments, or DENY it
// (feeding the reason back to the model so it can change course).
//
// Three scenarios run back to back, each on the mock provider (no API key):
//
//	① 批准   → 工具照常执行
//	② 拒绝   → 工具不执行，模型收到拒绝原因后改走安全方案
//	③ 改参   → 人工把危险路径改写到沙箱后再放行
//
// Only the `delete_file` tool is gated (via RequireApprovalFor); the safe tool
// runs without prompting. In a real CLI you would swap the scripted approver for
// middleware.ConsoleApprover(os.Stdin, os.Stdout).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/middleware"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/tool"
)

func main() {
	banner("goagent Human-in-the-Loop 示例",
		"工具调用前人工批准 / 拒绝 / 改参 —— 只 gate delete_file")

	// ① 批准:approver 放行,delete_file 真正执行。
	run("① 批准", approve(), `{"path":"/tmp/cache.tmp"}`)

	// ② 拒绝:approver 拒绝,工具不执行,模型据反馈改道。
	run("② 拒绝", deny("生产数据不可删除"), `{"path":"important.db"}`)

	// ③ 改参:approver 把目标改写到沙箱再放行。
	run("③ 改参", editTo(`{"path":"/sandbox/important.db"}`), `{"path":"important.db"}`)
}

// run executes one scenario: the model asks to delete firstArgs, the approver
// applies the given decision, and we print what actually happened.
func run(title string, decide middleware.Approver, firstArgs string) {
	fmt.Printf("\n──────── %s ────────\n", title)

	var deleted string // path the tool actually removed, if any
	deleteFile := tool.New("delete_file", "永久删除一个文件",
		func(_ *tool.Context, in struct {
			Path string `json:"path"`
		}) (string, error) {
			deleted = in.Path
			fmt.Printf("   🗑️  执行删除: %s\n", in.Path)
			return "deleted " + in.Path, nil
		})

	model := mock.New("m", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			if tr.IsError {
				return mock.Text("收到拒绝(" + tr.Content[0].(core.Text).Text + "),改为仅标记待人工处理。")
			}
			return mock.Text("已按批准完成操作。")
		}
		return mock.CallTool("c1", "delete_file", firstArgs)
	})

	ag := agent.New(agent.Config{
		Name:  "ops",
		Model: model,
		Tools: []tool.Tool{deleteFile},
		Middleware: []middleware.Middleware{
			middleware.HumanInTheLoop(middleware.HITLOptions{
				Gate:     middleware.RequireApprovalFor("delete_file"),
				Approver: logging(decide),
			}),
		},
	})
	r := runner.New(runner.Config{Root: ag})

	var final string
	for ev, err := range r.Run(context.Background(), "u", "s", core.UserText("清理无用文件")) {
		if err != nil {
			log.Fatal(err)
		}
		if ev.IsFinalResponse() {
			final = ev.Message.Text()
		}
	}

	fmt.Printf("   💬 模型最终答复: %s\n", final)
	if deleted == "" {
		fmt.Println("   ✅ 结果: 未删除任何文件")
	} else {
		fmt.Printf("   ✅ 结果: 实际删除了 %s\n", deleted)
	}
}

// --- 审批器（演示用,真实场景换成 ConsoleApprover 或接到工单/IM 系统）---------

func approve() middleware.Approver {
	return func(_ context.Context, _ core.ToolCall) (middleware.Decision, error) {
		return middleware.Approve(), nil
	}
}

func deny(reason string) middleware.Approver {
	return func(_ context.Context, _ core.ToolCall) (middleware.Decision, error) {
		return middleware.Deny(reason), nil
	}
}

func editTo(args string) middleware.Approver {
	return func(_ context.Context, _ core.ToolCall) (middleware.Decision, error) {
		return middleware.ApproveWithArgs(json.RawMessage(args)), nil
	}
}

// logging wraps an approver to print the request and verdict, mimicking what a
// human would see at the prompt.
func logging(next middleware.Approver) middleware.Approver {
	return func(ctx context.Context, call core.ToolCall) (middleware.Decision, error) {
		d, err := next(ctx, call)
		fmt.Printf("   ⏸️  待审批 %s%s → %s\n", call.Name, string(call.Args), verdict(d))
		return d, err
	}
}

func verdict(d middleware.Decision) string {
	switch {
	case d.Approve && d.EditedArgs != nil:
		return "改参批准 " + string(d.EditedArgs)
	case d.Approve:
		return "批准"
	default:
		return "拒绝(" + d.Reason + ")"
	}
}

func banner(title, sub string) {
	line := strings.Repeat("═", 56)
	fmt.Println(line)
	fmt.Println("  " + title)
	fmt.Println("  " + sub)
	fmt.Println(line)
}
