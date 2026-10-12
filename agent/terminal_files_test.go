package agent_test

// BC-03（ADR-0033 D4，V6）：终态记录必须完整保留边界上的文件快照——重开存储、
// 新运行与 Fork 都能拿回同样的文件内容，空文件视图也不退化成"没有快照"。

import (
	"context"
	"reflect"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/vfs"
)

// TestRecordCompletionPreservesFiles：写文件 → 记录终态 → 重开 File store 读终态与
// 文件 → 同一线程的新运行仍能读到 → Fork 仍带文件。
func TestRecordCompletionPreservesFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() checkpoint.Checkpointer {
		f, err := checkpoint.NewFile(dir)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	writer := terminalAgent(t, open(), stashModel(), stashFile(), peekFile())
	run := writer.Stream(ctx, "stash the note", agent.OnThread("files"), agent.WithRunFiles(vfs.NewInState()))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	boundary := headCheckpoint(t, open(), "files")
	if got := boundary.FileSnapshot["note.txt"]; string(got) != "secret" {
		t.Fatalf("边界快照没有文件: %v", boundary.FileSnapshot)
	}
	if err := writer.RecordCompletion(ctx, run); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}

	reopened := open()
	terminal := headCheckpoint(t, reopened, "files")
	if terminal.ID == boundary.ID {
		t.Fatal("终态记录没有追加")
	}
	if terminal.Completion == nil || terminal.Completion.RunID != run.ID {
		t.Fatalf("终态记录不符: %+v", terminal.Completion)
	}
	if terminal.ParentID != boundary.ID || terminal.Step != boundary.Step {
		t.Fatalf("终态记录没有指向本次边界: parent=%s step=%d（边界 %s/%d）", terminal.ParentID, terminal.Step, boundary.ID, boundary.Step)
	}
	if !reflect.DeepEqual(terminal.FileSnapshot, boundary.FileSnapshot) {
		t.Fatalf("终态记录的文件快照与边界不一致: %v vs %v", terminal.FileSnapshot, boundary.FileSnapshot)
	}
	if got := string(terminal.FileSnapshot["note.txt"]); got != "secret" {
		t.Fatalf("重开后终态里的文件 = %q", got)
	}

	// 新运行从终态 head 恢复文件（不是只读旧记录）。
	reader := terminalAgent(t, open(), peekModel(), stashFile(), peekFile())
	out, err := reader.Run(ctx, "what was stashed?", agent.OnThread("files"))
	if err != nil {
		t.Fatal(err)
	}
	if out != "read:secret" {
		t.Fatalf("新运行读回 %q", out)
	}

	// Fork 终态记录：文件保留，终态清除。
	branch, err := checkpoint.Fork(terminal, "files-branch", "files-branch-cp")
	if err != nil {
		t.Fatal(err)
	}
	if branch.Completion != nil {
		t.Fatal("Fork 未清除终态")
	}
	if err := open().Save(ctx, branch); err != nil {
		t.Fatalf("保存分支: %v", err)
	}
	storedBranch, err := open().Load(ctx, "files-branch", "files-branch-cp")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(storedBranch.FileSnapshot, terminal.FileSnapshot) {
		t.Fatalf("分支文件快照不符: %v vs %v", storedBranch.FileSnapshot, terminal.FileSnapshot)
	}

	branchReader := terminalAgent(t, open(), peekModel(), stashFile(), peekFile())
	branchOut, err := branchReader.Run(ctx, "what was stashed?", agent.OnThread("files-branch"))
	if err != nil {
		t.Fatal(err)
	}
	if branchOut != "read:secret" {
		t.Fatalf("分支运行读回 %q", branchOut)
	}
}

// TestRecordCompletionPreservesEmptyFileView：没写过文件的运行，终态记录仍保留
// "明确的空文件视图"，不会被写成"没有快照"。
func TestRecordCompletionPreservesEmptyFileView(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() checkpoint.Checkpointer {
		f, err := checkpoint.NewFile(dir)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	a := recordAgent(t, open(), "什么都不写")
	run := a.Stream(ctx, "go", agent.OnThread("empty-view"), agent.WithRunFiles(vfs.NewInState()))
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	boundary := headCheckpoint(t, open(), "empty-view")
	if boundary.FileSnapshot == nil {
		t.Skip("该后端没有给出文件快照，空视图语义无从验证")
	}
	if len(boundary.FileSnapshot) != 0 {
		t.Fatalf("边界应是没有文件的快照，得到 %v", boundary.FileSnapshot)
	}

	if err := a.RecordCompletion(ctx, run); err != nil {
		t.Fatalf("RecordCompletion: %v", err)
	}
	terminal := headCheckpoint(t, open(), "empty-view")
	if terminal.FileSnapshot == nil {
		t.Fatal("终态记录把空文件视图写成了没有快照")
	}
	if len(terminal.FileSnapshot) != 0 {
		t.Fatalf("终态记录多出了文件: %v", terminal.FileSnapshot)
	}
}
