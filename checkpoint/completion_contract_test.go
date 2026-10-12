package checkpoint_test

// BC-02（ADR-0033 D4）：Completion 的数据边界。本文件只覆盖"记录能存能读、
// 深拷贝互不影响、Fork 清除终态"，不覆盖谁写入终态——那是 BC-03 的 RecordCompletion。

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// backend 提供一对构造器：store 是本用例使用的实例，reopen 对 File 会重新打开
// 目录（证明真的落盘），对 Memory 返回同一实例。
type backend struct {
	store  checkpoint.Checkpointer
	reopen func() checkpoint.Checkpointer
}

func completionBackends(t *testing.T) map[string]func() backend {
	t.Helper()
	dir := t.TempDir()
	openFile := func() checkpoint.Checkpointer {
		f, err := checkpoint.NewFile(dir)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	return map[string]func() backend{
		"memory": func() backend {
			m := checkpoint.NewMemory()
			return backend{store: m, reopen: func() checkpoint.Checkpointer { return m }}
		},
		"file": func() backend {
			f := openFile()
			return backend{store: f, reopen: openFile}
		},
	}
}

func completionDir(t *testing.T) string { return t.TempDir() }

// resultFixture 造一个带全部可变字节的结果：工具调用参数与嵌套图片数据。
func resultFixture(text string, args, image []byte) *core.Result {
	return &core.Result{Message: core.Message{
		Role: core.RoleAssistant,
		Parts: []core.Part{
			core.Text{Text: text},
			core.ToolCall{ID: "call1", Name: "lookup", Args: args},
			core.ToolResult{CallID: "call2", Name: "note", Content: []core.Part{
				core.Image{MIME: "image/png", Data: image},
			}},
		},
	}}
}

// completionFixture 用调用方保留引用的切片，便于 Save 之后就地改写它们。
func completionFixture(runID string, args, image []byte) *checkpoint.Completion {
	if image == nil {
		image = []byte("imagex")
	}
	return &checkpoint.Completion{
		RunID:  runID,
		Kind:   checkpoint.CompletionCompleted,
		Result: resultFixture("最终回答", args, image),
	}
}

func terminalCheckpoint(threadID, id string, cp *checkpoint.Completion, snapshot map[string][]byte) *checkpoint.Checkpoint {
	return &checkpoint.Checkpoint{
		ID:           id,
		ThreadID:     threadID,
		Step:         3,
		State:        core.State{Messages: []core.Message{core.UserText("原始提问")}},
		Completion:   cp,
		FileSnapshot: snapshot,
	}
}

// mutablePayloads 取出终态里的两处可变字节：工具调用参数与嵌套图片数据。
func mutablePayloads(t *testing.T, c *checkpoint.Completion) (args []byte, image []byte) {
	t.Helper()
	if c == nil || c.Result == nil {
		t.Fatal("终态记录缺少结果")
	}
	for _, part := range c.Result.Message.Parts {
		switch p := part.(type) {
		case core.ToolCall:
			args = p.Args
		case core.ToolResult:
			for _, inner := range p.Content {
				if img, ok := inner.(core.Image); ok {
					image = img.Data
				}
			}
		}
	}
	if args == nil || image == nil {
		t.Fatalf("终态结果缺少可变字节数据：args=%v image=%v", args, image)
	}
	return args, image
}

// --- 往返 -------------------------------------------------------------------

// TestCompletionRoundTripAcrossStores：completed 与 failed 两种终态在 Memory/File
// 都能写入并按 Load/Latest/History 读回；File 走 reopen 验证落盘。
func TestCompletionRoundTripAcrossStores(t *testing.T) {
	ctx := context.Background()
	for name, newBackend := range completionBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackend()
			done := terminalCheckpoint("t1", "cp-done", completionFixture("run-1", []byte(`{"key":"before"}`), nil), nil)
			if err := b.store.Save(ctx, done); err != nil {
				t.Fatalf("Save: %v", err)
			}
			failed := terminalCheckpoint("t1", "cp-failed", &checkpoint.Completion{
				RunID: "run-2",
				Kind:  checkpoint.CompletionFailed,
				Err:   "工具结果未知",
			}, nil)
			if err := b.store.Save(ctx, failed); err != nil {
				t.Fatalf("Save: %v", err)
			}

			latest, err := b.reopen().Latest(ctx, "t1")
			if err != nil {
				t.Fatal(err)
			}
			if latest.ID != "cp-failed" {
				t.Fatalf("Latest = %s，期望 cp-failed", latest.ID)
			}
			if latest.Completion == nil || latest.Completion.Kind != checkpoint.CompletionFailed ||
				latest.Completion.RunID != "run-2" || latest.Completion.Err != "工具结果未知" ||
				latest.Completion.Result != nil {
				t.Fatalf("failed 终态读回不符: %+v", latest.Completion)
			}

			loaded, err := b.reopen().Load(ctx, "t1", "cp-done")
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Completion == nil || loaded.Completion.Kind != checkpoint.CompletionCompleted ||
				loaded.Completion.RunID != "run-1" || loaded.Completion.Err != "" {
				t.Fatalf("completed 终态读回不符: %+v", loaded.Completion)
			}
			if loaded.Completion.Result == nil || loaded.Completion.Result.Message.Text() != "最终回答" {
				t.Fatalf("completed 结果丢失: %+v", loaded.Completion.Result)
			}

			hist, err := b.reopen().History(ctx, "t1")
			if err != nil {
				t.Fatal(err)
			}
			if len(hist) != 2 || hist[0].Completion == nil || hist[1].Completion == nil {
				t.Fatalf("历史条数或终态字段不符: %d", len(hist))
			}

			// 没有终态的普通快照读出 nil。
			plain := terminalCheckpoint("t1", "cp-plain", nil, nil)
			if err := b.reopen().Save(ctx, plain); err != nil {
				t.Fatalf("Save plain: %v", err)
			}
			readBack, err := b.reopen().Load(ctx, "t1", "cp-plain")
			if err != nil {
				t.Fatal(err)
			}
			if readBack.Completion != nil {
				t.Fatalf("无终态记录读出了 completion: %+v", readBack.Completion)
			}
		})
	}
}

// TestCompletionJSONShape：空终态不写入字段（旧读取方看到的 JSON 不变），
// 有终态时四个字段按 ADR D4 的键名落盘。
func TestCompletionJSONShape(t *testing.T) {
	dir := completionDir(t)
	f, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := f.Save(ctx, terminalCheckpoint("t1", "cp-plain", nil, nil)); err != nil {
		t.Fatal(err)
	}
	if err := f.Save(ctx, terminalCheckpoint("t1", "cp-1", completionFixture("run-1", []byte(`{"key":"before"}`), nil), nil)); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "t1.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var plainLines, terminalLines int
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		// File 后端把快照写在带版本的外层记录里的 "checkpoint" 对象内。
		var wrapper struct {
			Checkpoint map[string]any `json:"checkpoint"`
		}
		if err := json.Unmarshal([]byte(line), &wrapper); err != nil || wrapper.Checkpoint == nil {
			continue // blob 行与其它附加行不参与本断言
		}
		id, _ := wrapper.Checkpoint["id"].(string)
		switch id {
		case "cp-plain":
			plainLines++
			if _, ok := wrapper.Checkpoint["completion"]; ok {
				t.Fatalf("空终态记录仍写出了 completion 字段: %s", line)
			}
		case "cp-1":
			terminalLines++
			raw, ok := wrapper.Checkpoint["completion"]
			if !ok {
				t.Fatalf("终态记录缺少 completion 字段: %s", line)
			}
			fields, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("completion 不是对象: %T", raw)
			}
			for _, key := range []string{"run_id", "kind", "result"} {
				if _, ok := fields[key]; !ok {
					t.Fatalf("completion 缺少 %q 键: %v", key, fields)
				}
			}
			if _, ok := fields["err"]; ok {
				t.Fatalf("空 err 不应序列化: %v", fields)
			}
			if fields["kind"] != checkpoint.CompletionCompleted || fields["run_id"] != "run-1" {
				t.Fatalf("completion 键值不符: %v", fields)
			}
		}
	}
	if plainLines != 1 || terminalLines != 1 {
		t.Fatalf("检查到的记录行数不符：plain=%d terminal=%d", plainLines, terminalLines)
	}
}

// --- 冻结与深拷贝 -----------------------------------------------------------

// TestCompletionFrozenAcrossStores：Save 之后调用方就地改写自己那份记录（含结果
// 内部的可变字节），存储里的历史不变；改写读取返回值也污染不了存储。
func TestCompletionFrozenAcrossStores(t *testing.T) {
	ctx := context.Background()
	for name, newBackend := range completionBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackend()
			savedArgs := []byte(`{"key":"before"}`)
			savedImage := []byte("imagex")
			caller := completionFixture("run-1", savedArgs, savedImage)
			if err := b.store.Save(ctx, terminalCheckpoint("t1", "cp1", caller, nil)); err != nil {
				t.Fatal(err)
			}

			// 调用方改自己的记录：标量、结果指针与保留切片的底层数组。
			caller.Kind = checkpoint.CompletionFailed
			caller.RunID = "run-被改写"
			caller.Err = "调用方后来改的"
			copy(savedArgs, []byte(`{"key":"after!"}`))
			copy(savedImage, []byte("XXXXXX"))
			caller.Result = nil

			stored, err := b.reopen().Load(ctx, "t1", "cp1")
			if err != nil {
				t.Fatal(err)
			}
			got := stored.Completion
			if got == nil {
				t.Fatal("存储里没有终态记录")
			}
			if got.Kind != checkpoint.CompletionCompleted || got.RunID != "run-1" || got.Err != "" {
				t.Fatalf("调用方改写污染了存储: %+v", got)
			}
			if got.Result == nil || got.Result.Message.Text() != "最终回答" {
				t.Fatalf("结果被污染或丢失: %+v", got.Result)
			}
			args, image := mutablePayloads(t, got)
			if string(args) != `{"key":"before"}` {
				t.Fatalf("工具参数未深拷贝: %s", args)
			}
			if string(image) != "imagex" {
				t.Fatalf("图片数据未深拷贝: %s", image)
			}

			// 改写读取返回对象，不能污染存储与后续读取。
			got.Kind = checkpoint.CompletionFailed
			mutableArgs, _ := mutablePayloads(t, stored.Completion)
			copy(mutableArgs, []byte(`{"key":"ZZZZZZ"}`))
			again, err := b.reopen().Load(ctx, "t1", "cp1")
			if err != nil {
				t.Fatal(err)
			}
			if again.Completion.Kind != checkpoint.CompletionCompleted {
				t.Fatalf("读取返回对象可污染存储: %+v", again.Completion)
			}
			secondArgs, _ := mutablePayloads(t, again.Completion)
			if string(secondArgs) != `{"key":"before"}` {
				t.Fatalf("两次读取共享可变字节: %s", secondArgs)
			}

			// History 与 Latest 走同一份冻结数据。
			hist, err := b.reopen().History(ctx, "t1")
			if err != nil {
				t.Fatal(err)
			}
			if len(hist) != 1 || hist[0].Completion.Kind != checkpoint.CompletionCompleted {
				t.Fatalf("History 终态不符: %+v", hist)
			}
			latest, err := b.reopen().Latest(ctx, "t1")
			if err != nil {
				t.Fatal(err)
			}
			if latest.Completion.Kind != checkpoint.CompletionCompleted {
				t.Fatalf("Latest 终态不符: %+v", latest.Completion)
			}
		})
	}
}

// --- 旧记录兼容 -------------------------------------------------------------

// TestCompletionLegacyReadable：不含 completion 字段的既有 JSONL 记录仍可读，
// Completion 为 nil；随后写入带终态的记录，两类记录同时可读。
func TestCompletionLegacyReadable(t *testing.T) {
	dir := completionDir(t)
	// 旧记录就是当时 Checkpoint 的 JSON 一行（不带 completion 键），所以用真实
	// 编码器造出来，再确认它确实没有该键。
	legacyCP := &checkpoint.Checkpoint{
		ID:       "old",
		ThreadID: "t3",
		Step:     1,
		State:    core.State{Messages: []core.Message{core.UserText("旧记录")}},
		Pause:    &checkpoint.Pause{Phase: "model", Recovery: checkpoint.RecoveryReplayModel},
	}
	legacy, err := json.Marshal(legacyCP)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(legacy), `"completion"`) {
		t.Fatalf("旧记录夹具不该带 completion 键: %s", legacy)
	}
	if err := os.WriteFile(filepath.Join(dir, "t3.jsonl"), append(legacy, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	f, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	old, err := f.Latest(ctx, "t3")
	if err != nil {
		t.Fatal(err)
	}
	if old.ID != "old" || old.Completion != nil || old.Pause == nil {
		t.Fatalf("旧记录读出异常: %+v", old)
	}
	if got := old.State.Messages[0].Text(); got != "旧记录" {
		t.Fatalf("旧记录状态丢失: %q", got)
	}

	if err := f.Save(ctx, terminalCheckpoint("t3", "new", completionFixture("run-9", []byte(`{}`), nil), nil)); err != nil {
		t.Fatal(err)
	}
	reopened, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := reopened.History(ctx, "t3")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("历史条数 = %d，期望 2", len(hist))
	}
	if hist[0].ID != "old" || hist[0].Completion != nil {
		t.Fatalf("旧记录被误读出终态: %+v", hist[0].Completion)
	}
	if hist[1].ID != "new" || hist[1].Completion == nil || hist[1].Completion.RunID != "run-9" {
		t.Fatalf("新记录终态丢失: %+v", hist[1].Completion)
	}
}

// --- Fork -------------------------------------------------------------------

// TestCompletionForkClearsTerminal：从带终态的快照 Fork 会清除 Completion，
// 同时保留原有文件快照语义（nil 与非 nil 空快照不同）。
func TestCompletionForkClearsTerminal(t *testing.T) {
	cases := []struct {
		name     string
		snapshot map[string][]byte
		nilWant  bool
	}{
		{"无文件快照", nil, true},
		{"空文件快照", map[string][]byte{}, false},
		{"有文件", map[string][]byte{"note.txt": []byte("内容")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parent := terminalCheckpoint("t1", "cp1", completionFixture("run-1", []byte(`{"key":"before"}`), nil), tc.snapshot)
			child, err := checkpoint.Fork(parent, "t2", "cp2")
			if err != nil {
				t.Fatalf("Fork: %v", err)
			}
			if child.Completion != nil {
				t.Fatalf("Fork 未清除终态记录: %+v", child.Completion)
			}
			if parent.Completion == nil || parent.Completion.RunID != "run-1" {
				t.Fatalf("Fork 破坏了父记录的终态: %+v", parent.Completion)
			}
			if tc.nilWant {
				if child.FileSnapshot != nil {
					t.Fatalf("nil 文件快照应仍为 nil，得到 %v", child.FileSnapshot)
				}
			} else if !reflect.DeepEqual(child.FileSnapshot, tc.snapshot) {
				t.Fatalf("文件快照不符: %v vs %v", child.FileSnapshot, tc.snapshot)
			}

			// Fork 之后再改写父记录的可变字节，子记录本来就没有终态，不该被写入。
			// 用等长替换，才不会把 RawMessage 截断成非法 JSON。
			parentArgs, _ := mutablePayloads(t, parent.Completion)
			copy(parentArgs, []byte(`{"key":"after!"}`))
			if child.Completion != nil {
				t.Fatal("Fork 之后父方改写写进了子记录")
			}

			// 两条都落盘后再读回：分支无终态，父记录的终态与文件快照不受影响。
			ctx := context.Background()
			dir := completionDir(t)
			f, err := checkpoint.NewFile(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Save(ctx, parent); err != nil {
				t.Fatal(err)
			}
			branch, err := checkpoint.Fork(parent, "t3", "cp3")
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Save(ctx, branch); err != nil {
				t.Fatal(err)
			}
			reopened, err := checkpoint.NewFile(dir)
			if err != nil {
				t.Fatal(err)
			}
			storedBranch, err := reopened.Load(ctx, "t3", "cp3")
			if err != nil {
				t.Fatal(err)
			}
			if storedBranch.Completion != nil {
				t.Fatalf("Fork 出的分支记录带上了终态: %+v", storedBranch.Completion)
			}
			if tc.nilWant {
				if storedBranch.FileSnapshot != nil {
					t.Fatalf("分支 nil 文件快照读回变成了 %v", storedBranch.FileSnapshot)
				}
			} else if !reflect.DeepEqual(storedBranch.FileSnapshot, tc.snapshot) {
				t.Fatalf("分支文件快照不符: %v vs %v", storedBranch.FileSnapshot, tc.snapshot)
			}
			storedParent, err := reopened.Load(ctx, "t1", "cp1")
			if err != nil {
				t.Fatal(err)
			}
			if storedParent.Completion == nil || storedParent.Completion.Kind != checkpoint.CompletionCompleted {
				t.Fatalf("父记录终态丢失: %+v", storedParent.Completion)
			}
		})
	}
}

// --- 提交摘要 ---------------------------------------------------------------

// digestBaseline 是修改摘要之前，下面 digestFixture 这份"无终态"快照的提交摘要。
// 用它钉住契约：Completion 缺席时摘要不变，旧日志的幂等重试不会被误判为冲突。
const digestBaseline = "44d357aa58156e2b271a557a4c2c6713a571fd4987a8d1062bd1b8c1962ec2e2"

func digestFixture(id string, cp *checkpoint.Completion) *checkpoint.Checkpoint {
	return &checkpoint.Checkpoint{
		ID:       id,
		ThreadID: "digest-thread",
		ParentID: "root",
		Step:     3,
		State: core.State{
			Messages: []core.Message{core.UserText("原始提问"), core.AssistantText("回答")},
			KV:       map[string]any{"k": "v"},
		},
		Completion: cp,
	}
}

func digestTerminal() *checkpoint.Completion {
	return &checkpoint.Completion{
		RunID:  "run-A",
		Kind:   checkpoint.CompletionCompleted,
		Result: resultFixture("answer A", []byte(`{"key":"before"}`), []byte("imagex")),
	}
}

// journalRequestDigest 取出 File 日志里某个检查点提交记录的外层摘要。
func journalRequestDigest(t *testing.T, dir, threadID, checkpointID string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, threadID+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Checkpoint *struct {
				ID string `json:"id"`
			} `json:"checkpoint"`
			RequestDigest string `json:"request_digest"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil || rec.Checkpoint == nil {
			continue
		}
		if rec.Checkpoint.ID == checkpointID && rec.RequestDigest != "" {
			return rec.RequestDigest
		}
	}
	t.Fatalf("日志里找不到 %s 的提交摘要", checkpointID)
	return ""
}

// TestCompletionDigestKeepsNilBaseline：无终态快照的提交摘要必须与加入该字段之前
// 一致，并且重开日志后同内容重试仍按幂等成功处理。
func TestCompletionDigestKeepsNilBaseline(t *testing.T) {
	dir := completionDir(t)
	f, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := f.Save(ctx, digestFixture("same-id", nil)); err != nil {
		t.Fatal(err)
	}
	if got := journalRequestDigest(t, dir, "digest-thread", "same-id"); got != digestBaseline {
		t.Fatalf("无终态快照的摘要变了：新=%s 基线=%s", got, digestBaseline)
	}
	reopened, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Save(ctx, digestFixture("same-id", nil)); err != nil {
		t.Fatalf("重开后同内容重试应幂等成功，实际: %v", err)
	}
	hist, err := reopened.History(ctx, "digest-thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("幂等重试追加了记录: %d", len(hist))
	}
}

// TestCompletionDigestDistinguishesTerminal：只有同 ID 且终态内容完全相同才算重复；
// 终态的每个字段、结果的内部字节，以及 nil 与非 nil 的切换都必须判为冲突，
// 冲突后存储内容不变。Memory 与 File（含重开）都验证。
func TestCompletionDigestDistinguishesTerminal(t *testing.T) {
	ctx := context.Background()
	for _, storeName := range []string{"memory", "file"} {
		t.Run(storeName, func(t *testing.T) {
			t.Run("同内容重试成功", func(t *testing.T) {
				local := newDigestBackend(t, storeName)
				if err := local.store.Save(ctx, digestFixture("same-id", digestTerminal())); err != nil {
					t.Fatal(err)
				}
				if err := local.store.Save(ctx, digestFixture("same-id", digestTerminal())); err != nil {
					t.Fatalf("同内容重试被拒: %v", err)
				}
				if err := local.reopen().Save(ctx, digestFixture("same-id", digestTerminal())); err != nil {
					t.Fatalf("重开后同内容重试被拒: %v", err)
				}
				latest, err := local.reopen().Latest(ctx, "digest-thread")
				if err != nil {
					t.Fatal(err)
				}
				if latest.Completion == nil || latest.Completion.RunID != "run-A" {
					t.Fatalf("幂等重试改写了存储: %+v", latest.Completion)
				}
			})

			conflicting := map[string]func(*checkpoint.Completion) *checkpoint.Completion{
				"RunID 不同": func(c *checkpoint.Completion) *checkpoint.Completion { c.RunID = "run-B"; return c },
				"Kind 不同":  func(c *checkpoint.Completion) *checkpoint.Completion { c.Kind = checkpoint.CompletionFailed; return c },
				"Err 不同":   func(c *checkpoint.Completion) *checkpoint.Completion { c.Err = "failed B"; return c },
				"结果文本不同": func(c *checkpoint.Completion) *checkpoint.Completion {
					c.Result = resultFixture("answer B", []byte(`{"key":"before"}`), []byte("imagex"))
					return c
				},
				"结果内部字节不同": func(c *checkpoint.Completion) *checkpoint.Completion {
					c.Result = resultFixture("answer A", []byte(`{"key":"after!"}`), []byte("imagex"))
					return c
				},
				"结果由非 nil 变 nil": func(c *checkpoint.Completion) *checkpoint.Completion { c.Result = nil; return c },
				"终态整体变 nil":      func(c *checkpoint.Completion) *checkpoint.Completion { return nil },
			}
			for label, mutate := range conflicting {
				t.Run(label, func(t *testing.T) {
					local := newDigestBackend(t, storeName)
					if err := local.store.Save(ctx, digestFixture("same-id", digestTerminal())); err != nil {
						t.Fatal(err)
					}
					if err := local.store.Save(ctx, digestFixture("same-id", mutate(digestTerminal()))); !errors.Is(err, checkpoint.ErrCheckpointConflict) {
						t.Fatalf("同 ID 不同终态应冲突，实际: %v", err)
					}
					// 冲突不能改写已存内容，也不能追加记录。
					latest, err := local.reopen().Latest(ctx, "digest-thread")
					if err != nil {
						t.Fatal(err)
					}
					if latest.Completion == nil || latest.Completion.RunID != "run-A" ||
						latest.Completion.Kind != checkpoint.CompletionCompleted ||
						latest.Completion.Err != "" || latest.Completion.Result == nil {
						t.Fatalf("冲突请求改写了存储: %+v", latest.Completion)
					}
					args, image := mutablePayloads(t, latest.Completion)
					if string(args) != `{"key":"before"}` || string(image) != "imagex" {
						t.Fatalf("冲突请求改写了结果字节: %s / %s", args, image)
					}
					hist, err := local.reopen().History(ctx, "digest-thread")
					if err != nil {
						t.Fatal(err)
					}
					if len(hist) != 1 {
						t.Fatalf("冲突请求追加了记录: %d", len(hist))
					}
				})
			}

			// 反方向：先提交无终态，再提交带终态的同 ID 记录同样冲突。
			t.Run("先 nil 后非 nil", func(t *testing.T) {
				local := newDigestBackend(t, storeName)
				if err := local.store.Save(ctx, digestFixture("same-id", nil)); err != nil {
					t.Fatal(err)
				}
				if err := local.store.Save(ctx, digestFixture("same-id", digestTerminal())); !errors.Is(err, checkpoint.ErrCheckpointConflict) {
					t.Fatalf("无终态记录被同 ID 终态覆盖应冲突，实际: %v", err)
				}
				latest, err := local.reopen().Latest(ctx, "digest-thread")
				if err != nil {
					t.Fatal(err)
				}
				if latest.Completion != nil {
					t.Fatalf("无终态记录被写入终态: %+v", latest.Completion)
				}
			})
		})
	}
}

// newDigestBackend 给每个子用例一套独立实例，避免同 ID 记录互相干扰。
func newDigestBackend(t *testing.T, name string) backend {
	t.Helper()
	if name == "memory" {
		m := checkpoint.NewMemory()
		return backend{store: m, reopen: func() checkpoint.Checkpointer { return m }}
	}
	dir := t.TempDir()
	open := func() checkpoint.Checkpointer {
		f, err := checkpoint.NewFile(dir)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	return backend{store: open(), reopen: open}
}
