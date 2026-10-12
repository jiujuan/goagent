package checkpoint

// SM-01（ADR-0034 D2）：谱系元数据与旧记录兼容。本文件只覆盖字段的存储、克隆
// 隔离、ParentRef 校验、Fork 归属重写，以及旧 journal 的提交摘要字节语义；
// 谁在运行时分配这些身份属于 SM-02。

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
)

// 下面三条摘要是加入谱系字段之前，testdata/legacy_digest_v1.jsonl 里每条提交
// 记录的外层摘要。它们随 fixture 一起固定，用来钉住"新字段缺省时摘要不变"。
const (
	legacyDigestCP1 = "0605ec99c8de031b95ce636ce342e7e9fa000090dff909eff2b899a102356874"
	legacyDigestCP2 = "e2a38e13b71a748f29bda5b2aa34e9bcacefbe31272729aece6cf6f0235ea19e"
	legacyDigestCP3 = "434bb9165a9c100e42bfdcdbe29469fe1bb3eadfba1fdf2282e91dc8fb8a8365"
)

func lineageTime(second int) time.Time {
	return time.Date(2026, 10, 12, 8, 30, second, 123456789, time.UTC)
}

type lineageBackend struct {
	store  Checkpointer
	reopen func() Checkpointer
}

func lineageBackends(t *testing.T) map[string]func() lineageBackend {
	t.Helper()
	dir := t.TempDir()
	return map[string]func() lineageBackend{
		"memory": func() lineageBackend {
			m := NewMemory()
			return lineageBackend{store: m, reopen: func() Checkpointer { return m }}
		},
		"file": func() lineageBackend {
			open := func() Checkpointer {
				f, err := NewFile(dir)
				if err != nil {
					t.Fatal(err)
				}
				return f
			}
			return lineageBackend{store: open(), reopen: open}
		},
	}
}

// --- 往返与隔离 -------------------------------------------------------------

func metadataFixture(threadID string) []*Checkpoint {
	return []*Checkpoint{
		{
			ID:        "meta-cp-1",
			ThreadID:  threadID,
			Step:      0,
			RunID:     "run-1",
			TurnID:    "turn-1",
			CreatedAt: lineageTime(0),
			State:     core.State{Messages: []core.Message{core.UserText("原始提问")}},
		},
		{
			ID:        "meta-cp-2",
			ThreadID:  threadID,
			ParentID:  "meta-cp-1",
			ParentRef: &Ref{ThreadID: threadID, CheckpointID: "meta-cp-1"},
			Step:      1,
			RunID:     "run-1",
			TurnID:    "turn-1",
			CreatedAt: lineageTime(1),
			State: core.State{
				Messages: []core.Message{core.UserText("原始提问"), core.AssistantText("回答")},
			},
		},
		{
			// 跨线程来源：fork 出的分支首条记录用 ParentRef 指明来源线程。
			ID:        "meta-cp-3",
			ThreadID:  threadID + "-branch",
			ParentID:  "meta-cp-2",
			ParentRef: &Ref{ThreadID: threadID, CheckpointID: "meta-cp-2"},
			Step:      0,
			RunID:     "run-2",
			TurnID:    "turn-2",
			CreatedAt: lineageTime(2),
			State: core.State{
				Messages: []core.Message{core.UserText("原始提问"), core.AssistantText("回答")},
			},
		},
	}
}

func assertLineage(t *testing.T, want, got *Checkpoint) {
	t.Helper()
	if got.ParentID != want.ParentID {
		t.Fatalf("ParentID = %q, want %q", got.ParentID, want.ParentID)
	}
	switch {
	case want.ParentRef == nil && got.ParentRef != nil:
		t.Fatalf("ParentRef = %+v, want nil", got.ParentRef)
	case want.ParentRef != nil && got.ParentRef == nil:
		t.Fatalf("ParentRef = nil, want %+v", want.ParentRef)
	case want.ParentRef != nil && *want.ParentRef != *got.ParentRef:
		t.Fatalf("ParentRef = %+v, want %+v", *got.ParentRef, *want.ParentRef)
	}
	if got.RunID != want.RunID || got.TurnID != want.TurnID {
		t.Fatalf("身份字段 = %q/%q, want %q/%q", got.RunID, got.TurnID, want.RunID, want.TurnID)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("CreatedAt = %s, want %s", got.CreatedAt, want.CreatedAt)
	}
}

func TestMetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	for name, newBackend := range lineageBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackend()
			threadID := "meta-" + name
			wants := metadataFixture(threadID)
			for _, cp := range wants {
				if err := b.store.Save(ctx, cp); err != nil {
					t.Fatalf("save %s: %v", cp.ID, err)
				}
			}
			for _, want := range wants {
				got, err := b.reopen().Load(ctx, want.ThreadID, want.ID)
				if err != nil {
					t.Fatalf("load %s: %v", want.ID, err)
				}
				assertLineage(t, want, got)
			}

			// 旧形状记录只有 ParentID，仍然可读回且不会被补出 ParentRef。
			legacy := &Checkpoint{
				ID:       "meta-legacy",
				ThreadID: threadID,
				ParentID: "meta-cp-2",
				Step:     2,
				State:    core.State{Messages: []core.Message{core.UserText("原始提问")}},
			}
			if err := b.store.Save(ctx, legacy); err != nil {
				t.Fatalf("save legacy: %v", err)
			}
			gotLegacy, err := b.reopen().Load(ctx, threadID, "meta-legacy")
			if err != nil {
				t.Fatalf("load legacy: %v", err)
			}
			if gotLegacy.ParentRef != nil || gotLegacy.RunID != "" || gotLegacy.TurnID != "" || !gotLegacy.CreatedAt.IsZero() {
				t.Fatalf("旧记录被补写了元数据: %+v / %q %q %s", gotLegacy.ParentRef, gotLegacy.RunID, gotLegacy.TurnID, gotLegacy.CreatedAt)
			}
		})
	}
}

func TestMetadataCloneIsolation(t *testing.T) {
	ctx := context.Background()
	for name, newBackend := range lineageBackends(t) {
		t.Run(name, func(t *testing.T) {
			b := newBackend()
			threadID := "isolate-" + name
			cp := &Checkpoint{
				ID:        "iso-1",
				ThreadID:  threadID,
				ParentID:  "iso-0",
				ParentRef: &Ref{ThreadID: threadID, CheckpointID: "iso-0"},
				RunID:     "run-iso",
				TurnID:    "turn-iso",
				CreatedAt: lineageTime(3),
				State:     core.State{Messages: []core.Message{core.UserText("提问")}},
			}
			if err := b.store.Save(ctx, cp); err != nil {
				t.Fatal(err)
			}
			// 保存之后调用方继续改自己那份，不能污染存储。
			cp.ParentRef.CheckpointID = "tampered"
			cp.ParentRef.ThreadID = "tampered-thread"
			cp.RunID = "tampered-run"
			cp.TurnID = "tampered-turn"
			cp.CreatedAt = lineageTime(9)

			got, err := b.reopen().Load(ctx, threadID, "iso-1")
			if err != nil {
				t.Fatal(err)
			}
			if got.ParentRef.CheckpointID != "iso-0" || got.ParentRef.ThreadID != threadID {
				t.Fatalf("存储被调用方改动污染: %+v", got.ParentRef)
			}
			if got.RunID != "run-iso" || got.TurnID != "turn-iso" || !got.CreatedAt.Equal(lineageTime(3)) {
				t.Fatalf("存储被调用方改动污染: %q %q %s", got.RunID, got.TurnID, got.CreatedAt)
			}

			// 读出的那份同样不能与存储共享指针。
			got.ParentRef.CheckpointID = "mutated-by-reader"
			got.RunID = "mutated-by-reader"
			reread, err := b.reopen().Load(ctx, threadID, "iso-1")
			if err != nil {
				t.Fatal(err)
			}
			if reread.ParentRef.CheckpointID != "iso-0" || reread.RunID != "run-iso" {
				t.Fatalf("读取结果与存储共享数据: %+v %q", reread.ParentRef, reread.RunID)
			}
		})
	}
}

// --- ParentRef 校验 ---------------------------------------------------------

func TestParentRefValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		cp      *Checkpoint
		wantErr bool
	}{
		{
			name: "只有 ParentID 的旧形状",
			cp:   &Checkpoint{ID: "v-1", ThreadID: "validate", ParentID: "v-0"},
		},
		{
			name: "完整且一致的 ParentRef",
			cp: &Checkpoint{
				ID: "v-2", ThreadID: "validate", ParentID: "v-0",
				ParentRef: &Ref{ThreadID: "source-thread", CheckpointID: "v-0"},
			},
		},
		{
			name: "ParentRef 缺线程",
			cp: &Checkpoint{
				ID: "v-3", ThreadID: "validate", ParentID: "v-0",
				ParentRef: &Ref{CheckpointID: "v-0"},
			},
			wantErr: true,
		},
		{
			name: "ParentRef 缺检查点",
			cp: &Checkpoint{
				ID: "v-4", ThreadID: "validate", ParentID: "v-0",
				ParentRef: &Ref{ThreadID: "validate"},
			},
			wantErr: true,
		},
		{
			name: "ParentRef 与 ParentID 不一致",
			cp: &Checkpoint{
				ID: "v-5", ThreadID: "validate", ParentID: "v-0",
				ParentRef: &Ref{ThreadID: "validate", CheckpointID: "other"},
			},
			wantErr: true,
		},
		{
			name: "ParentRef 有值但 ParentID 为空",
			cp: &Checkpoint{
				ID: "v-6", ThreadID: "validate",
				ParentRef: &Ref{ThreadID: "validate", CheckpointID: "v-0"},
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for storeName, newBackend := range lineageBackends(t) {
				b := newBackend()
				err := b.store.Save(ctx, tc.cp)
				if tc.wantErr {
					if err == nil {
						t.Fatalf("%s 存储接受了不一致的 ParentRef", storeName)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s 存储拒绝了合法记录: %v", storeName, err)
				}
			}
		})
	}

	// Durable Commit 走同一道校验。
	m := NewMemory()
	if _, err := m.Commit(ctx, CommitRequest{Checkpoint: &Checkpoint{
		ID: "v-7", ThreadID: "validate", ParentID: "v-0",
		ParentRef: &Ref{ThreadID: "validate", CheckpointID: "mismatch"},
	}}); err == nil {
		t.Fatal("Commit 接受了不一致的 ParentRef")
	}

	// 读回的旧记录重新保存不该被拒。
	threadID := "legacy-reload"
	fixture := &Checkpoint{ID: "v-8", ThreadID: threadID, ParentID: "v-7", State: core.State{}}
	mem := NewMemory()
	if err := mem.Save(ctx, fixture); err != nil {
		t.Fatal(err)
	}
	loaded, err := mem.Load(ctx, threadID, "v-8")
	if err != nil {
		t.Fatal(err)
	}
	if err := NewMemory().Save(ctx, loaded); err != nil {
		t.Fatalf("重存旧记录被拒: %v", err)
	}
}

// --- Fork 归属 --------------------------------------------------------------

func TestForkLineageMetadata(t *testing.T) {
	ctx := context.Background()
	parent := &Checkpoint{
		ID:        "fork-parent",
		ThreadID:  "fork-thread",
		ParentID:  "fork-root",
		ParentRef: &Ref{ThreadID: "fork-thread", CheckpointID: "fork-root"},
		Step:      4,
		RunID:     "run-parent",
		TurnID:    "turn-parent",
		CreatedAt: lineageTime(4),
		State:     core.State{Messages: []core.Message{core.UserText("提问"), core.AssistantText("回答")}},
	}
	if err := NewMemory().Save(ctx, parent); err != nil {
		t.Fatal(err)
	}

	forkTime := lineageTime(7)
	prevClock := lineageNow
	lineageNow = func() time.Time { return forkTime }
	t.Cleanup(func() { lineageNow = prevClock })

	child, err := Fork(parent, "fork-child", "fork-child-cp")
	if err != nil {
		t.Fatal(err)
	}
	if child.RunID != "" || child.TurnID != "" {
		t.Fatalf("Fork 继承了运行身份: %q %q", child.RunID, child.TurnID)
	}
	if want := (Ref{ThreadID: "fork-thread", CheckpointID: "fork-parent"}); child.ParentRef == nil || *child.ParentRef != want {
		t.Fatalf("Fork 的 ParentRef = %+v, want %+v", child.ParentRef, want)
	}
	if child.ParentID != "fork-parent" {
		t.Fatalf("Fork 的 ParentID = %q", child.ParentID)
	}
	if !child.CreatedAt.Equal(forkTime) {
		t.Fatalf("Fork 的创建时间 = %s, want 重新分配的 %s", child.CreatedAt, forkTime)
	}

	// 同一请求的提交重试必须得到同一摘要，否则幂等重试会被误判成冲突。
	retry, err := Fork(parent, "fork-child", "fork-child-cp")
	if err != nil {
		t.Fatal(err)
	}
	first, err := commitRequestDigest(child, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := commitRequestDigest(retry, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("同一次 Fork 重试的摘要不稳定: %s vs %s", first, second)
	}

	// 旧形状的父记录（无 ParentRef）fork 后仍得到可定位的归属。
	legacyParent := &Checkpoint{ID: "legacy-parent", ThreadID: "legacy-thread", ParentID: "legacy-root"}
	legacyChild, err := Fork(legacyParent, "legacy-branch", "legacy-branch-cp")
	if err != nil {
		t.Fatal(err)
	}
	if want := (Ref{ThreadID: "legacy-thread", CheckpointID: "legacy-parent"}); legacyChild.ParentRef == nil || *legacyChild.ParentRef != want {
		t.Fatalf("旧父记录 fork 后 ParentRef = %+v, want %+v", legacyChild.ParentRef, want)
	}
}

// --- 旧 journal 摘要兼容 ----------------------------------------------------

func legacyJournal(t *testing.T) (string, []string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "legacy_digest_v1.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	// 仓库配置了 autocrlf，签出可能变成 CRLF；日志按行解析前统一回 LF。
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	dir := t.TempDir()
	var payload []byte
	for _, line := range lines {
		if line == "" {
			continue
		}
		payload = append(payload, append([]byte(line), '\n')...)
	}
	if err := os.WriteFile(filepath.Join(dir, "legacy-lineage.jsonl"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, line := range lines {
		if line == "" {
			continue
		}
		var rec struct {
			Checkpoint *struct {
				ID string `json:"id"`
			} `json:"checkpoint"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Checkpoint != nil {
			ids = append(ids, rec.Checkpoint.ID)
		}
	}
	return dir, ids
}

func recordedDigest(t *testing.T, dir, checkpointID string) string {
	t.Helper()
	fh, err := os.Open(filepath.Join(dir, "legacy-lineage.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	scanner := bufio.NewScanner(fh)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var rec struct {
			Checkpoint    *struct{ ID string } `json:"checkpoint"`
			RequestDigest string               `json:"request_digest"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		if rec.Checkpoint != nil && rec.Checkpoint.ID == checkpointID {
			return rec.RequestDigest
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("fixture 里找不到 %s", checkpointID)
	return ""
}

// legacyRequests  reproduce testdata/legacy_digest_v1.jsonl 的原始请求。测试用这份
// 固定数据重试，不允许在运行时用新代码生成"旧格式"。
func legacyRequests() []*Checkpoint {
	return []*Checkpoint{
		{
			ID:       "legacy-cp-1",
			ThreadID: "legacy-lineage",
			Step:     0,
			State: core.State{
				Messages: []core.Message{core.UserText("原始提问")},
				KV:       map[string]any{"mode": "mock"},
			},
		},
		{
			ID:       "legacy-cp-2",
			ThreadID: "legacy-lineage",
			ParentID: "legacy-cp-1",
			Step:     1,
			State: core.State{
				Messages: []core.Message{core.UserText("原始提问"), core.AssistantText("第一步回答")},
				KV:       map[string]any{"mode": "mock", "step": "1"},
			},
		},
		{
			ID:       "legacy-cp-3",
			ThreadID: "legacy-lineage",
			ParentID: "legacy-cp-2",
			Step:     2,
			State: core.State{
				Messages: []core.Message{
					core.UserText("原始提问"),
					core.AssistantText("第一步回答"),
					core.UserText("继续"),
					core.AssistantText("最终回答"),
				},
				KV: map[string]any{"mode": "mock", "step": "2"},
			},
			Completion: &Completion{
				RunID:  "legacy-run-1",
				Kind:   CompletionCompleted,
				Result: &core.Result{Message: core.AssistantText("最终回答")},
			},
		},
	}
}

func TestLegacyDigestRetry(t *testing.T) {
	ctx := context.Background()
	wantDigests := []string{legacyDigestCP1, legacyDigestCP2, legacyDigestCP3}
	requests := legacyRequests()

	t.Run("fixture 摘要与固定值一致", func(t *testing.T) {
		dir, ids := legacyJournal(t)
		if len(ids) != len(wantDigests) {
			t.Fatalf("fixture 提交数 = %d, want %d", len(ids), len(wantDigests))
		}
		for i, id := range ids {
			if got := recordedDigest(t, dir, id); got != wantDigests[i] {
				t.Fatalf("%s 的摘要 = %s, want %s", id, got, wantDigests[i])
			}
		}
	})

	t.Run("读取不重写旧文件", func(t *testing.T) {
		dir, _ := legacyJournal(t)
		fp := filepath.Join(dir, "legacy-lineage.jsonl")
		before, err := os.ReadFile(fp)
		if err != nil {
			t.Fatal(err)
		}
		f, err := NewFile(dir)
		if err != nil {
			t.Fatal(err)
		}
		hist, err := f.History(ctx, "legacy-lineage")
		if err != nil {
			t.Fatal(err)
		}
		for _, cp := range hist {
			if _, err := f.Load(ctx, cp.ThreadID, cp.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := f.Latest(ctx, cp.ThreadID); err != nil {
				t.Fatal(err)
			}
		}
		after, err := os.ReadFile(fp)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatalf("读取重写了旧日志：读前 %d 字节，读后 %d 字节", len(before), len(after))
		}
	})

	t.Run("原请求幂等重试", func(t *testing.T) {
		dir, _ := legacyJournal(t)
		f, err := NewFile(dir)
		if err != nil {
			t.Fatal(err)
		}
		for i, cp := range requests {
			if err := f.Save(ctx, cp); err != nil {
				t.Fatalf("重试第 %d 条原请求失败: %v", i+1, err)
			}
		}
		hist, err := f.History(ctx, "legacy-lineage")
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) != 3 {
			t.Fatalf("幂等重试追加了记录: %d", len(hist))
		}
		for i, cp := range hist {
			if cp.ParentRef != nil || cp.RunID != "" || cp.TurnID != "" || !cp.CreatedAt.IsZero() {
				t.Fatalf("第 %d 条旧记录被补写了元数据", i)
			}
		}
		if hist[1].ParentID != "legacy-cp-1" || hist[2].ParentID != "legacy-cp-2" {
			t.Fatalf("旧谱系链读回不符: %q %q", hist[1].ParentID, hist[2].ParentID)
		}
		for i, cp := range requests {
			if got := recordedDigest(t, dir, cp.ID); got != wantDigests[i] {
				t.Fatalf("%s 重试后摘要变了: %s vs %s", cp.ID, got, wantDigests[i])
			}
		}
	})

	t.Run("同 ID 异元数据判冲突", func(t *testing.T) {
		for name, mutate := range map[string]func(*Checkpoint){
			"加 RunID":     func(cp *Checkpoint) { cp.RunID = "run-later" },
			"加 TurnID":    func(cp *Checkpoint) { cp.TurnID = "turn-later" },
			"加 CreatedAt": func(cp *Checkpoint) { cp.CreatedAt = lineageTime(1) },
			"加 ParentRef": func(cp *Checkpoint) {
				cp.ParentRef = &Ref{ThreadID: cp.ThreadID, CheckpointID: cp.ParentID}
			},
		} {
			t.Run(name, func(t *testing.T) {
				dir, _ := legacyJournal(t)
				f, err := NewFile(dir)
				if err != nil {
					t.Fatal(err)
				}
				cp := *legacyRequests()[1]
				mutate(&cp)
				if err := f.Save(ctx, &cp); !errors.Is(err, ErrCheckpointConflict) {
					t.Fatalf("改元数据后重试应冲突, got %v", err)
				}
				got, err := f.Load(ctx, cp.ThreadID, cp.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.ParentRef != nil || got.RunID != "" || got.TurnID != "" || !got.CreatedAt.IsZero() {
					t.Fatalf("冲突写污染了存储: %+v", got)
				}
			})
		}
	})
}
