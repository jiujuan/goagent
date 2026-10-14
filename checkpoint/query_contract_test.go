package checkpoint

// TASK-SM-10（ADR-0034 D7）：轻量摘要投影、版本化 cursor 与 Memory 查询。本文件按 store
// 工厂循环写，SM-11 接入 File 时同一套契约直接复用。谁在写入时维护摘要索引属于 Memory 的
// 实现（query_memory.go）；本文件只钉调用方看得见的行为：摘要不含载荷、分页在并发新增下
// 不重不漏、筛选与排序可重复、无效或过期 token 各有明确错误。

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/core"
)

const queryPayloadMarker = "PAYLOAD-MARKER-9133"

var queryClockBase = time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC)

// queryAt 是相对基准时钟的一个时间点，用来让摘要里的 CreatedAt 可预测。
func queryAt(d time.Duration) time.Time { return queryClockBase.Add(d) }

type queryBackend struct {
	name    string
	durable Durable
	query   Query
	reader  MetadataReader
	// setNow 把存储时钟固定到 at，并注册 t.Cleanup 之外的显式复位由测试自己负责。
	setNow func(t *testing.T, at time.Time)
	// pinned 报告当前缓存的投影代数，用来钉住投影上限。
	pinned func() int
}

type queryStore struct {
	name string
	open func(t *testing.T) queryBackend
}

func queryStores() []queryStore {
	return []queryStore{
		{name: "memory", open: openMemoryQueryBackend},
		{name: "file", open: openFileQueryBackend},
	}
}

func openFileQueryBackend(t *testing.T) queryBackend {
	t.Helper()
	m, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	setNow := func(t *testing.T, at time.Time) {
		t.Helper()
		m.mu.Lock()
		defer m.mu.Unlock()
		m.now = func() time.Time { return at }
		m.projections.now = func() time.Time { return at }
	}
	setNow(t, queryClockBase)
	return queryBackend{
		name:    "file",
		durable: m,
		query:   m,
		reader:  m,
		setNow:  setNow,
		pinned:  func() int { return m.projections.count() },
	}
}

func openMemoryQueryBackend(t *testing.T) queryBackend {
	t.Helper()
	m := NewMemory()
	setNow := func(t *testing.T, at time.Time) {
		t.Helper()
		m.mu.Lock()
		defer m.mu.Unlock()
		m.now = func() time.Time { return at }
		m.projections.now = func() time.Time { return at }
	}
	setNow(t, queryClockBase)
	return queryBackend{
		name:    "memory",
		durable: m,
		query:   m,
		reader:  m,
		setNow:  setNow,
		pinned:  func() int { return m.projections.count() },
	}
}

// checkpointSpec 描述一条要提交的快照；测试通过它控制身份、时间和终态，其余字段由 helper
// 填成"形状合法的最小记录"。
type checkpointSpec struct {
	thread string
	id     string
	runID  string
	turnID string
	// at 非零时写进 CreatedAt；零值表示让存储自己盖时间。
	at      time.Time
	kind    string
	paused  bool
	legacy  bool
	payload bool
}

func queryCommit(t *testing.T, b queryBackend, spec checkpointSpec) CommitReceipt {
	t.Helper()
	cp := &Checkpoint{
		ID:       spec.id,
		ThreadID: spec.thread,
		Step:     1,
		ParentID: "parent-" + spec.id,
		RunID:    spec.runID,
		TurnID:   spec.turnID,
	}
	if !spec.legacy {
		cp.ParentRef = &Ref{ThreadID: spec.thread, CheckpointID: "parent-" + spec.id}
	}
	if !spec.at.IsZero() {
		cp.CreatedAt = spec.at
	}
	if spec.kind != "" {
		cp.Completion = &Completion{RunID: spec.runID, Kind: spec.kind}
		if spec.kind == CompletionCompleted {
			cp.Completion.Result = &core.Result{Message: core.Message{
				Role:  core.RoleAssistant,
				Parts: []core.Part{core.Text{Text: queryPayloadMarker}},
			}}
		} else {
			cp.Completion.Err = queryPayloadMarker
		}
	}
	if spec.paused {
		cp.Pending = &PendingHITL{Step: 1, Pending: []core.ToolCall{{
			ID: "call-1", Name: "slow_tool", Args: json.RawMessage(`{"note":"` + queryPayloadMarker + `"}`),
		}}}
		cp.Pause = &Pause{ID: "pause-" + spec.id, Phase: "tool", Recovery: RecoveryResumeTools}
	}
	if spec.payload {
		cp.State = core.State{
			Messages: []core.Message{{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: queryPayloadMarker}}}},
			KV:       map[string]any{"note": queryPayloadMarker},
		}
		cp.FileSnapshot = map[string][]byte{"note.txt": []byte(queryPayloadMarker)}
	}
	rec, err := b.durable.Commit(context.Background(), CommitRequest{Checkpoint: cp})
	if err != nil {
		t.Fatalf("commit %s/%s: %v", spec.thread, spec.id, err)
	}
	return rec
}

func queryMustJSON(t *testing.T, v any) string {
	t.Helper()
	encoded, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	return string(encoded)
}

func queryRefs(items []CheckpointMetadata) []string {
	refs := make([]string, 0, len(items))
	for _, item := range items {
		refs = append(refs, item.Ref.CheckpointID)
	}
	return refs
}

func querySameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// queryCommitEight seeds the cross-thread order this file relies on:
// a1(60s) a2(120s,alpha) b1(120s,beta) a3(180s) a4(240s,alpha) b2(240s,beta) a5(300s) b3(360s).
func queryCommitEight(t *testing.T, b queryBackend) []string {
	t.Helper()
	specs := []checkpointSpec{
		{thread: "alpha", id: "a1", runID: "run-a1", turnID: "turn-1", at: queryAt(60 * time.Second)},
		{thread: "alpha", id: "a2", runID: "run-a2", turnID: "turn-2", at: queryAt(120 * time.Second)},
		{thread: "beta", id: "b1", runID: "run-b1", turnID: "turn-1", at: queryAt(120 * time.Second)},
		{thread: "alpha", id: "a3", runID: "run-a3", turnID: "turn-3", at: queryAt(180 * time.Second)},
		{thread: "alpha", id: "a4", runID: "run-a4", turnID: "turn-4", at: queryAt(240 * time.Second)},
		{thread: "beta", id: "b2", runID: "run-b2", turnID: "turn-2", at: queryAt(240 * time.Second)},
		{thread: "alpha", id: "a5", runID: "run-a5", turnID: "turn-5", at: queryAt(300 * time.Second)},
		{thread: "beta", id: "b3", runID: "run-b3", turnID: "turn-3", at: queryAt(360 * time.Second)},
	}
	for _, spec := range specs {
		queryCommit(t, b, spec)
	}
	return []string{"a1", "a2", "b1", "a3", "a4", "b2", "a5", "b3"}
}

func TestMetadataNoPayload(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommit(t, b, checkpointSpec{
				thread: "alpha", id: "cp-1", runID: "run-1", turnID: "turn-1",
				kind: CompletionCompleted, paused: true, payload: true,
			})

			page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("QueryCheckpoints: %v", err)
			}
			if len(page.Items) != 1 {
				t.Fatalf("摘要条数 = %d, 期望 1", len(page.Items))
			}
			encodedPage := queryMustJSON(t, page)
			queryAssertNoPayload(t, encodedPage)

			item, ok, err := b.reader.ReadCheckpointMetadata(context.Background(), Ref{ThreadID: "alpha", CheckpointID: "cp-1"})
			if err != nil || !ok {
				t.Fatalf("ReadCheckpointMetadata = ok %v err %v", ok, err)
			}
			queryAssertNoPayload(t, queryMustJSON(t, item))

			sessions, err := b.query.ListSessions(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("ListSessions: %v", err)
			}
			queryAssertNoPayload(t, queryMustJSON(t, sessions))

			if item.Revision != 1 {
				t.Errorf("Revision = %d, 期望 1", item.Revision)
			}
			if item.Step != 1 {
				t.Errorf("Step = %d, 期望 1", item.Step)
			}
			if item.RunID != "run-1" || item.TurnID != "turn-1" {
				t.Errorf("身份 = %q/%q, 期望 run-1/turn-1", item.RunID, item.TurnID)
			}
			if item.Ref != (Ref{ThreadID: "alpha", CheckpointID: "cp-1"}) {
				t.Errorf("Ref = %+v", item.Ref)
			}
			if item.ParentRef == nil || item.ParentRef.CheckpointID != "parent-cp-1" {
				t.Errorf("ParentRef = %+v, 期望 parent-cp-1", item.ParentRef)
			}
			if item.LegacyParentUnresolved {
				t.Errorf("带完整 ParentRef 的记录不应报 LegacyParentUnresolved")
			}
			if !item.Paused {
				t.Errorf("Paused = false, 期望 true（这条记录带着待批准的工具体）")
			}
			if item.CompletionKind != CompletionCompleted {
				t.Errorf("CompletionKind = %q, 期望 %q", item.CompletionKind, CompletionCompleted)
			}
			if item.CreatedAt.IsZero() {
				t.Errorf("CreatedAt 为零, 期望存储盖上的可见时间")
			}

			queryAssertNoPayloadTypes(t, reflect.TypeOf(CheckpointMetadata{}))
			queryAssertNoPayloadTypes(t, reflect.TypeOf(SessionSummary{}))
		})
	}
}

// queryAssertNoPayload 检查序列化后的投影里既没有载荷文本，也没有承载载荷的字段名。
func queryAssertNoPayload(t *testing.T, encoded string) {
	t.Helper()
	if strings.Contains(encoded, queryPayloadMarker) {
		t.Errorf("摘要里出现了运行载荷文本: %s", encoded)
	}
	for _, forbidden := range []string{"messages", "\"kv\"", "state", "file_snapshot", "\"args\"", "pending"} {
		if strings.Contains(encoded, forbidden) {
			t.Errorf("摘要里出现了载荷字段 %s: %s", forbidden, encoded)
		}
	}
}

// queryAssertNoPayloadTypes 从类型层面钉住投影形态：字段只允许标识、时间与终态类别这些标量，
// 出现 slice/map/interface/别的 struct 就说明有人把 State 或消息塞进了摘要。
func queryAssertNoPayloadTypes(t *testing.T, typ reflect.Type) {
	t.Helper()
	if typ.Kind() != reflect.Struct {
		t.Fatalf("%s 不是投影 struct", typ)
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			t.Errorf("%s.%s 是非导出字段, 契约测试看不到它的序列化结果", typ, field.Name)
			continue
		}
		queryAssertNoPayloadType(t, field.Type)
	}
}

func queryAssertNoPayloadType(t *testing.T, typ reflect.Type) {
	t.Helper()
	switch typ.Kind() {
	case reflect.String, reflect.Bool, reflect.Int, reflect.Uint64:
		return
	case reflect.Struct:
		if typ == reflect.TypeOf(time.Time{}) {
			return
		}
		if typ != reflect.TypeOf(Ref{}) && typ != reflect.TypeOf(CheckpointMetadata{}) {
			t.Errorf("%s 是不允许的 struct 字段类型", typ)
			return
		}
		for i := 0; i < typ.NumField(); i++ {
			queryAssertNoPayloadType(t, typ.Field(i).Type)
		}
		return
	case reflect.Pointer:
		if typ.Elem() != reflect.TypeOf(Ref{}) {
			t.Errorf("%s 是指向 %s 的指针, 摘要不得持有记录指针", typ, typ.Elem())
			return
		}
		queryAssertNoPayloadType(t, typ.Elem())
		return
	default:
		t.Errorf("%s(%s) 不能作为摘要字段", typ.Kind(), typ)
	}
}

func TestQueryStablePages(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			expected := queryCommitEight(t, b)

			page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 2})
			if err != nil {
				t.Fatalf("首页: %v", err)
			}
			if got := queryRefs(page.Items); !querySameStrings(got, expected[:2]) {
				t.Fatalf("首页 = %v, 期望 %v", got, expected[:2])
			}
			if page.NextCursor == "" {
				t.Fatalf("首页没有给出续页 cursor")
			}

			// 在两页之间新增：一条排在已返回项之前、一条排在中间、一条与前页时间打平、
			// 一条由存储盖在当前时钟之前（模拟另一处提交把更早的记录带进来）。
			queryCommit(t, b, checkpointSpec{thread: "gamma", id: "g-early", runID: "run-g1", at: queryAt(30 * time.Second)})
			queryCommit(t, b, checkpointSpec{thread: "gamma", id: "g-mid", runID: "run-g2", at: queryAt(130 * time.Second)})
			queryCommit(t, b, checkpointSpec{thread: "gamma", id: "g-tie", runID: "run-g3", at: queryAt(120 * time.Second)})
			queryCommit(t, b, checkpointSpec{thread: "delta", id: "d-stamped", runID: "run-g4"})

			// 这些新增必须真的进到存储里，否则整测试可以空转。
			fresh, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: MaxQueryLimit})
			if err != nil {
				t.Fatalf("新鲜查询: %v", err)
			}
			if len(fresh.Items) != 12 {
				t.Fatalf("新鲜查询条数 = %d, 期望 12（8 条原有 + 4 条新增）", len(fresh.Items))
			}

			// 固定投影的续页：既不能重复也不能漏，且不包含投影之后新增的记录。
			got := append([]string(nil), page.Items[0].Ref.CheckpointID, page.Items[1].Ref.CheckpointID)
			cursor := page.NextCursor
			pages := 1
			for cursor != "" {
				next, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 2, Cursor: cursor})
				if err != nil {
					t.Fatalf("第 %d 页: %v", pages+1, err)
				}
				pages++
				if pages > 8 {
					t.Fatalf("翻页没有收敛: %v", got)
				}
				for _, item := range next.Items {
					got = append(got, item.Ref.CheckpointID)
				}
				cursor = next.NextCursor
			}
			if !querySameStrings(got, expected) {
				t.Errorf("跨页序列 = %v, 期望固定投影 %v", got, expected)
			}
			for _, id := range got {
				if strings.HasPrefix(id, "g-") || id == "d-stamped" {
					t.Errorf("固定投影里出现了翻页之后新增的 %s", id)
				}
			}
			if pages != 4 {
				t.Errorf("页数 = %d, 期望 4（8 条 / 每页 2 条）", pages)
			}
		})
	}
}

func TestQuerySingleThreadTimelineUsesRevision(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			// 提交顺序决定 Revision，显式时间故意倒着写，逼查询在"按时间"与"按 Revision"之间选。
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "s1", at: queryAt(500 * time.Second)})
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "s2", at: queryAt(400 * time.Second)})
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "s3", at: queryAt(300 * time.Second)})
			queryCommit(t, b, checkpointSpec{thread: "beta", id: "x1", at: queryAt(100 * time.Second)})

			page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{ThreadID: "alpha"})
			if err != nil {
				t.Fatalf("单线程查询: %v", err)
			}
			if got := queryRefs(page.Items); !querySameStrings(got, []string{"s1", "s2", "s3"}) {
				t.Errorf("单线程时间线 = %v, 期望按 Revision 升序 [s1 s2 s3]", got)
			}
			for i, item := range page.Items {
				if item.Revision != uint64(i+1) {
					t.Errorf("第 %d 条 Revision = %d", i, item.Revision)
				}
			}

			across, err := b.query.QueryCheckpoints(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("跨线程查询: %v", err)
			}
			if got := queryRefs(across.Items); !querySameStrings(got, []string{"x1", "s3", "s2", "s1"}) {
				t.Errorf("跨线程顺序 = %v, 期望按 (CreatedAt,ThreadID,Revision,ID)", got)
			}
		})
	}
}

func TestQueryFilterBinding(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "f1", runID: "run-1", at: queryAt(60 * time.Second), kind: CompletionCompleted})
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "f2", runID: "run-2", at: queryAt(120 * time.Second), kind: CompletionFailed})
			queryCommit(t, b, checkpointSpec{thread: "beta", id: "f3", runID: "run-3", at: queryAt(180 * time.Second), kind: CompletionCompleted})
			// 旧记录：存储时钟为零时提交，CreatedAt 保持未知。
			b.setNow(t, time.Time{})
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "f-legacy", runID: "run-1", legacy: true})
			b.setNow(t, queryClockBase)

			all, err := b.query.QueryCheckpoints(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("无筛选: %v", err)
			}
			if len(all.Items) != 4 {
				t.Fatalf("无筛选条数 = %d, 期望 4", len(all.Items))
			}

			cases := []struct {
				name     string
				opts     ListOptions
				expected []string
			}{
				// 带 ThreadID 的查询是单线程时间线，按 Revision 升序，不按时间。
				{"thread", ListOptions{ThreadID: "alpha"}, []string{"f1", "f2", "f-legacy"}},
				{"run", ListOptions{RunID: "run-1"}, []string{"f-legacy", "f1"}},
				{"completed", ListOptions{CompletionKind: CompletionCompleted}, []string{"f1", "f3"}},
				{"failed", ListOptions{CompletionKind: CompletionFailed}, []string{"f2"}},
				{"fromInclusive", ListOptions{CreatedFrom: queryAt(120 * time.Second)}, []string{"f2", "f3"}},
				{"beforeExclusive", ListOptions{CreatedBefore: queryAt(120 * time.Second)}, []string{"f1"}},
				{"window", ListOptions{CreatedFrom: queryAt(60 * time.Second), CreatedBefore: queryAt(180 * time.Second)}, []string{"f1", "f2"}},
				{"timeFilterDropsUnknown", ListOptions{CreatedBefore: queryAt(30 * time.Second)}, nil},
				{"threadAndRun", ListOptions{ThreadID: "beta", RunID: "run-3"}, []string{"f3"}},
				{"threadMiss", ListOptions{ThreadID: "gamma"}, nil},
			}
			for _, tc := range cases {
				page, err := b.query.QueryCheckpoints(context.Background(), tc.opts)
				if err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				if got := queryRefs(page.Items); !querySameStrings(got, tc.expected) {
					t.Errorf("%s: 结果 = %v, 期望 %v", tc.name, got, tc.expected)
				}
			}

			// 未知时间的记录在无时间筛选时诚实返回，带时间筛选时被排除。
			for _, item := range all.Items {
				if item.Ref.CheckpointID == "f-legacy" {
					if !item.CreatedAt.IsZero() {
						t.Errorf("旧记录 CreatedAt = %v, 期望保持未知", item.CreatedAt)
					}
					if !item.LegacyParentUnresolved {
						t.Errorf("只有 parent_id 的记录应报 LegacyParentUnresolved")
					}
					if item.ParentRef != nil {
						t.Errorf("旧记录 ParentRef = %+v, 期望 nil（不得猜测线程）", item.ParentRef)
					}
				}
			}

			// cursor 绑定筛选：换了任何筛选条件都用不了同一串 token。
			base := ListOptions{ThreadID: "alpha", Limit: 1}
			first, err := b.query.QueryCheckpoints(context.Background(), base)
			if err != nil || first.NextCursor == "" {
				t.Fatalf("筛选首页 = %+v, %v", first, err)
			}
			other := ListOptions{Limit: 1}
			otherWithRun := ListOptions{ThreadID: "alpha", RunID: "run-1", Limit: 1}
			otherWithKind := ListOptions{ThreadID: "alpha", CompletionKind: CompletionCompleted, Limit: 1}
			otherWithFrom := ListOptions{ThreadID: "alpha", CreatedFrom: queryAt(60 * time.Second), Limit: 1}
			bindings := []struct {
				name string
				opts ListOptions
			}{
				{"去掉 thread", other},
				{"换成 beta", ListOptions{ThreadID: "beta", Limit: 1}},
				{"加 run", otherWithRun},
				{"加 kind", otherWithKind},
				{"加时间", otherWithFrom},
			}
			for _, binding := range bindings {
				if _, err := b.query.QueryCheckpoints(context.Background(), binding.opts.WithCursorForTest(first.NextCursor)); !errors.Is(err, ErrInvalidCursor) {
					t.Errorf("%s: 错用 cursor 应报 ErrInvalidCursor, 实得 %v", binding.name, err)
				}
			}
			// 两个查询域互不相通。
			if _, err := b.query.ListSessions(context.Background(), ListOptions{Limit: 1, Cursor: first.NextCursor}); !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("checkpoint cursor 用在会话列表应报 ErrInvalidCursor, 实得 %v", err)
			}
			sessionPage, err := b.query.ListSessions(context.Background(), ListOptions{Limit: 1})
			if err != nil || sessionPage.NextCursor == "" {
				t.Fatalf("会话首页 = %+v, %v", sessionPage, err)
			}
			if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 1, Cursor: sessionPage.NextCursor}); !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("会话 cursor 用在 checkpoint 查询应报 ErrInvalidCursor, 实得 %v", err)
			}

			// 同一条 cursor 复用不推进投影：两次续页得到同一批内容。
			repeatA, err := b.query.QueryCheckpoints(context.Background(), ListOptions{ThreadID: "alpha", Limit: 1, Cursor: first.NextCursor})
			if err != nil {
				t.Fatalf("续页: %v", err)
			}
			repeatB, err := b.query.QueryCheckpoints(context.Background(), ListOptions{ThreadID: "alpha", Limit: 1, Cursor: first.NextCursor})
			if err != nil {
				t.Fatalf("重复续页: %v", err)
			}
			if !querySameStrings(queryRefs(repeatA.Items), queryRefs(repeatB.Items)) {
				t.Errorf("同一 cursor 两次续页结果不同: %v vs %v", queryRefs(repeatA.Items), queryRefs(repeatB.Items))
			}
		})
	}
}

// WithCursorForTest 只在本文件里用：把已有 token 装进一组新筛选。
func (o ListOptions) WithCursorForTest(cursor string) ListOptions {
	o.Cursor = cursor
	return o
}

func TestQueryExpiredCursor(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommitEight(t, b)

			page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 2})
			if err != nil || page.NextCursor == "" {
				t.Fatalf("首页 = %+v, %v", page, err)
			}
			sessions, err := b.query.ListSessions(context.Background(), ListOptions{Limit: 1})
			if err != nil || sessions.NextCursor == "" {
				t.Fatalf("会话首页 = %+v, %v", sessions, err)
			}

			b.setNow(t, queryClockBase.Add(CursorTTL-time.Second))
			if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 2, Cursor: page.NextCursor}); err != nil {
				t.Errorf("未到 15 分钟就过期: %v", err)
			}
			if _, err := b.query.ListSessions(context.Background(), ListOptions{Limit: 1, Cursor: sessions.NextCursor}); err != nil {
				t.Errorf("会话投影未到 15 分钟就过期: %v", err)
			}

			b.setNow(t, queryClockBase.Add(CursorTTL))
			if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 2, Cursor: page.NextCursor}); !errors.Is(err, ErrCursorExpired) {
				t.Errorf("满 15 分钟的 checkpoint cursor 应报 ErrCursorExpired, 实得 %v", err)
			}
			if _, err := b.query.ListSessions(context.Background(), ListOptions{Limit: 1, Cursor: sessions.NextCursor}); !errors.Is(err, ErrCursorExpired) {
				t.Errorf("满 15 分钟的会话 cursor 应报 ErrCursorExpired, 实得 %v", err)
			}

			// 过期不损坏存储：不带 cursor 的新查询照常工作。
			fresh, err := b.query.QueryCheckpoints(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("过期后的新查询: %v", err)
			}
			if len(fresh.Items) != 8 {
				t.Errorf("过期后的新查询条数 = %d, 期望 8", len(fresh.Items))
			}
		})
	}
}

func TestQueryProtocolConstants(t *testing.T) {
	if DefaultQueryLimit != 50 {
		t.Errorf("DefaultQueryLimit = %d, 期望 50", DefaultQueryLimit)
	}
	if MaxQueryLimit != 200 {
		t.Errorf("MaxQueryLimit = %d, 期望 200", MaxQueryLimit)
	}
	if CursorTTL != 15*time.Minute {
		t.Errorf("CursorTTL = %v, 期望 15m", CursorTTL)
	}
	if MaxPinnedProjections != 128 {
		t.Errorf("MaxPinnedProjections = %d, 期望 128", MaxPinnedProjections)
	}
}

func TestQueryInvalidCursor(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommitEight(t, b)
			pinnedPage, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 2})
			if err != nil {
				t.Fatalf("首页: %v", err)
			}
			// 版本位是被改坏的第一种可能：拿着一条真 token 只改版本号。
			tamperedVersion := queryDecodeToken(t, pinnedPage.NextCursor)
			tamperedVersion.Version = cursorFormatVersion + 1
			tamperedFilter := queryDecodeToken(t, pinnedPage.NextCursor)
			tamperedFilter.Filter = "deadbeef"
			// 代号与偏移也照同样的方式改：保留真指纹，让"只有这一处能拒"的判断不被前置校验掩盖。
			tamperedGeneration := queryDecodeToken(t, pinnedPage.NextCursor)
			tamperedGeneration.Generation = 0
			tamperedOffset := queryDecodeToken(t, pinnedPage.NextCursor)
			tamperedOffset.Offset = -1

			bad := []struct {
				name  string
				token string
			}{
				{"不是 base64", "not-a-token!!"},
				{"base64 但不是 JSON", base64.RawURLEncoding.EncodeToString([]byte("junk"))},
				{"未知版本", queryToken(t, tamperedVersion)},
				{"指纹被改写", queryToken(t, tamperedFilter)},
				{"代号为零", queryToken(t, tamperedGeneration)},
				{"负偏移", queryToken(t, tamperedOffset)},
				{"未知版本（伪造）", queryToken(t, cursorPayload{Version: cursorFormatVersion + 1, Generation: 1, Filter: "f", Offset: 1})},
				{"代号为零（伪造）", queryToken(t, cursorPayload{Version: cursorFormatVersion, Generation: 0, Filter: "f", Offset: 1})},
				{"负偏移（伪造）", queryToken(t, cursorPayload{Version: cursorFormatVersion, Generation: 1, Filter: "f", Offset: -1})},
			}
			for _, tc := range bad {
				if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 2, Cursor: tc.token}); !errors.Is(err, ErrInvalidCursor) {
					t.Errorf("%s: 应报 ErrInvalidCursor, 实得 %v", tc.name, err)
				}
				if _, err := b.query.ListSessions(context.Background(), ListOptions{Limit: 2, Cursor: tc.token}); !errors.Is(err, ErrInvalidCursor) {
					t.Errorf("%s（会话）: 应报 ErrInvalidCursor, 实得 %v", tc.name, err)
				}
			}

			// 偏移越过投影末尾：投影长度固定，越界就是 token 被改过。
			decoded := queryDecodeToken(t, pinnedPage.NextCursor)
			decoded.Offset = 99
			if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 2, Cursor: queryToken(t, decoded)}); !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("越界偏移应报 ErrInvalidCursor, 实得 %v", err)
			}

			for _, limit := range []int{MaxQueryLimit + 1, 1_000} {
				if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: limit}); !errors.Is(err, ErrInvalidQuery) {
					t.Errorf("Limit=%d 应报 ErrInvalidQuery, 实得 %v", limit, err)
				}
				if _, err := b.query.ListSessions(context.Background(), ListOptions{Limit: limit}); !errors.Is(err, ErrInvalidQuery) {
					t.Errorf("会话 Limit=%d 应报 ErrInvalidQuery, 实得 %v", limit, err)
				}
				if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: limit, Cursor: pinnedPage.NextCursor}); !errors.Is(err, ErrInvalidQuery) {
					t.Errorf("续页 Limit=%d 应报 ErrInvalidQuery, 实得 %v", limit, err)
				}
			}
			if page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: MaxQueryLimit}); err != nil || len(page.Items) != 8 {
				t.Errorf("Limit=上限 = %d 条, %v", len(page.Items), err)
			}
			for _, limit := range []int{0, -5} {
				page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: limit})
				if err != nil {
					t.Errorf("Limit=%d 应回落到默认值: %v", limit, err)
					continue
				}
				if len(page.Items) != 8 {
					t.Errorf("Limit=%d 的条数 = %d, 期望 8", limit, len(page.Items))
				}
			}
		})
	}
}

func queryToken(t *testing.T, payload cursorPayload) string {
	t.Helper()
	return encodeCursor(payload)
}

func queryDecodeToken(t *testing.T, token string) cursorPayload {
	t.Helper()
	payload, err := decodeCursor(token)
	if err != nil {
		t.Fatalf("decode %q: %v", token, err)
	}
	return payload
}

func TestQueryProjectionCap(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommitEight(t, b)

			var oldest string
			var newest string
			for i := 0; i < MaxPinnedProjections+1; i++ {
				page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 1})
				if err != nil {
					t.Fatalf("第 %d 次首页: %v", i, err)
				}
				if i == 0 {
					oldest = page.NextCursor
				}
				newest = page.NextCursor
			}
			if got := b.pinned(); got != MaxPinnedProjections {
				t.Errorf("投影代数 = %d, 期望上限 %d", got, MaxPinnedProjections)
			}
			if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 1, Cursor: oldest}); !errors.Is(err, ErrCursorExpired) {
				t.Errorf("最旧的投影被挤掉后应报 ErrCursorExpired, 实得 %v", err)
			}
			if _, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 1, Cursor: newest}); err != nil {
				t.Errorf("最新的投影应仍可用: %v", err)
			}
			if got := b.pinned(); got != MaxPinnedProjections {
				t.Errorf("淘汰后投影代数 = %d, 期望 %d", got, MaxPinnedProjections)
			}
		})
	}
}

func TestListSessionsEnumeratesCommittedThreads(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			// 只有收件箱、没有快照的线程不是持久化会话。
			if _, err := b.durable.Deliver(context.Background(), "ghost", InboxMessage{
				ID:      "msg-1",
				Message: core.Message{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: "hello"}}},
			}); err != nil {
				t.Fatalf("Deliver: %v", err)
			}

			queryCommit(t, b, checkpointSpec{thread: "zeta", id: "z1", runID: "run-z1", at: queryAt(60 * time.Second)})
			queryCommit(t, b, checkpointSpec{thread: "zeta", id: "z2", runID: "run-z2", at: queryAt(120 * time.Second), kind: CompletionFailed})
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "a1", runID: "run-a1", at: queryAt(180 * time.Second), kind: CompletionCompleted})

			page, err := b.query.ListSessions(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("ListSessions: %v", err)
			}
			var names []string
			for _, item := range page.Items {
				names = append(names, item.ThreadID)
			}
			if !querySameStrings(names, []string{"alpha", "zeta"}) {
				t.Fatalf("会话列表 = %v, 期望按 ThreadID 升序且不含 ghost", names)
			}
			if page.Items[1].Head.Ref.CheckpointID != "z2" || page.Items[1].Head.Revision != 2 {
				t.Errorf("zeta 的 head = %+v, 期望 z2/revision 2", page.Items[1].Head)
			}
			if page.Items[0].Head.CompletionKind != CompletionCompleted {
				t.Errorf("alpha 的 head 终态 = %q", page.Items[0].Head.CompletionKind)
			}
			if page.NextCursor != "" {
				t.Errorf("三条以内不该给续页 cursor: %q", page.NextCursor)
			}

			// 时间/终态筛选同样作用于 head；上限与 cursor 也走同一套投影规则。
			filtered, err := b.query.ListSessions(context.Background(), ListOptions{CreatedFrom: queryAt(150 * time.Second)})
			if err != nil {
				t.Fatalf("时间筛选会话列表: %v", err)
			}
			if len(filtered.Items) != 1 || filtered.Items[0].ThreadID != "alpha" {
				t.Errorf("时间筛选结果 = %+v, 期望只剩 alpha", filtered.Items)
			}
			byKind, err := b.query.ListSessions(context.Background(), ListOptions{CompletionKind: CompletionFailed})
			if err != nil {
				t.Fatalf("终态筛选会话列表: %v", err)
			}
			if len(byKind.Items) != 1 || byKind.Items[0].ThreadID != "zeta" {
				t.Errorf("终态筛选结果 = %+v, 期望只剩 zeta", byKind.Items)
			}

			first, err := b.query.ListSessions(context.Background(), ListOptions{Limit: 1})
			if err != nil || first.NextCursor == "" {
				t.Fatalf("分页首页 = %+v, %v", first, err)
			}
			second, err := b.query.ListSessions(context.Background(), ListOptions{Limit: 1, Cursor: first.NextCursor})
			if err != nil {
				t.Fatalf("分页续页: %v", err)
			}
			if len(second.Items) != 1 || second.Items[0].ThreadID != "zeta" {
				t.Errorf("续页 = %+v, 期望 zeta", second.Items)
			}

			// 失败的提交不会创建会话。
			_, err = b.durable.Commit(context.Background(), CommitRequest{
				Checkpoint:       &Checkpoint{ID: "d1", ThreadID: "delta"},
				ExpectedRevision: func() *uint64 { v := uint64(99); return &v }(),
			})
			if !errors.Is(err, ErrRevisionConflict) {
				t.Fatalf("越界 Revision 应报 ErrRevisionConflict, 实得 %v", err)
			}
			after, err := b.query.ListSessions(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("失败提交后的会话列表: %v", err)
			}
			for _, item := range after.Items {
				if item.ThreadID == "delta" {
					t.Errorf("失败的提交注册出了会话 delta: %+v", after.Items)
				}
			}
		})
	}
}

func TestQueryIndexIgnoresFailedCommit(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "ok-1", runID: "run-1", at: queryAt(60 * time.Second)})

			// Revision 预期不符。
			if _, err := b.durable.Commit(context.Background(), CommitRequest{
				Checkpoint:       &Checkpoint{ID: "bad-1", ThreadID: "alpha"},
				ExpectedRevision: func() *uint64 { v := uint64(77); return &v }(),
			}); !errors.Is(err, ErrRevisionConflict) {
				t.Fatalf("预期 Revision 冲突应报错: %v", err)
			}
			// 谱系自相矛盾（ParentRef 与 parent_id 不一致）。
			if _, err := b.durable.Commit(context.Background(), CommitRequest{
				Checkpoint: &Checkpoint{ID: "bad-2", ThreadID: "alpha", ParentID: "parent-x", ParentRef: &Ref{ThreadID: "alpha", CheckpointID: "parent-y"}},
			}); err == nil {
				t.Fatalf("矛盾的 ParentRef 应被拒绝")
			}

			page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("查询: %v", err)
			}
			if got := queryRefs(page.Items); !querySameStrings(got, []string{"ok-1"}) {
				t.Fatalf("失败提交改变了摘要索引: %v", got)
			}

			// 同一请求重投是幂等确认，不能再追加一条摘要。
			receipt := queryCommit(t, b, checkpointSpec{thread: "alpha", id: "ok-1", runID: "run-1", at: queryAt(60 * time.Second)})
			if receipt.Revision != 1 {
				t.Errorf("重投 Revision = %d, 期望 1", receipt.Revision)
			}
			retry, err := b.query.QueryCheckpoints(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("重投后查询: %v", err)
			}
			if len(retry.Items) != 1 {
				t.Fatalf("重投追加了摘要: %+v", retry.Items)
			}
			if retry.Items[0].CreatedAt != queryAt(60*time.Second) {
				t.Errorf("重投改写了 CreatedAt: %v", retry.Items[0].CreatedAt)
			}
		})
	}
}

func TestMetadataReaderHonestLookups(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "r1", runID: "run-1", turnID: "turn-1", at: queryAt(60 * time.Second)})
			b.setNow(t, time.Time{})
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "r-legacy", runID: "run-1", legacy: true})
			b.setNow(t, queryClockBase)

			item, ok, err := b.reader.ReadCheckpointMetadata(context.Background(), Ref{ThreadID: "alpha", CheckpointID: "r-legacy"})
			if err != nil || !ok {
				t.Fatalf("读旧记录 = ok %v err %v", ok, err)
			}
			if !item.CreatedAt.IsZero() {
				t.Errorf("旧记录 CreatedAt = %v, 期望未知", item.CreatedAt)
			}
			if !item.LegacyParentUnresolved || item.ParentRef != nil {
				t.Errorf("旧记录谱系 = %+v / %+v", item.ParentRef, item.LegacyParentUnresolved)
			}
			if item.CompletionKind != "" {
				t.Errorf("没有终态记录的 CompletionKind = %q, 期望空", item.CompletionKind)
			}

			// 只有线程对得上才返回：同一 CheckpointID 在别的线程上不存在。
			if _, ok, err := b.reader.ReadCheckpointMetadata(context.Background(), Ref{ThreadID: "beta", CheckpointID: "r1"}); err != nil || ok {
				t.Errorf("跨线程误读 = ok %v err %v", ok, err)
			}
			if _, ok, err := b.reader.ReadCheckpointMetadata(context.Background(), Ref{ThreadID: "alpha", CheckpointID: "nope"}); err != nil || ok {
				t.Errorf("不存在的快照 = ok %v err %v", ok, err)
			}

			head, ok, err := b.reader.ReadSessionHead(context.Background(), "alpha")
			if err != nil || !ok {
				t.Fatalf("读 head = ok %v err %v", ok, err)
			}
			if head.Ref.CheckpointID != "r-legacy" {
				t.Errorf("head = %+v, 期望最后可见的 r-legacy", head)
			}
			if _, ok, err := b.reader.ReadSessionHead(context.Background(), "ghost"); err != nil || ok {
				t.Errorf("未知线程 head = ok %v err %v", ok, err)
			}

			// reader 与查询页面对同一条记录给出相同摘要。
			page, err := b.query.QueryCheckpoints(context.Background(), ListOptions{ThreadID: "alpha"})
			if err != nil {
				t.Fatalf("查询: %v", err)
			}
			for _, item := range page.Items {
				read, ok, err := b.reader.ReadCheckpointMetadata(context.Background(), item.Ref)
				if err != nil || !ok {
					t.Fatalf("按页面 Ref 回读 %v = ok %v err %v", item.Ref, ok, err)
				}
				if queryMustJSON(t, read) != queryMustJSON(t, item) {
					t.Errorf("reader 与页面摘要不一致: %+v vs %+v", read, item)
				}
			}
		})
	}
}

func TestQueryPageMetadataIsolation(t *testing.T) {
	for _, store := range queryStores() {
		t.Run(store.name, func(t *testing.T) {
			b := store.open(t)
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "i1", runID: "run-1", at: queryAt(60 * time.Second)})
			queryCommit(t, b, checkpointSpec{thread: "alpha", id: "i2", runID: "run-2", at: queryAt(120 * time.Second)})

			first, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 1})
			if err != nil || len(first.Items) != 1 {
				t.Fatalf("首页 = %+v, %v", first, err)
			}
			first.Items[0].ParentRef.CheckpointID = "tampered"
			first.Items[0].RunID = "tampered-run"
			first.Items[0].Ref.CheckpointID = "tampered-ref"

			// 同一冻结投影回读被改写过的那一条：返回给调用方的必须是副本。
			reread := queryDecodeToken(t, first.NextCursor)
			reread.Offset = 0
			again, err := b.query.QueryCheckpoints(context.Background(), ListOptions{Limit: 1, Cursor: queryToken(t, reread)})
			if err != nil {
				t.Fatalf("回到投影开头: %v", err)
			}
			if again.Items[0].ParentRef.CheckpointID != "parent-i1" || again.Items[0].RunID != "run-1" {
				t.Errorf("调用方改动污染了冻结投影: %+v", again.Items[0])
			}

			second, err := b.query.QueryCheckpoints(context.Background(), ListOptions{})
			if err != nil {
				t.Fatalf("第二次查询: %v", err)
			}
			if second.Items[0].ParentRef.CheckpointID != "parent-i1" || second.Items[0].RunID != "run-1" {
				t.Errorf("调用方改动污染了新投影: %+v", second.Items[0])
			}
			read, ok, err := b.reader.ReadCheckpointMetadata(context.Background(), Ref{ThreadID: "alpha", CheckpointID: "i1"})
			if err != nil || !ok {
				t.Fatalf("回读: ok %v err %v", ok, err)
			}
			if read.ParentRef.CheckpointID != "parent-i1" {
				t.Errorf("reader 也看到了被改写的 ParentRef: %+v", read.ParentRef)
			}

			// reader 返回的同样必须是副本。
			read.ParentRef.CheckpointID = "tampered-again"
			secondRead, ok, err := b.reader.ReadCheckpointMetadata(context.Background(), Ref{ThreadID: "alpha", CheckpointID: "i1"})
			if err != nil || !ok {
				t.Fatalf("再次回读: ok %v err %v", ok, err)
			}
			if secondRead.ParentRef.CheckpointID != "parent-i1" {
				t.Errorf("摘要索引被调用方改写: %+v", secondRead.ParentRef)
			}
		})
	}
}
