package agent

// IF-06：没有线程协调能力的存储没有 BeginRun 帮忙拦截在途头，普通新运行必须
// 在 Run.prepare 的 restore 分支自己拒绝。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// statusPlainStore 只实现 Checkpointer 的四个方法，不实现 ThreadCoordinator。
type statusPlainStore struct {
	mu       sync.Mutex
	byThread map[string][]*checkpoint.Checkpoint
}

func newStatusPlainStore() *statusPlainStore {
	return &statusPlainStore{byThread: map[string][]*checkpoint.Checkpoint{}}
}

func (s *statusPlainStore) Save(_ context.Context, cp *checkpoint.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byThread[cp.ThreadID] = append(s.byThread[cp.ThreadID], cp)
	return nil
}

func (s *statusPlainStore) Load(_ context.Context, threadID, id string) (*checkpoint.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cp := range s.byThread[threadID] {
		if cp.ID == id {
			return cp, nil
		}
	}
	return nil, fmt.Errorf("checkpoint %q not found on thread %q", id, threadID)
}

func (s *statusPlainStore) Latest(_ context.Context, threadID string) (*checkpoint.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hist := s.byThread[threadID]
	if len(hist) == 0 {
		return nil, nil
	}
	return hist[len(hist)-1], nil
}

func (s *statusPlainStore) History(_ context.Context, threadID string) ([]*checkpoint.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*checkpoint.Checkpoint(nil), s.byThread[threadID]...), nil
}

func TestPrepareRejectsInFlightHead(t *testing.T) {
	ctx := context.Background()
	const threadID = "prepare-inflight"
	store := newStatusPlainStore()
	call := core.ToolCall{ID: "c1", Name: "rec"}
	if err := store.Save(ctx, &checkpoint.Checkpoint{
		ID:       "cp-inflight",
		ThreadID: threadID,
		Step:     0,
		State:    core.State{Messages: []core.Message{core.UserText("go")}},
		InFlight: &checkpoint.InFlightBatch{Step: 0, Calls: []core.ToolCall{call}},
		Snapshot: &checkpoint.StepSnapshot{Step: 0, Advertised: []string{"rec"}, ToolDigest: "seeded"},
	}); err != nil {
		t.Fatal(err)
	}

	var modelCalls, toolCalls atomic.Int32
	a, err := New(
		WithCheckpointer(store),
		WithModel(mock.New("m", func(*llm.Request) *llm.Response {
			modelCalls.Add(1)
			return mock.CallTool("c1", "rec", `{}`)
		})),
		WithTools(tool.New("rec", "记录调用", func(*tool.Context, struct{}) (string, error) {
			toolCalls.Add(1)
			return "ok", nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := a.Run(ctx, "next", OnThread(threadID)); !errors.Is(err, checkpoint.ErrResumeRequired) {
		t.Fatalf("新运行 error = %v, want ErrResumeRequired", err)
	}
	if got := modelCalls.Load(); got != 0 {
		t.Fatalf("被拒绝的新运行调用了 %d 次模型", got)
	}
	if got := toolCalls.Load(); got != 0 {
		t.Fatalf("被拒绝的新运行启动了 %d 次工具", got)
	}
}

// TestBuildTerminalRecordRejectsPendingBoundary keeps the sibling guard in the
// same condition: a boundary waiting for a person is not a committed step either.
func TestBuildTerminalRecordRejectsPendingBoundary(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	cp := &checkpoint.Checkpoint{
		ID:       "cp-pending",
		ThreadID: "pending-boundary",
		Step:     1,
		Pending:  &checkpoint.PendingHITL{Step: 1},
	}
	if err := store.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	head, err := store.Latest(ctx, cp.ThreadID)
	if err != nil || head == nil {
		t.Fatalf("read seeded head: head=%+v err=%v", head, err)
	}
	_, err = buildTerminalRecord(ctx, store, head.ThreadID, "completion-id", head.ID, head.Revision,
		&checkpoint.Completion{RunID: "run-1", Kind: checkpoint.CompletionCompleted}, "")
	if err == nil || !strings.Contains(err.Error(), "pause point") {
		t.Fatalf("pending boundary error = %v, want the pause-point refusal", err)
	}
}
