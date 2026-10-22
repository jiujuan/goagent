package agent_test

// IF-06：在途批次在查询、会话投影和分支路径上的读法。在途是"有工作没做完"，
// 暂停是"等人作决定"，两者不能互相冒充。

import (
	"context"
	"errors"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// TestRunStatusReportsInFlight：在途头部独立报告为 InFlight，不落进 Paused。
func TestRunStatusReportsInFlight(t *testing.T) {
	ctx := context.Background()
	const threadID = "status-inflight"
	store := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	calls := seedCalls()
	seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, threadID, 0, calls[0]))

	a := terminalAgent(t, store, inFlightResumeModel())
	status, err := a.RunStatus(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Exists || !status.InFlight {
		t.Fatalf("在途头部没有被报告: %+v", status)
	}
	if status.Paused {
		t.Fatalf("在途头部被当成暂停: %+v", status)
	}
	if status.Completion != nil {
		t.Fatalf("在途头部带了终态记录: %+v", status.Completion)
	}
	if status.Latest == nil || status.Latest.InFlight == nil {
		t.Fatalf("Latest 没有保留在途批次: %+v", status.Latest)
	}
}

// TestRunStatusPausedUnchanged：既有暂停读法不变；普通中间步骤两者都不报。
func TestRunStatusPausedUnchanged(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()

	const pausedThread = "status-paused-flag"
	pausedAgent, run := statusPausedAgent(t, store, pausedThread)
	if !run.Paused() {
		t.Fatal("夹具没有停在暂停点")
	}
	paused, err := pausedAgent.RunStatus(ctx, pausedThread)
	if err != nil {
		t.Fatal(err)
	}
	if !paused.Paused {
		t.Fatalf("审批暂停没有被报告: %+v", paused)
	}
	if paused.InFlight {
		t.Fatalf("暂停头部被当成在途: %+v", paused)
	}

	const plainThread = "status-plain-flag"
	plain := terminalAgent(t, store, mock.New("plain", func(*llm.Request) *llm.Response {
		return mock.Text("done")
	}))
	done := plain.Stream(ctx, "go", agent.OnThread(plainThread))
	if _, err := done.Wait(); err != nil {
		t.Fatal(err)
	}
	ordinary, err := plain.RunStatus(ctx, plainThread)
	if err != nil {
		t.Fatal(err)
	}
	if ordinary.Paused || ordinary.InFlight {
		t.Fatalf("普通步骤头部不该带暂停或在途标记: %+v", ordinary)
	}
}

// TestCheckpointMetadataInFlightVsPaused：Session 的两条读法对同一批头部给出
// 一致且互斥的答案——History 的投影分开两个标记，GetHead 的原始记录保留批次。
func TestCheckpointMetadataInFlightVsPaused(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()

	const inFlightThread = "metadata-inflight"
	rec := newRecordingTool("rec")
	calls := seedCalls()
	head := seedInFlightHead(t, store, inFlightThread, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)})
	a := terminalAgent(t, store, inFlightResumeModel())
	session, err := a.OpenSession(ctx, inFlightThread)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := session.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil || raw.InFlight == nil {
		t.Fatalf("原始头部丢了在途批次: %+v", raw)
	}
	page, err := session.History(ctx, checkpoint.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	inFlightMeta := metadataFor(t, page, head.ID)
	if !inFlightMeta.InFlight || inFlightMeta.Paused {
		t.Fatalf("在途头部的投影 = %+v", inFlightMeta)
	}

	const pausedThread = "metadata-paused"
	pausedAgent, _ := statusPausedAgent(t, store, pausedThread)
	pausedSession, err := pausedAgent.OpenSession(ctx, pausedThread)
	if err != nil {
		t.Fatal(err)
	}
	pausedHead, err := pausedSession.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pausedHead == nil || pausedHead.InFlight != nil {
		t.Fatalf("暂停头部带了在途批次: %+v", pausedHead)
	}
	pausedPage, err := pausedSession.History(ctx, checkpoint.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pausedMeta := metadataFor(t, pausedPage, pausedHead.ID)
	if !pausedMeta.Paused || pausedMeta.InFlight {
		t.Fatalf("暂停头部的投影 = %+v", pausedMeta)
	}
}

// TestSessionForkFromRejectsInFlight：agent 侧的分叉入口同样拒绝对在途头做分支。
func TestSessionForkFromRejectsInFlight(t *testing.T) {
	ctx := context.Background()
	const threadID = "fork-inflight"
	store := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	calls := seedCalls()
	head := seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec),
		[]core.Message{core.UserText("go"), pairFor(calls)})

	a := terminalAgent(t, store, inFlightResumeModel())
	session, err := a.OpenSession(ctx, threadID)
	if err != nil {
		t.Fatal(err)
	}
	branch, err := session.ForkFrom(ctx, head.ID, "fork-inflight-branch")
	if !errors.Is(err, checkpoint.ErrInFlightConflict) {
		t.Fatalf("ForkFrom error = %v, want ErrInFlightConflict", err)
	}
	if branch != nil {
		t.Fatalf("被拒绝的分叉仍返回了会话: %+v", branch)
	}
	target, err := store.Latest(ctx, "fork-inflight-branch")
	if err != nil {
		t.Fatal(err)
	}
	if target != nil {
		t.Fatalf("被拒绝的分叉写了目标线程: %+v", target)
	}
}

func metadataFor(t *testing.T, page checkpoint.MetadataPage, checkpointID string) checkpoint.CheckpointMetadata {
	t.Helper()
	for _, item := range page.Items {
		if item.Ref.CheckpointID == checkpointID {
			return item
		}
	}
	t.Fatalf("摘要里没有 checkpoint %q: %+v", checkpointID, page.Items)
	return checkpoint.CheckpointMetadata{}
}
