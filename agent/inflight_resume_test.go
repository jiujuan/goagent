package agent_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

// pairFor builds the assistant message that declares a tool batch.
func pairFor(calls []core.ToolCall) core.Message {
	return core.Message{Role: core.RoleAssistant, Parts: toolParts(calls)}
}

// inFlightToolDigest mirrors the loop's tool digest so a seeded snapshot
// describes the tool table the test builds.
func inFlightToolDigest(tools ...tool.Tool) string {
	schemas := tool.Schemas(tools)
	h := sha256.New()
	writeField := func(b []byte) {
		_, _ = h.Write(strconv.AppendInt(nil, int64(len(b)), 10))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write(b)
	}
	writeField(strconv.AppendInt(nil, int64(len(schemas)), 10))
	for _, s := range schemas {
		writeField([]byte(s.Name))
		writeField([]byte(s.Description))
		writeField(s.Parameters)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func inFlightSnapshot(step int, tools ...tool.Tool) *checkpoint.StepSnapshot {
	names := make([]string, len(tools))
	for i, t := range tools {
		names[i] = t.Name()
	}
	return &checkpoint.StepSnapshot{
		Step:           step,
		Advertised:     names,
		ToolDigest:     inFlightToolDigest(tools...),
		SettingsDigest: "seeded-settings",
	}
}

// seedInFlightHead writes a valid in-flight head, optionally together with the
// per-call ledger records a crashed run would have left.
func seedInFlightHead(
	t *testing.T,
	store checkpoint.Durable,
	threadID string,
	step int,
	calls []core.ToolCall,
	snapshot *checkpoint.StepSnapshot,
	msgs []core.Message,
	executions ...checkpoint.ToolExecution,
) *checkpoint.Checkpoint {
	t.Helper()
	cp := &checkpoint.Checkpoint{
		ID:       core.NewID("seed"),
		ThreadID: threadID,
		Step:     step,
		State:    core.State{Messages: msgs},
		InFlight: &checkpoint.InFlightBatch{Step: step, Calls: calls},
		Snapshot: snapshot,
	}
	if _, err := store.Commit(context.Background(), checkpoint.CommitRequest{
		Checkpoint:     cp,
		ToolExecutions: executions,
	}); err != nil {
		t.Fatalf("seed in-flight head: %v", err)
	}
	head, err := store.Latest(context.Background(), threadID)
	if err != nil {
		t.Fatalf("read seeded head: %v", err)
	}
	return head
}

func pendingExecution(t *testing.T, threadID string, step int, call core.ToolCall) checkpoint.ToolExecution {
	t.Helper()
	invocationID, key := phase2Invocation(threadID, step, call)
	return checkpoint.ToolExecution{
		InvocationID:   invocationID,
		CallID:         call.ID,
		Tool:           call.Name,
		IdempotencyKey: key,
		State:          checkpoint.ToolPending,
	}
}

func completedExecution(t *testing.T, threadID string, step int, call core.ToolCall) checkpoint.ToolExecution {
	t.Helper()
	ex := pendingExecution(t, threadID, step, call)
	ex.Attempt = 1
	ex.State = checkpoint.ToolCompleted
	ex.Result = &core.Message{Role: core.RoleTool, Parts: []core.Part{core.ToolResult{
		CallID:  call.ID,
		Name:    call.Name,
		Content: []core.Part{core.Text{Text: "stored result"}},
	}}}
	return ex
}

// staleDescTool keeps the name while changing the schema, so the current tool
// digest no longer matches a snapshot seeded before the change.
type staleDescTool struct{ tool.Tool }

func (staleDescTool) Description() string { return "an older description of the tool" }

func inFlightResumeModel() *mock.Model {
	return mock.New("inflight-resume", func(*llm.Request) *llm.Response { return mock.Text("done") })
}

func seedCalls() []core.ToolCall {
	return []core.ToolCall{{ID: "c1", Name: "rec", Args: json.RawMessage(`{}`)}}
}

func TestResumeDetectsInFlight(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := seedCalls()
	head := seedInFlightHead(t, store, "inflight-resume", 0, calls,
		inFlightSnapshot(0, rec), []core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, "inflight-resume", 0, calls[0]))

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, "inflight-resume")
	if err != nil {
		t.Fatalf("Resume(in-flight) = %v, want a run", err)
	}
	if run == nil || run.ThreadID != "inflight-resume" {
		t.Fatalf("resumed run = %+v, want the seeded thread", run)
	}
	claims := store.recordedClaims()
	if len(claims) != 1 {
		t.Fatalf("claims = %d, want 1", len(claims))
	}
	if want := "checkpoint-" + head.ID; claims[0].PauseID != want {
		t.Fatalf("claim pause id = %q, want %q", claims[0].PauseID, want)
	}
	latest, err := store.Latest(ctx, "inflight-resume")
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.ID != head.ID || latest.InFlight == nil {
		t.Fatalf("head after Resume = %+v, want the unchanged in-flight checkpoint %q", latest, head.ID)
	}
}

func TestResumeInFlightRejectsApprovals(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := seedCalls()
	seedInFlightHead(t, store, "inflight-approvals", 0, calls,
		inFlightSnapshot(0, rec), []core.Message{core.UserText("go"), pairFor(calls)})

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Resume(ctx, "inflight-approvals", agent.Allow("c1"))
	if !errors.Is(err, agent.ErrInFlightUnrecoverable) {
		t.Fatalf("Resume with approvals = %v, want ErrInFlightUnrecoverable", err)
	}
	if !strings.Contains(err.Error(), "approvals") {
		t.Fatalf("error = %q, want it to name approvals", err)
	}
	if got := len(store.recordedClaims()); got != 0 {
		t.Fatalf("claims = %d after a rejected resume, want 0", got)
	}
}

func TestResumeInFlightNeedsDurableStore(t *testing.T) {
	ctx := context.Background()
	mem := checkpoint.NewMemory()
	rec := newRecordingTool("rec")
	calls := seedCalls()
	cp := &checkpoint.Checkpoint{
		ID:       "plain-inflight",
		ThreadID: "inflight-plain",
		State:    core.State{Messages: []core.Message{core.UserText("go"), pairFor(calls)}},
		InFlight: &checkpoint.InFlightBatch{Step: 0, Calls: calls},
		Snapshot: inFlightSnapshot(0, rec),
	}
	if err := mem.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	plain := &plainSaveStore{Checkpointer: mem}

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(plain),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Resume(ctx, "inflight-plain")
	if !errors.Is(err, agent.ErrInFlightUnrecoverable) {
		t.Fatalf("Resume on a plain store = %v, want ErrInFlightUnrecoverable", err)
	}
	if !errors.Is(err, checkpoint.ErrDurableUnavailable) {
		t.Fatalf("error = %v, want ErrDurableUnavailable", err)
	}
}

func TestResumeInFlightRequiresResolvableTools(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := []core.ToolCall{{ID: "c1", Name: "ghost", Args: json.RawMessage(`{}`)}}
	snapshot := &checkpoint.StepSnapshot{
		Step:           0,
		Advertised:     []string{"ghost"},
		ToolDigest:     "ghost-digest",
		SettingsDigest: "seeded-settings",
	}
	seedInFlightHead(t, store, "inflight-ghost", 0, calls, snapshot,
		[]core.Message{core.UserText("go"), pairFor(calls)})

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Resume(ctx, "inflight-ghost")
	if !errors.Is(err, agent.ErrToolUnavailable) {
		t.Fatalf("Resume with an unresolvable tool = %v, want ErrToolUnavailable", err)
	}
	if !errors.Is(err, agent.ErrInFlightUnrecoverable) {
		t.Fatalf("error = %v, want ErrInFlightUnrecoverable", err)
	}
	if got := len(store.recordedClaims()); got != 0 {
		t.Fatalf("claims = %d after a rejected resume, want 0", got)
	}
}

func TestResumeInFlightMatchesAssistantMessage(t *testing.T) {
	calls := []core.ToolCall{
		{ID: "c1", Name: "rec", Args: json.RawMessage(`{}`)},
		{ID: "c2", Name: "rec", Args: json.RawMessage(`{}`)},
	}
	newAgent := func(t *testing.T, store checkpoint.Durable, rec tool.Tool) *agent.Agent {
		t.Helper()
		a, err := agent.New(
			agent.WithModel(inFlightResumeModel()),
			agent.WithTools(rec),
			agent.WithCheckpointer(store),
		)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	cases := []struct {
		name string
		msgs []core.Message
	}{
		{
			name: "last message is not assistant",
			msgs: []core.Message{
				core.UserText("go"),
				pairFor(calls),
				{Role: core.RoleTool, Parts: toolParts(nil)},
			},
		},
		{
			name: "assistant declared fewer calls",
			msgs: []core.Message{core.UserText("go"), pairFor(calls[:1])},
		},
		{
			name: "second call id differs",
			msgs: []core.Message{core.UserText("go"), pairFor([]core.ToolCall{
				calls[0],
				{ID: "c3", Name: "rec", Args: json.RawMessage(`{}`)},
			})},
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			threadID := fmt.Sprintf("inflight-mismatch-%d", i)
			store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
			rec := newRecordingTool("rec")
			head := seedInFlightHead(t, store, threadID, 0, calls, inFlightSnapshot(0, rec), tc.msgs)
			a := newAgent(t, store, rec)
			_, err := a.Resume(ctx, threadID)
			if !errors.Is(err, agent.ErrInFlightUnrecoverable) {
				t.Fatalf("Resume with a mismatched assistant message = %v, want ErrInFlightUnrecoverable", err)
			}
			// The mismatch is found after the claim, so that claim must be released
			// for another attempt.
			if _, err := store.Claim(ctx, checkpoint.ClaimRequest{
				ThreadID: threadID,
				Revision: head.Revision,
				PauseID:  "checkpoint-" + head.ID,
				Owner:    "test",
				Lease:    time.Minute,
			}); err != nil {
				t.Fatalf("claim after a failed history match = %v, want the claim released", err)
			}
		})
	}
	t.Run("rewritten args still match", func(t *testing.T) {
		ctx := context.Background()
		store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
		rec := newRecordingTool("rec")
		batch := []core.ToolCall{{ID: "c1", Name: "rec", Args: json.RawMessage(`{"v":1}`)}}
		declared := []core.ToolCall{{ID: "c1", Name: "rec", Args: json.RawMessage(`{"v":0}`)}}
		seedInFlightHead(t, store, "inflight-rewritten-args", 0, batch,
			inFlightSnapshot(0, rec), []core.Message{core.UserText("go"), pairFor(declared)})
		a := newAgent(t, store, rec)
		if _, err := a.Resume(ctx, "inflight-rewritten-args"); err != nil {
			t.Fatalf("Resume with rewritten args = %v, want success", err)
		}
	})
}

func TestResumeInFlightPreflightBeforeClaim(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := []core.ToolCall{{ID: "c1", Name: "ghost", Args: json.RawMessage(`{}`)}}
	snapshot := &checkpoint.StepSnapshot{
		Step:           0,
		Advertised:     []string{"ghost"},
		ToolDigest:     "ghost-digest",
		SettingsDigest: "seeded-settings",
	}
	seedInFlightHead(t, store, "inflight-preflight", 0, calls, snapshot,
		[]core.Message{core.UserText("go"), pairFor(calls)})
	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Resume(ctx, "inflight-preflight"); err == nil {
		t.Fatal("Resume accepted an unresolvable batch")
	}
	if got := len(store.recordedClaims()); got != 0 {
		t.Fatalf("claims = %d after preflight failed, want 0", got)
	}
	if got := len(store.recordedCommits()); got != 1 {
		t.Fatalf("commits = %d, want only the seed", got)
	}
}

func TestResumeInFlightDoesNotSetReplayBaseline(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := seedCalls()
	stale := staleDescTool{rec}
	seedInFlightHead(t, store, "inflight-no-baseline", 0, calls,
		inFlightSnapshot(0, stale), []core.Message{core.UserText("go"), pairFor(calls)},
		completedExecution(t, "inflight-no-baseline", 0, calls[0]))

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
		agent.WithStepSnapshotPolicy(agent.StepSnapshotStrict),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, "inflight-no-baseline")
	if err != nil {
		t.Fatal(err)
	}
	res, err := run.Wait()
	if err != nil {
		t.Fatalf("run after an in-flight resume = %v, want no drift error", err)
	}
	// The completed call replays without touching the handler, and the model is
	// asked only at the next step — where a replay baseline would have raised
	// strict drift against the changed description.
	if got := len(rec.invocations()); got != 0 {
		t.Fatalf("handlers ran %d times, want the completed call replayed", got)
	}
	if got := res.Message.Text(); got != "done" {
		t.Fatalf("result = %q, want the next step's answer", got)
	}
}

func TestResumeInFlightSummaryDriftBlocksExecution(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := seedCalls()
	seedInFlightHead(t, store, "inflight-drift-block", 0, calls,
		inFlightSnapshot(0, staleDescTool{rec}), []core.Message{core.UserText("go"), pairFor(calls)},
		pendingExecution(t, "inflight-drift-block", 0, calls[0]))

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Resume(ctx, "inflight-drift-block")
	if !errors.Is(err, agent.ErrStepViewDrift) {
		t.Fatalf("Resume after a tool change = %v, want ErrStepViewDrift", err)
	}
	if !errors.Is(err, agent.ErrInFlightUnrecoverable) {
		t.Fatalf("error = %v, want ErrInFlightUnrecoverable", err)
	}
	if got := len(store.recordedClaims()); got != 0 {
		t.Fatalf("claims = %d after drift rejection, want 0", got)
	}
}

func TestResumeInFlightSummaryDriftAllowsPureReplay(t *testing.T) {
	ctx := context.Background()
	store := &claimRecordingStore{Durable: checkpoint.NewMemory()}
	rec := newRecordingTool("rec")
	calls := seedCalls()
	seedInFlightHead(t, store, "inflight-drift-replay", 0, calls,
		inFlightSnapshot(0, staleDescTool{rec}), []core.Message{core.UserText("go"), pairFor(calls)},
		completedExecution(t, "inflight-drift-replay", 0, calls[0]))

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(ctx, "inflight-drift-replay")
	if err != nil {
		t.Fatalf("Resume of a pure replay after a tool change = %v, want success", err)
	}
	if got := len(store.recordedClaims()); got != 1 {
		t.Fatalf("claims = %d, want 1", got)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatalf("run after a pure replay = %v, want success", err)
	}
	if got := len(rec.invocations()); got != 0 {
		t.Fatalf("handlers ran %d times, want a pure replay", got)
	}
}

// brokenLedgerStore fails every tool-ledger read.
type brokenLedgerStore struct {
	checkpoint.Durable
	err error
}

func (s brokenLedgerStore) ToolExecution(context.Context, string, string) (checkpoint.ToolExecution, bool, error) {
	return checkpoint.ToolExecution{}, false, s.err
}

func TestResumeInFlightLedgerReadFailureRejects(t *testing.T) {
	ctx := context.Background()
	ledgerErr := errors.New("ledger unavailable")
	store := &claimRecordingStore{Durable: brokenLedgerStore{
		Durable: checkpoint.NewMemory(),
		err:     ledgerErr,
	}}
	rec := newRecordingTool("rec")
	calls := seedCalls()
	seedInFlightHead(t, store, "inflight-ledger-fail", 0, calls,
		inFlightSnapshot(0, rec), []core.Message{core.UserText("go"), pairFor(calls)})

	a, err := agent.New(
		agent.WithModel(inFlightResumeModel()),
		agent.WithTools(rec),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Resume(ctx, "inflight-ledger-fail")
	if !errors.Is(err, agent.ErrInFlightUnrecoverable) {
		t.Fatalf("Resume with a failing ledger = %v, want ErrInFlightUnrecoverable", err)
	}
	if !strings.Contains(err.Error(), "tool execution") {
		t.Fatalf("error = %q, want it to name the ledger read", err)
	}
	if got := len(store.recordedClaims()); got != 0 {
		t.Fatalf("claims = %d after ledger failure, want 0", got)
	}
}
