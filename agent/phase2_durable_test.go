package agent_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/bus"
	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/tool"
)

func TestDurableInboxDelivery(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	var sawDurable atomic.Bool
	model := mock.New("inbox", func(req *llm.Request) *llm.Response {
		for _, msg := range req.Messages {
			if msg.Role == core.RoleUser && msg.Text() == "durable instruction" {
				sawDurable.Store(true)
			}
		}
		return mock.Text("delivered")
	})
	a, err := agent.New(agent.WithModel(model), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}

	msg := checkpoint.InboxMessage{ID: "producer-message-1", Message: core.UserText("durable instruction")}
	if receipt, err := a.Deliver(ctx, "t1", msg); err != nil || receipt.Duplicate {
		t.Fatalf("first delivery = %+v, %v", receipt, err)
	}
	if receipt, err := a.Deliver(ctx, "t1", msg); err != nil || !receipt.Duplicate {
		t.Fatalf("duplicate delivery = %+v, %v", receipt, err)
	}

	got, err := a.Run(ctx, "normal request", agent.OnThread("t1"))
	if err != nil || got != "delivered" {
		t.Fatalf("run = %q, %v", got, err)
	}
	if !sawDurable.Load() {
		t.Fatal("model did not receive the durable inbox message")
	}
	inbox, err := store.Inbox(ctx, "t1")
	if err != nil || len(inbox) != 0 {
		t.Fatalf("inbox after committed turn = %+v, %v", inbox, err)
	}
}

func TestApprovalDecisionIdempotency(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	gate := &gateOnce{}
	var calls atomic.Int32
	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		calls.Add(1)
		return "done", nil
	})
	model := mock.New("approval", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			return mock.Text("saw:" + tr.Content[0].(core.Text).Text)
		}
		return mock.CallTool("c1", "danger", `{"path":"report"}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(danger),
		agent.WithMiddleware(gate),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}

	run := a.Stream(ctx, "go", agent.OnThread("t1"))
	events, cancel := run.Events(bus.Lossless)
	defer cancel()
	pending := waitForPause(t, run)
	decision := agent.ApprovalDecision{
		DecisionID: "approval-1", CallID: pending[0].CallID, PolicyVersion: "policy-v1", Approved: true,
	}
	first, err := run.RecordApproval(ctx, decision)
	if err != nil || first.Duplicate {
		t.Fatalf("first approval = %+v, %v", first, err)
	}
	duplicate, err := run.RecordApproval(ctx, decision)
	if err != nil || !duplicate.Duplicate || duplicate.Decision != first.Decision {
		t.Fatalf("duplicate approval = %+v, %v", duplicate, err)
	}
	// Recording against a settled pause must not reopen its sealed stream.
	pausedEvents := 0
	for ev := range events {
		switch ev.(type) {
		case core.ApprovalDecided:
			t.Fatal("approval reopened a sealed run stream")
		case core.Interrupted:
			pausedEvents++
		}
	}
	if pausedEvents != 1 {
		t.Fatalf("pause terminal events=%d", pausedEvents)
	}
	recorded, err := store.Decisions(ctx, "t1", first.Decision.PauseID)
	if err != nil || len(recorded) != 1 || recorded[0] != first.Decision {
		t.Fatalf("durable decisions=%+v, %v", recorded, err)
	}

	conflict := decision
	conflict.DecisionID = "approval-2"
	if _, err := run.RecordApproval(ctx, conflict); !errors.Is(err, checkpoint.ErrDecisionConflict) {
		t.Fatalf("second decision for call error = %v, want ErrDecisionConflict", err)
	}
	for _, invalid := range []agent.ApprovalDecision{
		{DecisionID: "wrong-pause", CallID: pending[0].CallID, PolicyVersion: "policy-v1", PauseID: "not-the-pause", Approved: true},
		{DecisionID: "wrong-tool", CallID: pending[0].CallID, PolicyVersion: "policy-v1", Tool: "other", Approved: true},
		{DecisionID: "wrong-parameters", CallID: pending[0].CallID, PolicyVersion: "policy-v1", ParameterDigest: "wrong", Approved: true},
		{DecisionID: "wrong-digest", CallID: pending[0].CallID, PolicyVersion: "policy-v1", Digest: "wrong", Approved: true},
	} {
		if _, err := run.RecordApproval(ctx, invalid); err == nil {
			t.Fatalf("invalid approval %+v was accepted", invalid)
		}
	}

	continued, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var replayed bool
	for ev, err := range continued.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := ev.(core.ApprovalDecided); ok && got.Decision.DecisionID == first.Decision.DecisionID {
			replayed = true
		}
	}
	result, err := continued.Wait()
	if err != nil || result.Message.Text() != "saw:rewritten:done" {
		t.Fatalf("continued result = %+v, %v", result, err)
	}
	if !replayed {
		t.Fatal("resumed event stream did not replay the durable approval audit event")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("tool calls = %d, want 1", got)
	}
}

func TestUnknownToolOutcomeDoesNotReplay(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	gate := &gateOnce{}
	var calls atomic.Int32
	var seenUnknown atomic.Bool
	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		calls.Add(1)
		return "should not run", nil
	})
	model := mock.New("unknown", func(req *llm.Request) *llm.Response {
		if tr, ok := mock.LastToolResult(req); ok {
			if tr.IsError && strings.Contains(tr.Content[0].(core.Text).Text, "outcome was not durably recorded") {
				seenUnknown.Store(true)
			}
			return mock.Text("handled unknown outcome")
		}
		return mock.CallTool("c1", "danger", `{"target":"external"}`)
	})
	a, err := agent.New(
		agent.WithModel(model),
		agent.WithTools(danger),
		agent.WithMiddleware(gate),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	run, pending := pauseOn(t, a, ctx, "go")
	call := core.ToolCall{ID: pending[0].CallID, Name: pending[0].Tool, Args: pending[0].Args}
	invocationID, idempotencyKey := phase2Invocation("t1", 0, call)
	claimed, err := store.ClaimTool(ctx, checkpoint.ToolClaimRequest{
		ThreadID: "t1", InvocationID: invocationID, CallID: call.ID, Tool: call.Name, IdempotencyKey: idempotencyKey,
	})
	if err != nil || claimed.State != checkpoint.ToolClaimed {
		t.Fatalf("simulate pre-crash tool claim = %+v, %v", claimed, err)
	}

	run.Decide(agent.Allow(call.ID))
	continued, err := run.Resume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := continued.Wait()
	if err != nil || result.Message.Text() != "handled unknown outcome" {
		t.Fatalf("continued result = %+v, %v", result, err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("unknown tool outcome replayed handler %d times", got)
	}
	if !seenUnknown.Load() {
		t.Fatal("model did not receive the explicit unknown-outcome result")
	}
}

func TestInboxAckSurvivesProcessKill(t *testing.T) {
	if os.Getenv("GOAGENT_INBOX_KILL_HELPER") == "1" {
		runInboxKillHelper(t)
		return
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestInboxAckSurvivesProcessKill$")
	cmd.Env = append(os.Environ(), "GOAGENT_INBOX_KILL_HELPER=1", "GOAGENT_DURABLE_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := scanProcessLines(stdout)
	waitForProcessLine(t, lines, "ready")
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("killed inbox helper exited successfully")
	}

	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := store.Inbox(context.Background(), "t1")
	if err != nil || len(inbox) != 1 || inbox[0].ID != "kill-message-1" || inbox[0].Message.Text() != "survive kill" {
		t.Fatalf("inbox after killing helper = %+v, %v; helper stderr: %s", inbox, err, stderr.String())
	}
}

func TestConcurrentResumeSingleClaim(t *testing.T) {
	if os.Getenv("GOAGENT_RESUME_CLAIM_HELPER") == "1" {
		runResumeClaimHelper(t)
		return
	}

	ctx := context.Background()
	dir := t.TempDir()
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	gate := &gateOnce{}
	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		return "parent should not execute", nil
	})
	a, err := agent.New(
		agent.WithModel(phase2ResumeModel()),
		agent.WithTools(danger),
		agent.WithMiddleware(gate),
		agent.WithCheckpointer(store),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, pending := pauseOn(t, a, ctx, "go")
	if pending[0].CallID != "c1" {
		t.Fatalf("pending call ID = %q, want c1", pending[0].CallID)
	}

	releasePath := filepath.Join(dir, "release-tool")
	first, firstLines, firstErr := startResumeClaimWorker(t, dir, releasePath)
	waitForProcessLine(t, firstLines, "tool-started")

	second := exec.Command(os.Args[0], "-test.run=^TestConcurrentResumeSingleClaim$")
	second.Env = resumeClaimEnv(dir, releasePath)
	output, err := second.CombinedOutput()
	if err != nil {
		t.Fatalf("second resume worker failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "claim-conflict") {
		t.Fatalf("second resume worker did not report claim conflict:\n%s", output)
	}
	if err := os.WriteFile(releasePath, []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err != nil {
		t.Fatalf("claim owner worker failed: %v; stderr: %s", err, firstErr.String())
	}
	lines := drainProcessLines(firstLines)
	if !containsLine(lines, "resume-complete") {
		t.Fatalf("claim owner did not complete: stdout=%v stderr=%s", lines, firstErr.String())
	}
}

func runInboxKillHelper(t *testing.T) {
	t.Helper()
	dir := os.Getenv("GOAGENT_DURABLE_DIR")
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := agent.New(agent.WithModel(mock.New("helper", func(*llm.Request) *llm.Response { return mock.Text("unused") })), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Deliver(context.Background(), "t1", checkpoint.InboxMessage{ID: "kill-message-1", Message: core.UserText("survive kill")}); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(os.Stdout, "ready")
	for {
		time.Sleep(time.Hour)
	}
}

func runResumeClaimHelper(t *testing.T) {
	t.Helper()
	dir := os.Getenv("GOAGENT_DURABLE_DIR")
	releasePath := os.Getenv("GOAGENT_RESUME_RELEASE")
	store, err := checkpoint.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	danger := tool.New("danger", "dangerous op", func(_ *tool.Context, _ struct{}) (string, error) {
		fmt.Fprintln(os.Stdout, "tool-started")
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(releasePath); err == nil {
				return "done", nil
			}
			if time.Now().After(deadline) {
				return "", errors.New("timed out waiting for parent to release tool")
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	a, err := agent.New(agent.WithModel(phase2ResumeModel()), agent.WithTools(danger), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	run, err := a.Resume(context.Background(), "t1", agent.Allow("c1"))
	if errors.Is(err, checkpoint.ErrClaimConflict) {
		fmt.Fprintln(os.Stdout, "claim-conflict")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Wait(); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(os.Stdout, "resume-complete")
}

func phase2ResumeModel() *mock.Model {
	return mock.New("resume", func(req *llm.Request) *llm.Response {
		if _, ok := mock.LastToolResult(req); ok {
			return mock.Text("complete")
		}
		return mock.CallTool("c1", "danger", "{}")
	})
}

func waitForPause(t *testing.T, run *agent.Run) []core.ApprovalRequest {
	t.Helper()
	var pending []core.ApprovalRequest
	for ev, err := range run.Iter() {
		if err != nil {
			t.Fatal(err)
		}
		if interrupted, ok := ev.(core.Interrupted); ok {
			pending = interrupted.Pending
		}
	}
	if len(pending) == 0 {
		t.Fatal("expected an interrupted run with pending approval")
	}
	return pending
}

func phase2Invocation(threadID string, step int, call core.ToolCall) (string, string) {
	h := sha256.New()
	_, _ = h.Write([]byte(threadID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(fmt.Sprint(step)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(call.ID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(call.Name))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(call.Args)
	sum := hex.EncodeToString(h.Sum(nil))
	return "tool-" + sum, "idemp-" + sum
}

func startResumeClaimWorker(t *testing.T, dir, releasePath string) (*exec.Cmd, <-chan string, *bytes.Buffer) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestConcurrentResumeSingleClaim$")
	cmd.Env = resumeClaimEnv(dir, releasePath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, scanProcessLines(stdout), &stderr
}

func resumeClaimEnv(dir, releasePath string) []string {
	return append(os.Environ(), "GOAGENT_RESUME_CLAIM_HELPER=1", "GOAGENT_DURABLE_DIR="+dir, "GOAGENT_RESUME_RELEASE="+releasePath)
}

func scanProcessLines(r interface{ Read([]byte) (int, error) }) <-chan string {
	lines := make(chan string, 16)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	return lines
}

func waitForProcessLine(t *testing.T, lines <-chan string, want string) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("process exited before printing %q", want)
			}
			if line == want {
				return
			}
		case <-timeout.C:
			t.Fatalf("timed out waiting for process line %q", want)
		}
	}
}

func drainProcessLines(lines <-chan string) []string {
	var out []string
	for line := range lines {
		out = append(out, line)
	}
	return out
}

func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}

// TestDeliverWithAgentPhase is the durable end of the phase contract: a record
// delivered before the run starts arrives at the first step whether it asks for
// before_model or is left unset (Agent.Deliver), while a next_turn record that
// arrives mid-turn stays in the inbox until the turn restarts. Parking the step-1
// model call is what puts the delivery inside the turn instead of before it.
func TestDeliverWithAgentPhase(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	thread := "phase-thread"
	if _, err := store.Deliver(ctx, thread, checkpoint.InboxMessage{
		ID:      "now",
		Message: core.UserText("durable now"),
	}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	m := newBlockedCallModel(2, func(call int, _ *llm.Request) *llm.Response {
		switch call {
		case 1:
			return mock.CallTool("c1", "quick", `{}`)
		case 2:
			return mock.CallTool("c2", "quick", `{}`)
		case 3:
			return mock.Text("mid")
		default:
			return mock.Text("final")
		}
	})
	a, err := agent.New(agent.WithModel(m), agent.WithTools(quickTool("quick")), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	run := a.Stream(ctx, "start", agent.OnThread(thread))
	driven := make(chan []core.Event, 1)
	go func() {
		events, res, err := collectRun(t, run)
		if err != nil {
			t.Errorf("run: %v", err)
		}
		if got := res.Message.Text(); got != "final" {
			t.Errorf("result = %q, want final", got)
		}
		driven <- events
	}()
	releaseOnce := sync.OnceFunc(func() { close(m.release) })
	t.Cleanup(releaseOnce)
	go func() {
		<-m.entered
		if _, err := a.DeliverWith(ctx, thread, checkpoint.InboxMessage{
			ID:      "later",
			Message: core.UserText("durable later"),
		}, core.InputNextTurn); err != nil {
			t.Errorf("DeliverWith: %v", err)
		}
		releaseOnce()
	}()
	events := <-driven

	history := m.history()
	if len(history) != 4 {
		t.Fatalf("model calls = %d, want 4", len(history))
	}
	if !hasText(history[0], "durable now") {
		t.Fatalf("the unset-phase inbox message skipped the first step: %v", history[0])
	}
	if hasText(history[2], "durable later") {
		t.Fatalf("the next_turn inbox message arrived mid-turn: %v", history[2])
	}
	if !hasText(history[3], "durable later") {
		t.Fatalf("the next_turn inbox message never arrived: %v", history[3])
	}

	var sawNow, sawLater bool
	for _, e := range deliveredEvents(events) {
		if e.Source != core.InputDurable {
			continue
		}
		switch e.Phase {
		case core.InputBeforeModel:
			sawNow = true
			if e.Step != 0 || len(e.MessageIDs) != 1 || e.MessageIDs[0] != "now" || e.Count != 1 {
				t.Fatalf("before_model durable delivery = %+v, want step 0 with only ID now", e)
			}
		case core.InputNextTurn:
			sawLater = true
			if e.Step != 3 || len(e.MessageIDs) != 1 || e.MessageIDs[0] != "later" || e.Count != 1 {
				t.Fatalf("next_turn durable delivery = %+v, want step 3 with only ID later", e)
			}
		default:
			t.Fatalf("unexpected durable delivery phase %q", e.Phase)
		}
	}
	if !sawNow || !sawLater {
		t.Fatalf("durable delivery events missing before_model=%v next_turn=%v", sawNow, sawLater)
	}

	inbox, err := store.Inbox(ctx, thread)
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(inbox) != 0 {
		t.Fatalf("inbox still holds %+v after both messages were consumed", inbox)
	}
}

// TestDeliverRejectsUnknownPhase keeps an unreadable delivery instruction out of
// the inbox instead of letting a later run guess what it meant.
func TestDeliverRejectsUnknownPhase(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.NewMemory()
	a, err := agent.New(agent.WithModel(mock.New("stub", func(*llm.Request) *llm.Response {
		return mock.Text("done")
	})), agent.WithCheckpointer(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = a.DeliverWith(ctx, "reject-thread", checkpoint.InboxMessage{
		ID:      "bad",
		Message: core.UserText("odd"),
	}, core.InputPhase("later"))
	if !errors.Is(err, core.ErrUnknownInputPhase) {
		t.Fatalf("DeliverWith error = %v, want ErrUnknownInputPhase", err)
	}
	inbox, err := store.Inbox(ctx, "reject-thread")
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(inbox) != 0 {
		t.Fatalf("inbox = %+v, want the refused message absent", inbox)
	}
}
