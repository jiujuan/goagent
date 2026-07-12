package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
	"github.com/jiujuan/goagent/runner"
	"github.com/jiujuan/goagent/session"
	"github.com/jiujuan/goagent/tool"
)

func TestParallelPersistsRealBranchesAndDeterministicMerge(t *testing.T) {
	done := make(chan struct{})
	second := &signalAgent{name: "second", done: done}
	first := agent.New(agent.Config{
		Name:            "first",
		DisableTransfer: true,
		Model: mock.New("first", func(*llm.Request) *llm.Response {
			return mock.Text("first-declared")
		}),
	})
	root := agent.Parallel("parallel", &waitingAgent{done: done, inner: first}, second)
	store, sess, runErr := driveWithStore(root)
	_ = store
	if runErr != nil {
		t.Fatal(runErr)
	}

	events := sess.Events()
	if len(events) != 4 {
		t.Fatalf("logical event count = %d, want 4", len(events))
	}
	base := events[0]
	firstEvent, secondEvent, merge := events[1], events[2], events[3]
	if firstEvent.Author != "first" || secondEvent.Author != "second" {
		t.Fatalf("logical authors = [%s %s], want [first second]", firstEvent.Author, secondEvent.Author)
	}
	if firstEvent.ParentID != base.ID || secondEvent.ParentID != base.ID {
		t.Fatalf("branch parents = [%s %s], want base %s", firstEvent.ParentID, secondEvent.ParentID, base.ID)
	}
	if !firstEvent.Detached || !secondEvent.Detached {
		t.Fatal("parallel branch events were not persisted as detached")
	}
	if len(merge.MergeParents) != 2 || merge.MergeParents[0] != firstEvent.ID || merge.MergeParents[1] != secondEvent.ID {
		t.Fatalf("merge parents = %v, want [%s %s]", merge.MergeParents, firstEvent.ID, secondEvent.ID)
	}
	if sess.Leaf() != merge.ID {
		t.Fatalf("leaf = %s, want merge %s", sess.Leaf(), merge.ID)
	}
}

func TestParallelMergesIsolatedDirectToolState(t *testing.T) {
	left := stateWritingAgent("left", "left.key", "L")
	right := stateWritingAgent("right", "right.key", "R")
	_, sess, err := driveWithStore(agent.Parallel("parallel", left, right))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := sess.State().Get("left.key"); got != "L" {
		t.Fatalf("left.key = %v, want L", got)
	}
	if got, _ := sess.State().Get("right.key"); got != "R" {
		t.Fatalf("right.key = %v, want R", got)
	}
}

func TestParallelRejectsConflictingStateByDefault(t *testing.T) {
	root := agent.Parallel("parallel",
		stateWritingAgent("left", "shared", "L"),
		stateWritingAgent("right", "shared", "R"),
	)
	_, sess, err := driveWithStore(root)
	if err == nil {
		t.Fatal("conflicting parallel state returned nil error")
	}
	if _, ok := sess.State().Get("shared"); ok {
		t.Fatal("conflicting branch state was published")
	}
	if got := len(sess.Messages()); got != 1 {
		t.Fatalf("active messages = %d, want only the base user message", got)
	}
}

func TestParallelConflictPolicyIsDeterministic(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy agent.StateConflictPolicy
		want   string
	}{
		{name: "earlier", policy: agent.PreferEarlierBranch, want: "L"},
		{name: "later", policy: agent.PreferLaterBranch, want: "R"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := agent.ParallelWithOptions("parallel", agent.ParallelOptions{StateConflict: tc.policy},
				stateWritingAgent("left", "shared", "L"),
				stateWritingAgent("right", "shared", "R"),
			)
			_, sess, err := driveWithStore(root)
			if err != nil {
				t.Fatal(err)
			}
			if got, _ := sess.State().Get("shared"); got != tc.want {
				t.Fatalf("shared = %v, want %s", got, tc.want)
			}
		})
	}
}

func TestNestedParallelMergesIntoOuterBranch(t *testing.T) {
	inner := agent.Parallel("inner",
		stateWritingAgent("inner-left", "inner.left", "IL"),
		stateWritingAgent("inner-right", "inner.right", "IR"),
	)
	outer := agent.Parallel("outer", inner, stateWritingAgent("outer-right", "outer.right", "OR"))
	_, sess, err := driveWithStore(outer)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"inner.left": "IL", "inner.right": "IR", "outer.right": "OR"} {
		if got, _ := sess.State().Get(key); got != want {
			t.Fatalf("state %s = %v, want %s", key, got, want)
		}
	}
	var innerMerge, outerMerge *core.Event
	for _, event := range sess.Events() {
		if len(event.MergeParents) == 0 {
			continue
		}
		switch event.Author {
		case "inner":
			innerMerge = event
		case "outer":
			outerMerge = event
		}
	}
	if innerMerge == nil || !innerMerge.Detached {
		t.Fatalf("inner merge = %+v, want detached merge", innerMerge)
	}
	if outerMerge == nil || outerMerge.Detached {
		t.Fatalf("outer merge = %+v, want active merge", outerMerge)
	}
}

func TestTransferInsideParallelSeesOwnBranchHistory(t *testing.T) {
	var captured []core.Message
	child := agent.New(agent.Config{
		Name:            "child",
		DisableTransfer: true,
		Model: mock.New("child", func(req *llm.Request) *llm.Response {
			captured = core.CloneMessages(req.Messages)
			return mock.Text("child-done")
		}),
	})
	coordinator := agent.New(agent.Config{
		Name:      "coordinator",
		SubAgents: []agent.Agent{child},
		Model: mock.New("coordinator", func(*llm.Request) *llm.Response {
			return mock.CallTool("transfer", "transfer_to_agent", `{"agent_name":"child"}`)
		}),
	})
	_, _, err := driveWithStore(agent.Parallel("parallel", coordinator, replier("sibling", "sibling-done")))
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) < 3 {
		t.Fatalf("child history has %d messages, want user + transfer call + tool result", len(captured))
	}
	if captured[0].Role != core.RoleUser || captured[1].Role != core.RoleAssistant || captured[2].Role != core.RoleTool {
		t.Fatalf("child roles = %v, want [user assistant tool]", rolesOf(captured))
	}
	for _, message := range captured {
		if message.Text() == "sibling-done" {
			t.Fatal("transfer target observed sibling branch output")
		}
	}
}

func TestParallelBranchStateIsInvisibleToSibling(t *testing.T) {
	written := make(chan struct{})
	observed := make(chan bool, 1)
	writer := &stateProbeAgent{name: "writer", writeKey: "secret", writeValue: "value", done: written}
	reader := &stateProbeAgent{name: "reader", wait: written, readKey: "secret", observed: observed}
	_, _, err := driveWithStore(agent.Parallel("parallel", writer, reader))
	if err != nil {
		t.Fatal(err)
	}
	if <-observed {
		t.Fatal("reader branch observed writer branch state before merge")
	}
}

func TestParallelFailureDoesNotPublishMerge(t *testing.T) {
	failing := agentFunc{name: "failing", run: func(agent.InvocationContext) core.Stream {
		return core.Fail(errors.New("branch failed"))
	}}
	_, sess, err := driveWithStore(agent.Parallel("parallel", failing, replier("other", "other-done")))
	if err == nil {
		t.Fatal("parallel branch failure returned nil error")
	}
	if got := len(sess.Messages()); got != 1 {
		t.Fatalf("active messages = %d, want only base after failed fan-in", got)
	}
	for _, event := range sess.Events() {
		if len(event.MergeParents) > 0 {
			t.Fatal("failed parallel run published a merge event")
		}
	}
}

func TestParallelPartialEventDoesNotAdvanceBranchTip(t *testing.T) {
	partial := agentFunc{name: "partial", run: func(ictx agent.InvocationContext) core.Stream {
		return func(yield func(*core.Event, error) bool) {
			progress := core.AssistantText("partial")
			yield(&core.Event{ID: "partial-event", Author: "partial", Partial: true, Message: &progress}, nil)
			final := core.AssistantText("final")
			yield(&core.Event{ID: "final-event", Author: "partial", Message: &final}, nil)
		}
	}}
	_, sess, err := driveWithStore(agent.Parallel("parallel", partial))
	if err != nil {
		t.Fatal(err)
	}
	events := sess.Events()
	if len(events) != 3 {
		t.Fatalf("committed event count = %d, want user + final + merge", len(events))
	}
	if events[1].ID != "final-event" || events[1].ParentID != events[0].ID {
		t.Fatalf("final event = %+v, want direct child of base", events[1])
	}
	if events[2].MergeParents[0] != "final-event" {
		t.Fatalf("merge parents = %v, want [final-event]", events[2].MergeParents)
	}
}

func TestParallelMergePublishesStateDelete(t *testing.T) {
	store := session.InMemory()
	sess, _ := store.GetOrCreate(context.Background(), "goagent", "user", "session")
	sess.State().Set("remove", "present")
	remove := tool.New("remove_state", "remove branch state", func(ctx *tool.Context, _ struct{}) (string, error) {
		ctx.State.Delete("remove")
		return "removed", nil
	})
	remover := agent.New(agent.Config{
		Name: "remover", DisableTransfer: true, Tools: []tool.Tool{remove},
		Model: mock.New("remover", func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text("done")
			}
			return mock.CallTool("remove-call", "remove_state", `{}`)
		}),
	})
	r := runner.New(runner.Config{Root: agent.Parallel("parallel", remover), Store: store})
	for _, err := range r.Run(context.Background(), "user", "session", core.UserText("go")) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := sess.State().Get("remove"); ok {
		t.Fatal("merged state deletion was not published")
	}
}

func TestConcurrentToolStateFollowsModelCallOrder(t *testing.T) {
	secondWrote := make(chan struct{})
	first := tool.New("first", "write first value", func(ctx *tool.Context, _ struct{}) (string, error) {
		<-secondWrote // finish physically after the second tool
		ctx.State.Set("shared", "first")
		return "first", nil
	})
	second := tool.New("second", "write second value", func(ctx *tool.Context, _ struct{}) (string, error) {
		ctx.State.Set("shared", "second")
		close(secondWrote)
		return "second", nil
	})
	model := mock.New("model", func(req *llm.Request) *llm.Response {
		if len(req.Messages) > 1 {
			return mock.Text("done")
		}
		return &llm.Response{Message: core.Message{Role: core.RoleAssistant, Parts: []core.Part{
			core.ToolCall{ID: "1", Name: "first", Args: []byte(`{}`)},
			core.ToolCall{ID: "2", Name: "second", Args: []byte(`{}`)},
		}}, StopReason: llm.StopToolUse}
	})
	_, sess, err := driveWithStore(agent.New(agent.Config{
		Name: "agent", Model: model, Tools: []tool.Tool{first, second}, DisableTransfer: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := sess.State().Get("shared"); got != "second" {
		t.Fatalf("shared = %v, want second from the later model tool call", got)
	}
}

func TestStoppingParallelConsumerLeavesBaseActive(t *testing.T) {
	store := session.InMemory()
	r := runner.New(runner.Config{
		Root:  agent.Parallel("parallel", replier("left", "L"), replier("right", "R")),
		Store: store,
	})
	for event, err := range r.Run(context.Background(), "user", "session", core.UserText("go")) {
		if err != nil {
			t.Fatal(err)
		}
		if event.Author != "user" && !event.Partial {
			break
		}
	}
	sess, _ := store.GetOrCreate(context.Background(), "goagent", "user", "session")
	if got := len(sess.Messages()); got != 1 {
		t.Fatalf("active messages = %d, want only base after early stop", got)
	}
	for _, event := range sess.Events() {
		if len(event.MergeParents) > 0 {
			t.Fatal("early-stopped parallel run published a merge event")
		}
	}
}

func stateWritingAgent(name, key, value string) agent.Agent {
	write := tool.New("write_state", "write isolated branch state", func(ctx *tool.Context, _ struct{}) (string, error) {
		ctx.State.Set(key, value)
		return "written", nil
	})
	return agent.New(agent.Config{
		Name:            name,
		DisableTransfer: true,
		Tools:           []tool.Tool{write},
		Model: mock.New(name, func(req *llm.Request) *llm.Response {
			if _, ok := mock.LastToolResult(req); ok {
				return mock.Text(name + "-done")
			}
			return mock.CallTool(name+"-call", "write_state", `{}`)
		}),
	})
}

func driveWithStore(root agent.Agent) (session.Store, *session.Session, error) {
	store := session.InMemory()
	r := runner.New(runner.Config{Root: root, Store: store})
	var runErr error
	for _, err := range r.Run(context.Background(), "user", "session", core.UserText("go")) {
		if err != nil {
			runErr = err
		}
	}
	sess, err := store.GetOrCreate(context.Background(), "goagent", "user", "session")
	if err != nil {
		return store, nil, err
	}
	return store, sess, runErr
}

func rolesOf(messages []core.Message) []core.Role {
	out := make([]core.Role, len(messages))
	for i, message := range messages {
		out[i] = message.Role
	}
	return out
}

type agentFunc struct {
	name string
	run  func(agent.InvocationContext) core.Stream
}

func (a agentFunc) Name() string                                { return a.name }
func (a agentFunc) Description() string                         { return a.name }
func (a agentFunc) SubAgents() []agent.Agent                    { return nil }
func (a agentFunc) Run(ctx agent.InvocationContext) core.Stream { return a.run(ctx) }

type stateProbeAgent struct {
	name       string
	wait       <-chan struct{}
	done       chan struct{}
	writeKey   string
	writeValue any
	readKey    string
	observed   chan<- bool
}

func (a *stateProbeAgent) Name() string             { return a.name }
func (a *stateProbeAgent) Description() string      { return a.name }
func (a *stateProbeAgent) SubAgents() []agent.Agent { return nil }
func (a *stateProbeAgent) Run(ictx agent.InvocationContext) core.Stream {
	return func(yield func(*core.Event, error) bool) {
		if a.wait != nil {
			select {
			case <-a.wait:
			case <-ictx.Done():
				yield(nil, ictx.Err())
				return
			}
		}
		if a.readKey != "" {
			_, ok := ictx.MutableState().Get(a.readKey)
			a.observed <- ok
		}
		if a.writeKey != "" {
			ictx.MutableState().Set(a.writeKey, a.writeValue)
		}
		if a.done != nil {
			close(a.done)
		}
		msg := core.AssistantText(a.name + "-done")
		yield(&core.Event{ID: core.NewID("evt"), Author: a.name, Message: &msg}, nil)
	}
}

var _ agent.Agent = agentFunc{}
var _ agent.Agent = (*stateProbeAgent)(nil)
