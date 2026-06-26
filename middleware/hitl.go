package middleware

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"slices"
	"strings"
	"sync"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// Decision is a human's verdict on one tool call gated by HumanInTheLoop.
type Decision struct {
	// Approve lets the call execute.
	Approve bool
	// EditedArgs, when set on an approved decision, replaces the call's Args so
	// the tool runs with human-corrected input.
	EditedArgs json.RawMessage
	// Reason explains a denial; it is fed back to the model so it can adapt.
	Reason string
}

// Approve approves a call unchanged.
func Approve() Decision { return Decision{Approve: true} }

// ApproveWithArgs approves a call but replaces its arguments.
func ApproveWithArgs(args json.RawMessage) Decision {
	return Decision{Approve: true, EditedArgs: args}
}

// Deny rejects a call; reason is surfaced to the model.
func Deny(reason string) Decision { return Decision{Reason: reason} }

// Approver is consulted (blocking) for each tool call that needs human review.
// It must respect ctx and return promptly on cancellation. A non-nil error
// fails the whole model call.
type Approver func(ctx context.Context, call core.ToolCall) (Decision, error)

// HITLOptions configures HumanInTheLoop.
type HITLOptions struct {
	// Gate decides whether a tool call requires human approval. Nil gates every
	// call; use RequireApprovalFor to gate only specific tools.
	Gate func(core.ToolCall) bool
	// Approver is consulted for each gated call. Required.
	Approver Approver
}

// HumanInTheLoop returns a Middleware that pauses before tool execution: every
// tool call the model requests is shown to a human Approver, who may approve it,
// approve it with edited arguments, or deny it with feedback. Approved calls
// flow on to the turn engine for normal execution; denied calls never run and
// the model is told they were rejected, so it can choose another path.
//
// It works purely as a model decorator: it intercepts the assistant response
// carrying the tool calls and rewrites it before the engine sees it. Denied
// calls are removed, so no orphaned tool_use ever reaches the provider. When a
// turn mixes approved and denied calls, the denial feedback is injected as a
// user note before the next model call; when every call in a turn is denied,
// the middleware re-invokes the model itself with the denials as tool results,
// so the turn does not dead-end. Injected feedback lives only in the current
// run's model context — like steering, it is not persisted as its own event.
func HumanInTheLoop(opts HITLOptions) Middleware {
	if opts.Approver == nil {
		panic("middleware: HumanInTheLoop requires an Approver")
	}
	gate := opts.Gate
	if gate == nil {
		gate = func(core.ToolCall) bool { return true }
	}
	return func(next llm.Model) llm.Model {
		h := &hitl{next: next, gate: gate, approver: opts.Approver}
		return Wrap(next, h.generate)
	}
}

type hitl struct {
	next     llm.Model
	gate     func(core.ToolCall) bool
	approver Approver

	mu      sync.Mutex
	pending []core.Message // denial notes to inject before the next model call
}

func (h *hitl) generate(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		// Work on a local copy: we may append synthetic messages (injected denial
		// feedback, or an internal denial round) without touching the engine's
		// history slice.
		work := *req
		work.Messages = slices.Clone(req.Messages)
		if notes := h.drain(); len(notes) > 0 {
			work.Messages = append(work.Messages, notes...)
		}

		for {
			final, ok := h.streamCapture(ctx, &work, yield)
			if !ok {
				return // error or consumer-stop already handled
			}
			if len(h.gatedCalls(final.Message)) == 0 {
				yield(final, nil) // nothing to approve: pass through untouched
				return
			}

			rewritten, denials, err := h.adjudicate(ctx, final)
			if err != nil {
				yield(nil, err)
				return
			}

			if len(rewritten.Message.ToolCalls()) > 0 {
				// Some calls survived: hand them to the engine. Stash any denial
				// feedback for the engine's next model call.
				if len(denials) > 0 {
					h.stash(denialNote(denials))
				}
				yield(rewritten, nil)
				return
			}

			// Every gated call was denied. Ending the turn here would dead-end
			// the model, so re-invoke it ourselves with the denials as tool
			// results and let it react.
			work.Messages = append(work.Messages, final.Message, denialResults(denials))
		}
	}
}

// streamCapture consumes one model call, forwarding partial responses for live
// rendering and returning the final (non-partial) one. The bool is false when
// the call errored or the consumer stopped (already reported via yield).
func (h *hitl) streamCapture(ctx context.Context, req *llm.Request, yield func(*llm.Response, error) bool) (*llm.Response, bool) {
	var final *llm.Response
	for resp, err := range h.next.Generate(ctx, req) {
		if err != nil {
			yield(nil, err)
			return nil, false
		}
		if resp.Partial {
			if !yield(resp, nil) {
				return nil, false
			}
			continue
		}
		final = resp
	}
	if final == nil {
		final = &llm.Response{Message: core.AssistantText(""), StopReason: llm.StopEnd}
	}
	return final, true
}

// gatedCalls returns the tool calls in msg that require approval.
func (h *hitl) gatedCalls(msg core.Message) []core.ToolCall {
	var out []core.ToolCall
	for _, c := range msg.ToolCalls() {
		if h.gate(c) {
			out = append(out, c)
		}
	}
	return out
}

type denial struct {
	callID string
	name   string
	reason string
}

// adjudicate asks the Approver about each gated call and rebuilds the assistant
// message: approved calls are kept (with edited args if any), denied calls are
// dropped and recorded. final is left unmodified.
func (h *hitl) adjudicate(ctx context.Context, final *llm.Response) (*llm.Response, []denial, error) {
	parts := final.Message.Parts
	newParts := make([]core.Part, 0, len(parts))
	var denials []denial
	for _, p := range parts {
		call, isCall := p.(core.ToolCall)
		if !isCall || !h.gate(call) {
			newParts = append(newParts, p)
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		d, err := h.approver(ctx, call)
		if err != nil {
			return nil, nil, err
		}
		switch {
		case d.Approve:
			if d.EditedArgs != nil {
				call.Args = d.EditedArgs
			}
			newParts = append(newParts, call)
		default:
			denials = append(denials, denial{callID: call.ID, name: call.Name, reason: d.Reason})
		}
	}
	rewritten := *final
	rewritten.Message = core.Message{Role: final.Message.Role, Parts: newParts}
	return &rewritten, denials, nil
}

func (h *hitl) stash(m core.Message) {
	h.mu.Lock()
	h.pending = append(h.pending, m)
	h.mu.Unlock()
}

func (h *hitl) drain() []core.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pending) == 0 {
		return nil
	}
	out := h.pending
	h.pending = nil
	return out
}

// denialNote renders denials as a user-role nudge, injected before the next
// model call when other calls in the same turn were approved.
func denialNote(ds []denial) core.Message {
	var b strings.Builder
	b.WriteString("[系统] 以下工具调用已被人工拒绝，请勿重试：")
	for i, d := range ds {
		if i > 0 {
			b.WriteString("；")
		}
		b.WriteString(d.name)
		if d.reason != "" {
			b.WriteString("（")
			b.WriteString(d.reason)
			b.WriteString("）")
		}
	}
	return core.Message{Role: core.RoleUser, Parts: []core.Part{core.Text{Text: b.String()}}}
}

// denialResults renders denials as tool results paired (by CallID) with the
// original tool calls, used to re-invoke the model when a whole turn is denied.
func denialResults(ds []denial) core.Message {
	parts := make([]core.Part, len(ds))
	for i, d := range ds {
		reason := d.reason
		if reason == "" {
			reason = "denied by human reviewer"
		}
		parts[i] = core.ToolResult{
			CallID:  d.callID,
			Name:    d.name,
			IsError: true,
			Content: []core.Part{core.Text{Text: "人工拒绝：" + reason}},
		}
	}
	return core.Message{Role: core.RoleTool, Parts: parts}
}

// RequireApprovalFor builds a Gate matching the named tools — the common case
// where only a few tools (delete, send, exec, ...) need human sign-off.
func RequireApprovalFor(names ...string) func(core.ToolCall) bool {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return func(c core.ToolCall) bool {
		_, ok := set[c.Name]
		return ok
	}
}

// ConsoleApprover builds an Approver that prompts on a text stream: 'a' to
// approve, 'e' to approve with edited JSON args, anything else to deny (with an
// optional reason). Reads are serialized, so it is safe under the concurrent
// tool calls of a single turn. Note: a blocking read does not observe ctx
// cancellation; it suits interactive CLIs, not unattended runs.
func ConsoleApprover(in io.Reader, out io.Writer) Approver {
	r := bufio.NewReader(in)
	var mu sync.Mutex
	return func(_ context.Context, call core.ToolCall) (Decision, error) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(out, "\n待审批 → %s  args=%s\n", call.Name, call.Args)
		fmt.Fprint(out, "[a]批准  [e]改参批准  [其他]拒绝 ? ")
		line, err := r.ReadString('\n')
		if err != nil {
			return Decision{}, err
		}
		switch strings.TrimSpace(line) {
		case "a", "approve", "y", "":
			return Approve(), nil
		case "e", "edit":
			fmt.Fprint(out, "新参数(JSON): ")
			args, err := r.ReadString('\n')
			if err != nil {
				return Decision{}, err
			}
			return ApproveWithArgs(json.RawMessage(strings.TrimSpace(args))), nil
		default:
			fmt.Fprint(out, "拒绝原因: ")
			reason, _ := r.ReadString('\n')
			return Deny(strings.TrimSpace(reason)), nil
		}
	}
}
