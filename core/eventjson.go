package core

import (
	"encoding/json"
	"errors"
	"fmt"
)

// MarshalEvent / UnmarshalEvent give the sealed Event union a tagged-JSON
// encoding, so events can cross a process boundary (the Redis progress bus) or
// be written to a trace log. Each variant is wrapped as {"type": "...", ...}.
//
// Message/Part/ToolCall/ToolResult fields reuse Message's own JSON (which knows
// how to envelope sealed Part types) by carrying them inside a Message. Errors
// are reduced to their string (a transported error is informational, not the
// original value).

type eventWire struct {
	Type     string            `json:"type"`
	RunID    string            `json:"run_id,omitempty"`
	ThreadID string            `json:"thread_id,omitempty"`
	Step     int               `json:"step,omitempty"`
	Message  *Message          `json:"message,omitempty"`
	Usage    *Usage            `json:"usage,omitempty"`
	Err      string            `json:"err,omitempty"`
	Pending  []ApprovalRequest `json:"pending,omitempty"`
	Progress *ProgressInfo     `json:"progress,omitempty"`
	NodeID   string            `json:"node_id,omitempty"`
	Status   string            `json:"status,omitempty"`
	CallID   string            `json:"call_id,omitempty"`
}

// MarshalEvent encodes an Event to tagged JSON.
func MarshalEvent(ev Event) ([]byte, error) {
	var w eventWire
	switch e := ev.(type) {
	case RunStarted:
		w.Type, w.RunID, w.ThreadID = "run_started", e.RunID, e.ThreadID
	case RunDone:
		m := e.Result.Message
		w.Type, w.Message = "run_done", &m
	case RunFailed:
		w.Type = "run_failed"
		if e.Err != nil {
			w.Err = e.Err.Error()
		}
	case TurnStarted:
		w.Type, w.Step = "turn_started", e.Step
	case TurnDone:
		w.Type, w.Step = "turn_done", e.Step
	case MessageDelta:
		m := e.Delta
		w.Type, w.Message = "message_delta", &m
	case MessageDone:
		m := e.Message
		w.Type, w.Message, w.Usage = "message_done", &m, e.Usage
	case ToolStarted:
		w.Type = "tool_started"
		w.Message = &Message{Role: RoleAssistant, Parts: []Part{e.Call}}
	case ToolUpdate:
		w.Type, w.CallID = "tool_update", e.CallID
		w.Message = &Message{Role: RoleTool, Parts: []Part{e.Partial}}
	case ToolDone:
		w.Type = "tool_done"
		w.Message = &Message{Role: RoleTool, Parts: []Part{e.Result}}
	case Interrupted:
		w.Type, w.Pending = "interrupted", e.Pending
	case Progress:
		p := e.Job
		w.Type, w.Progress = "progress", &p
	case PlanNodeStarted:
		w.Type, w.NodeID = "plan_node_started", e.NodeID
	case PlanNodeDone:
		w.Type, w.NodeID, w.Status = "plan_node_done", e.NodeID, e.Status
		if e.Err != nil {
			w.Err = e.Err.Error()
		}
	default:
		return nil, fmt.Errorf("core: cannot marshal event of type %T", ev)
	}
	return json.Marshal(w)
}

// UnmarshalEvent decodes a tagged-JSON event produced by MarshalEvent.
func UnmarshalEvent(data []byte) (Event, error) {
	var w eventWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	switch w.Type {
	case "run_started":
		return RunStarted{RunID: w.RunID, ThreadID: w.ThreadID}, nil
	case "run_done":
		return RunDone{Result: Result{Message: deref(w.Message)}}, nil
	case "run_failed":
		return RunFailed{Err: toErr(w.Err)}, nil
	case "turn_started":
		return TurnStarted{Step: w.Step}, nil
	case "turn_done":
		return TurnDone{Step: w.Step}, nil
	case "message_delta":
		return MessageDelta{Delta: deref(w.Message)}, nil
	case "message_done":
		return MessageDone{Message: deref(w.Message), Usage: w.Usage}, nil
	case "tool_started":
		return ToolStarted{Call: firstPartOf[ToolCall](w.Message)}, nil
	case "tool_update":
		return ToolUpdate{CallID: w.CallID, Partial: firstPart(w.Message)}, nil
	case "tool_done":
		return ToolDone{Result: firstPartOf[ToolResult](w.Message)}, nil
	case "interrupted":
		return Interrupted{Pending: w.Pending}, nil
	case "progress":
		var p ProgressInfo
		if w.Progress != nil {
			p = *w.Progress
		}
		return Progress{Job: p}, nil
	case "plan_node_started":
		return PlanNodeStarted{NodeID: w.NodeID}, nil
	case "plan_node_done":
		return PlanNodeDone{NodeID: w.NodeID, Status: w.Status, Err: toErr(w.Err)}, nil
	default:
		return nil, fmt.Errorf("core: unknown event type %q", w.Type)
	}
}

func deref(m *Message) Message {
	if m == nil {
		return Message{}
	}
	return *m
}

func toErr(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
}

func firstPart(m *Message) Part {
	if m == nil || len(m.Parts) == 0 {
		return nil
	}
	return m.Parts[0]
}

// firstPartOf returns the first part of the wrapped message as type T (zero if
// absent), recovering a ToolCall or ToolResult carried for transport.
func firstPartOf[T Part](m *Message) T {
	var zero T
	if m == nil {
		return zero
	}
	for _, p := range m.Parts {
		if v, ok := p.(T); ok {
			return v
		}
	}
	return zero
}
