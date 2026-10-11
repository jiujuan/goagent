package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jiujuan/goagent/core"
)

var (
	ErrRevisionConflict        = errors.New("checkpoint: expected revision does not match head")
	ErrResumeRequired          = errors.New("checkpoint: paused thread requires resume")
	ErrCoordinationUnavailable = errors.New("checkpoint: thread coordination unavailable")
	ErrCheckpointConflict      = errors.New("checkpoint: checkpoint ID conflicts with existing content")
)

type BeginRunRequest struct {
	ThreadID string
	Owner    string
	Lease    time.Duration
}

type BeginRunResult struct {
	Claim      Claim
	Checkpoint *Checkpoint
}

// ThreadCoordinator is the optional durable capability used to serialize
// ordinary writers. Durable remains unchanged for third-party implementations.
type ThreadCoordinator interface {
	Durable
	BeginRun(context.Context, BeginRunRequest) (BeginRunResult, error)
	RenewClaim(context.Context, Claim, time.Duration) (Claim, error)
}

type commitDigestCheckpoint struct {
	ID           string                   `json:"id"`
	ThreadID     string                   `json:"thread_id"`
	ParentID     string                   `json:"parent_id,omitempty"`
	Step         int                      `json:"step"`
	State        commitDigestState        `json:"state"`
	Pending      *commitDigestPendingHITL `json:"pending,omitempty"`
	Pause        *Pause                   `json:"pause,omitempty"`
	FileSnapshot map[string][]byte        `json:"file_snapshot"`
}

type commitDigestState struct {
	Messages []commitDigestMessage `json:"messages"`
	Todos    []core.Todo           `json:"todos"`
	KV       map[string]any        `json:"kv"`
}

type commitDigestMessage struct {
	Role  core.Role          `json:"role"`
	Parts []commitDigestPart `json:"parts"`
}

type commitDigestPart struct {
	Type       string             `json:"type"`
	Text       string             `json:"text,omitempty"`
	MIME       string             `json:"mime,omitempty"`
	Data       []byte             `json:"data"`
	URL        string             `json:"url,omitempty"`
	DurationMs int                `json:"duration_ms,omitempty"`
	ID         string             `json:"id,omitempty"`
	Name       string             `json:"name,omitempty"`
	Args       []byte             `json:"args"`
	CallID     string             `json:"call_id,omitempty"`
	IsError    bool               `json:"is_error,omitempty"`
	Content    []commitDigestPart `json:"content"`
}

type commitDigestToolCall struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Args []byte `json:"args"`
}

type commitDigestPendingHITL struct {
	Step    int                    `json:"step"`
	Pending []commitDigestToolCall `json:"pending"`
}

type commitDigestToolExecution struct {
	InvocationID   string               `json:"invocation_id"`
	CallID         string               `json:"call_id"`
	Tool           string               `json:"tool"`
	IdempotencyKey string               `json:"idempotency_key,omitempty"`
	Attempt        int                  `json:"attempt"`
	State          ToolExecutionState   `json:"state"`
	Result         *commitDigestMessage `json:"result,omitempty"`
}

type commitDigestClaim struct {
	ThreadID     string `json:"thread_id"`
	Revision     uint64 `json:"revision"`
	PauseID      string `json:"pause_id"`
	Owner        string `json:"owner"`
	FencingToken uint64 `json:"fencing_token"`
}

type commitDigestPayload struct {
	Checkpoint     commitDigestCheckpoint `json:"checkpoint"`
	AckMessageIDs  []string               `json:"ack_message_ids"`
	Claim          *commitDigestClaim     `json:"claim,omitempty"`
	ToolExecutions []json.RawMessage      `json:"tool_executions"`
}

// commitRequestDigest identifies the content made visible by Commit. Store
// assigned revisions and retry-only controls are deliberately excluded.
func commitRequestDigest(cp *Checkpoint, ackMessageIDs []string, claim *Claim, executions []ToolExecution) (string, error) {
	acks := append([]string(nil), ackMessageIDs...)
	sort.Strings(acks)
	acks = compactStrings(acks)

	digestExecutions := make(map[string]commitDigestToolExecution, len(executions))
	for _, execution := range executions {
		digestExecution, err := makeCommitDigestToolExecution(execution)
		if err != nil {
			return "", err
		}
		// Commit publishes executions into an InvocationID-keyed ledger, where a
		// later entry in the same request replaces an earlier one. Digest that
		// final visible state so retry equivalence matches the commit semantics.
		digestExecutions[execution.InvocationID] = digestExecution
	}
	encodedExecutions := make([]json.RawMessage, 0, len(digestExecutions))
	for _, execution := range digestExecutions {
		encoded, err := json.Marshal(execution)
		if err != nil {
			return "", err
		}
		encodedExecutions = append(encodedExecutions, encoded)
	}
	sort.Slice(encodedExecutions, func(i, j int) bool {
		return string(encodedExecutions[i]) < string(encodedExecutions[j])
	})

	state, err := makeCommitDigestState(cp.State)
	if err != nil {
		return "", err
	}
	payload := commitDigestPayload{
		Checkpoint: commitDigestCheckpoint{
			ID:           cp.ID,
			ThreadID:     cp.ThreadID,
			ParentID:     cp.ParentID,
			Step:         cp.Step,
			State:        state,
			Pending:      makeCommitDigestPending(cp.Pending),
			Pause:        cp.Pause,
			FileSnapshot: cp.FileSnapshot,
		},
		AckMessageIDs:  acks,
		ToolExecutions: encodedExecutions,
	}
	if claim != nil {
		payload.Claim = &commitDigestClaim{
			ThreadID:     claim.ThreadID,
			Revision:     claim.Revision,
			PauseID:      claim.PauseID,
			Owner:        claim.Owner,
			FencingToken: claim.FencingToken,
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func makeCommitDigestState(state core.State) (commitDigestState, error) {
	out := commitDigestState{Todos: state.Todos, KV: state.KV}
	if state.Messages == nil {
		return out, nil
	}
	out.Messages = make([]commitDigestMessage, len(state.Messages))
	for i, message := range state.Messages {
		converted, err := makeCommitDigestMessage(message)
		if err != nil {
			return commitDigestState{}, err
		}
		out.Messages[i] = converted
	}
	return out, nil
}

func makeCommitDigestMessage(message core.Message) (commitDigestMessage, error) {
	out := commitDigestMessage{Role: message.Role}
	if message.Parts == nil {
		return out, nil
	}
	out.Parts = make([]commitDigestPart, len(message.Parts))
	for i, part := range message.Parts {
		converted, err := makeCommitDigestPart(part)
		if err != nil {
			return commitDigestMessage{}, err
		}
		out.Parts[i] = converted
	}
	return out, nil
}

func makeCommitDigestPart(part core.Part) (commitDigestPart, error) {
	switch value := part.(type) {
	case core.Text:
		return commitDigestPart{Type: "text", Text: value.Text}, nil
	case core.Thinking:
		return commitDigestPart{Type: "thinking", Text: value.Text}, nil
	case core.Image:
		return commitDigestPart{Type: "image", MIME: value.MIME, Data: value.Data, URL: value.URL}, nil
	case core.Video:
		return commitDigestPart{Type: "video", MIME: value.MIME, Data: value.Data, URL: value.URL, DurationMs: value.DurationMs}, nil
	case core.ToolCall:
		return commitDigestPart{Type: "tool_call", ID: value.ID, Name: value.Name, Args: []byte(value.Args)}, nil
	case core.ToolResult:
		out := commitDigestPart{Type: "tool_result", CallID: value.CallID, Name: value.Name, IsError: value.IsError}
		if value.Content == nil {
			return out, nil
		}
		out.Content = make([]commitDigestPart, len(value.Content))
		for i, content := range value.Content {
			converted, err := makeCommitDigestPart(content)
			if err != nil {
				return commitDigestPart{}, err
			}
			out.Content[i] = converted
		}
		return out, nil
	default:
		return commitDigestPart{}, fmt.Errorf("checkpoint: unsupported message part %T", part)
	}
}

func makeCommitDigestPending(pending *PendingHITL) *commitDigestPendingHITL {
	if pending == nil {
		return nil
	}
	out := &commitDigestPendingHITL{Step: pending.Step}
	if pending.Pending == nil {
		return out
	}
	out.Pending = make([]commitDigestToolCall, len(pending.Pending))
	for i, call := range pending.Pending {
		out.Pending[i] = commitDigestToolCall{ID: call.ID, Name: call.Name, Args: []byte(call.Args)}
	}
	return out
}

func makeCommitDigestToolExecution(execution ToolExecution) (commitDigestToolExecution, error) {
	out := commitDigestToolExecution{
		InvocationID: execution.InvocationID, CallID: execution.CallID, Tool: execution.Tool,
		IdempotencyKey: execution.IdempotencyKey, Attempt: execution.Attempt, State: execution.State,
	}
	if execution.Result == nil {
		return out, nil
	}
	result, err := makeCommitDigestMessage(*execution.Result)
	if err != nil {
		return commitDigestToolExecution{}, err
	}
	out.Result = &result
	return out, nil
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}
