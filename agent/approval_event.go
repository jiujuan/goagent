package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/jiujuan/goagent/checkpoint"
	"github.com/jiujuan/goagent/core"
)

// ApprovalDecision is the structured, durable audit record for one pending
// tool call. It is an alias so callers use the same schema in agent APIs and
// on the event stream.
type ApprovalDecision = core.ApprovalDecision

// RecordApproval validates and durably records a human decision for the latest
// paused checkpoint of threadID. DecisionID and PolicyVersion are required so a
// caller can retry safely and later identify the policy that authorized it.
func (a *Agent) RecordApproval(ctx context.Context, threadID string, decision ApprovalDecision) (checkpoint.DecisionReceipt, error) {
	if err := core.CheckThreadID(threadID); err != nil {
		return checkpoint.DecisionReceipt{}, err
	}
	durable, ok := a.store.(checkpoint.Durable)
	if !ok {
		return checkpoint.DecisionReceipt{}, checkpoint.ErrDurableUnavailable
	}
	if decision.DecisionID == "" {
		return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: approval decision ID is required")
	}
	if decision.PolicyVersion == "" {
		return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: approval policy version is required")
	}
	if decision.CallID == "" {
		return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: approval call ID is required")
	}

	cp, err := a.store.Latest(ctx, threadID)
	if err != nil {
		return checkpoint.DecisionReceipt{}, err
	}
	if cp == nil || cp.Pending == nil || len(cp.Pending.Pending) == 0 {
		return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: thread %q has no pending tool approval", threadID)
	}
	if err := validatePause(cp); err != nil {
		return checkpoint.DecisionReceipt{}, err
	}

	call, err := pendingCall(cp.Pending.Pending, decision.CallID)
	if err != nil {
		return checkpoint.DecisionReceipt{}, err
	}
	pauseID := checkpoint.EffectivePauseID(cp)
	parameterDigest := approvalParameterDigest(call.Args)
	if decision.PauseID != "" && decision.PauseID != pauseID {
		return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: approval pause ID does not match latest checkpoint")
	}
	if decision.Tool != "" && decision.Tool != call.Name {
		return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: approval tool %q does not match pending tool %q", decision.Tool, call.Name)
	}
	if decision.ParameterDigest != "" && decision.ParameterDigest != parameterDigest {
		return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: approval parameter digest does not match pending call")
	}

	decision.PauseID = pauseID
	decision.Tool = call.Name
	decision.ParameterDigest = parameterDigest
	expectedDigest := approvalDecisionDigest(decision)
	if decision.Digest != "" && decision.Digest != expectedDigest {
		return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: approval digest does not match decision content")
	}
	decision.Digest = expectedDigest

	existing, err := durable.Decisions(ctx, threadID, pauseID)
	if err != nil {
		return checkpoint.DecisionReceipt{}, err
	}
	for _, prior := range existing {
		if prior.CallID == decision.CallID && prior.DecisionID != decision.DecisionID {
			return checkpoint.DecisionReceipt{}, fmt.Errorf("agent: pending call %q already has decision %q: %w", decision.CallID, prior.DecisionID, checkpoint.ErrDecisionConflict)
		}
	}
	return durable.RecordDecision(ctx, threadID, decision)
}

// RecordApproval records a structured decision against this run's thread and
// publishes its audit event immediately on the run topic.
func (r *Run) RecordApproval(ctx context.Context, decision ApprovalDecision) (checkpoint.DecisionReceipt, error) {
	receipt, err := r.agent.RecordApproval(ctx, r.ThreadID, decision)
	if err != nil {
		return checkpoint.DecisionReceipt{}, err
	}
	r.bus.Publish(r.topic, core.ApprovalDecided{Decision: receipt.Decision, Duplicate: receipt.Duplicate})
	return receipt, nil
}

func pendingCall(calls []core.ToolCall, callID string) (core.ToolCall, error) {
	var found core.ToolCall
	count := 0
	for _, call := range calls {
		if call.ID == callID {
			found = call
			count++
		}
	}
	switch count {
	case 1:
		return found, nil
	case 0:
		return core.ToolCall{}, fmt.Errorf("agent: call %q is not pending", callID)
	default:
		return core.ToolCall{}, fmt.Errorf("agent: call %q is ambiguous in pending approval", callID)
	}
}

func approvalsFromDurable(cp *checkpoint.Checkpoint, decisions []core.ApprovalDecision) ([]Approval, error) {
	if len(decisions) == 0 {
		return nil, nil
	}
	if cp == nil || cp.Pending == nil {
		return nil, fmt.Errorf("agent: durable approvals exist without pending tool calls")
	}
	pauseID := checkpoint.EffectivePauseID(cp)
	out := make([]Approval, 0, len(decisions))
	seenCalls := make(map[string]struct{}, len(decisions))
	for _, decision := range decisions {
		call, err := pendingCall(cp.Pending.Pending, decision.CallID)
		if err != nil {
			return nil, fmt.Errorf("agent: durable approval %q: %w", decision.DecisionID, err)
		}
		if decision.DecisionID == "" || decision.PolicyVersion == "" || decision.PauseID != pauseID || decision.Tool != call.Name || decision.ParameterDigest != approvalParameterDigest(call.Args) || decision.Digest != approvalDecisionDigest(decision) {
			return nil, fmt.Errorf("agent: durable approval %q does not match pending call", decision.DecisionID)
		}
		if _, duplicate := seenCalls[decision.CallID]; duplicate {
			return nil, fmt.Errorf("agent: multiple durable approvals exist for pending call %q", decision.CallID)
		}
		seenCalls[decision.CallID] = struct{}{}
		out = append(out, Approval{CallID: decision.CallID, Approve: decision.Approved, Reason: decision.Reason})
	}
	return out, nil
}

func approvalParameterDigest(args []byte) string {
	sum := sha256.Sum256(args)
	return hex.EncodeToString(sum[:])
}

func approvalDecisionDigest(decision core.ApprovalDecision) string {
	payload := struct {
		DecisionID      string `json:"decision_id"`
		PauseID         string `json:"pause_id"`
		CallID          string `json:"call_id"`
		Tool            string `json:"tool"`
		ParameterDigest string `json:"parameter_digest"`
		PolicyVersion   string `json:"policy_version"`
		Approved        bool   `json:"approved"`
		Reason          string `json:"reason"`
	}{
		DecisionID:      decision.DecisionID,
		PauseID:         decision.PauseID,
		CallID:          decision.CallID,
		Tool:            decision.Tool,
		ParameterDigest: decision.ParameterDigest,
		PolicyVersion:   decision.PolicyVersion,
		Approved:        decision.Approved,
		Reason:          decision.Reason,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("agent: marshal approval decision digest: %v", err))
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
