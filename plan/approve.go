package plan

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/jiujuan/goagent/middleware"
)

// Approver gates a plan and its steps for human-in-the-loop control. It reuses
// middleware.Decision so plan approval shares the vocabulary of the existing
// HITL middleware: Approve / ApproveWithArgs / Deny(reason).
//
// A nil Approver means "approve everything". ApprovePlan, when it denies, aborts
// the whole plan before any step runs. ApproveStep, when it denies a step, marks
// that step Skipped (its dependents then treat it as a skipped upstream).
type Approver interface {
	ApprovePlan(ctx context.Context, p *Plan) (middleware.Decision, error)
	ApproveStep(ctx context.Context, s *Step) (middleware.Decision, error)
}

// approveStep consults the approver for one step, defaulting to approval when no
// approver is configured or the step does not require it.
func approveStep(ctx context.Context, a Approver, s *Step) (middleware.Decision, error) {
	if a == nil || !s.NeedApproval {
		return middleware.Approve(), nil
	}
	return a.ApproveStep(ctx, s)
}

// approvePlan consults the approver for the whole plan, defaulting to approval.
func approvePlan(ctx context.Context, a Approver, p *Plan) (middleware.Decision, error) {
	if a == nil {
		return middleware.Approve(), nil
	}
	return a.ApprovePlan(ctx, p)
}

// Funcs adapts plain functions into an Approver. A nil field approves that level
// unconditionally, so you can gate only steps (or only the whole plan) without
// implementing the full interface.
type Funcs struct {
	Plan func(ctx context.Context, p *Plan) (middleware.Decision, error)
	Step func(ctx context.Context, s *Step) (middleware.Decision, error)
}

func (f Funcs) ApprovePlan(ctx context.Context, p *Plan) (middleware.Decision, error) {
	if f.Plan == nil {
		return middleware.Approve(), nil
	}
	return f.Plan(ctx, p)
}

func (f Funcs) ApproveStep(ctx context.Context, s *Step) (middleware.Decision, error) {
	if f.Step == nil {
		return middleware.Approve(), nil
	}
	return f.Step(ctx, s)
}

var _ Approver = Funcs{}

// ConsoleApprover builds an Approver that prompts on a text stream for each step
// that requires approval: 'a' to approve, anything else to deny with an optional
// reason. Reads are serialized, so it is safe under concurrently-scheduled
// steps. Like middleware.ConsoleApprover, a blocking read does not observe ctx
// cancellation — it suits interactive CLIs, not unattended runs.
func ConsoleApprover(in io.Reader, out io.Writer) Approver {
	r := bufio.NewReader(in)
	var mu sync.Mutex
	return Funcs{
		Step: func(_ context.Context, s *Step) (middleware.Decision, error) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Fprintf(out, "\n待审批步骤 → %s（%s）\n", s.Name, s.Description)
			fmt.Fprint(out, "[a]批准  [其他]拒绝 ? ")
			line, err := r.ReadString('\n')
			if err != nil {
				return middleware.Decision{}, err
			}
			switch strings.TrimSpace(line) {
			case "a", "approve", "y", "":
				return middleware.Approve(), nil
			default:
				fmt.Fprint(out, "拒绝原因: ")
				reason, _ := r.ReadString('\n')
				return middleware.Deny(strings.TrimSpace(reason)), nil
			}
		},
	}
}
