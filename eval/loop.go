package eval

import (
	"fmt"

	"github.com/jiujuan/goagent/agent"
	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// loop.go is closed loop A — online self-correction, expressed as middleware over
// an agent.Loop. Gate scores the worker's final answer: pass → Escalate (breaks
// the enclosing agent.Loop); fail → steer the critique into the next round and
// Continue. ToolGuard scores each tool result and, on failure, rewrites it to an
// error so the model self-corrects on the next turn.
//
// This reuses the refine pattern (examples/refine) and changes no AgentLoop
// control flow:
//
//	worker, _ := agent.New(
//	    agent.WithModel(m),
//	    agent.WithInstruction("…若上文有评审意见就据此改进…"),
//	    agent.WithMiddleware(eval.Gate(eval.Rubric(judge, "是否完整准确"), 0.8)),
//	)
//	answer, _ := agent.Loop("refine", 3, worker).Run(ctx, task)

// Gate turns a Scorer into an Escalate gate over the worker's final answer.
// Attach it to the worker and wrap the worker in agent.Loop(name, maxRounds,
// worker). The scorer should be reference-free (Rubric, Faithfulness, …), since
// no gold answer exists at runtime.
func Gate(scorer Scorer, threshold float64) agent.Middleware {
	return &gate{scorer: scorer, threshold: threshold}
}

type gate struct {
	agent.BaseMiddleware
	scorer    Scorer
	threshold float64
}

// AfterModel judges only the final answer (a model reply with no tool calls);
// replies that request tools pass through untouched so their tools run.
func (g *gate) AfterModel(lc *agent.LoopContext, resp *llm.Response) (core.Directive, error) {
	if len(resp.Message.ToolCalls()) > 0 {
		return core.Directive{}, nil // not a final answer yet
	}
	answer := resp.Message.Text()
	sc, err := g.scorer.Score(lc, Sample{Input: firstUserText(lc.History), Output: answer})
	if err != nil {
		return core.Directive{}, err
	}
	if sc.Value >= g.threshold {
		// Accept: Escalate breaks the enclosing agent.Loop with this answer.
		return core.Directive{Kind: core.Escalate, Reason: "eval gate passed: " + sc.Reason}, nil
	}
	// Reject: steer the critique so the next loop round sees it, then Continue.
	// The steering queue lives on the shared RunContext, so it survives into the
	// next round (where PrepareTurn drains it) even though State.Messages is
	// rewritten between rounds.
	lc.Steer(core.UserText(fmt.Sprintf(
		"评审意见（评分 %.2f，未达标 %.2f）。请据此改进你的回答：%s",
		sc.Value, g.threshold, sc.Reason)))
	return core.Directive{}, nil // Continue → loopRunner runs another round
}

// ToolGuard scores each successful tool result; a failing result is rewritten to
// IsError with the critique appended, so the model retries on the next turn.
// Pair it with an output-shaped scorer (JSONSchema, a faithfulness judge, …) —
// not NoToolError, which only restates the existing IsError flag.
func ToolGuard(check Scorer) agent.Middleware {
	return &toolGuard{check: check}
}

type toolGuard struct {
	agent.BaseMiddleware
	check Scorer
}

// AfterTool validates a non-error result; on failure it flips IsError and
// appends the reason so the model sees why and self-corrects. The loop now
// stores the result after AfterTool, so this rewrite reaches the model history.
func (t *toolGuard) AfterTool(lc *agent.LoopContext, tr *core.ToolResult) (core.Directive, error) {
	if tr.IsError {
		return core.Directive{}, nil // already failed; nothing to add
	}
	sample := Sample{
		Output: partsText(tr.Content),
		Tool:   &ToolEpisode{Result: *tr},
	}
	sc, err := t.check.Score(lc, sample)
	if err != nil {
		return core.Directive{}, err
	}
	if !sc.Passed {
		tr.IsError = true
		tr.Content = append(tr.Content, core.Text{
			Text: fmt.Sprintf("\n[评估未通过] %s（请修正后重试）", sc.Reason),
		})
	}
	return core.Directive{}, nil
}

// firstUserText returns the text of the first user message in a history, the
// originating request a runtime judge scores against.
func firstUserText(history []core.Message) string {
	for _, m := range history {
		if m.Role == core.RoleUser {
			return m.Text()
		}
	}
	return ""
}
