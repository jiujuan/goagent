package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/llm"
)

// judge.go holds the LLM-as-Judge scorers — the semantic tier. A judge runs a
// (typically cheaper) llm.Model with a rubric prompt and parses a structured
// {"score": n, "reason": "..."} reply, which is normalized to [0,1] against a
// configurable ceiling. Judges are deterministic under a mock model, so every
// example and test runs offline.

// judgeConfig holds tunables shared by judge constructors.
type judgeConfig struct {
	threshold float64 // pass cutoff on the normalized score
	maxScore  float64 // raw rubric ceiling (e.g. 5 for a 1..5 rubric)
	retries   int     // re-ask on unparseable judge output
}

func defaultJudgeConfig() judgeConfig { return judgeConfig{threshold: 0.7, maxScore: 5, retries: 2} }

// JudgeOpt tunes a judge scorer.
type JudgeOpt func(*judgeConfig)

// WithThreshold sets the normalized pass cutoff.
func WithThreshold(t float64) JudgeOpt { return func(c *judgeConfig) { c.threshold = t } }

// WithMaxScore sets the raw rubric ceiling used to normalize the judge's score.
func WithMaxScore(m float64) JudgeOpt { return func(c *judgeConfig) { c.maxScore = m } }

// WithRetries sets how many times to re-ask the judge on unparseable output.
func WithRetries(n int) JudgeOpt { return func(c *judgeConfig) { c.retries = n } }

func applyJudgeOpts(opts []JudgeOpt) judgeConfig {
	c := defaultJudgeConfig()
	for _, o := range opts {
		o(&c)
	}
	return c
}

// Rubric grades Output against a single natural-language criterion.
func Rubric(judge llm.Model, criterion string, opts ...JudgeOpt) Scorer {
	cfg := applyJudgeOpts(opts)
	return newScorer("rubric", func(ctx context.Context, s Sample) (Score, error) {
		user := fmt.Sprintf("评分标准：%s\n\n用户问题：\n%s\n\n被评回答：\n%s",
			criterion, s.Input, s.Output)
		return runJudge(ctx, judge, cfg, "rubric", gradeSystem(cfg.maxScore), user)
	})
}

// Reference grades Output against the gold Reference answer (correctness and
// completeness relative to a known-good answer).
func Reference(judge llm.Model, opts ...JudgeOpt) Scorer {
	cfg := applyJudgeOpts(opts)
	return newScorer("reference", func(ctx context.Context, s Sample) (Score, error) {
		if s.Reference == "" {
			return Score{Name: "reference"}, fmt.Errorf("eval: Reference judge needs Sample.Reference")
		}
		user := fmt.Sprintf("评分标准：被评回答在事实正确性与完整性上，与参考答案的一致程度。\n\n用户问题：\n%s\n\n参考答案：\n%s\n\n被评回答：\n%s",
			s.Input, s.Reference, s.Output)
		return runJudge(ctx, judge, cfg, "reference", gradeSystem(cfg.maxScore), user)
	})
}

// Faithfulness grades whether Output is supported by the grounding material
// (tool outputs in the trajectory, else the reference/context) — anti-hallucination.
func Faithfulness(judge llm.Model, opts ...JudgeOpt) Scorer {
	cfg := applyJudgeOpts(opts)
	return newScorer("faithfulness", func(ctx context.Context, s Sample) (Score, error) {
		grounding := groundingText(s)
		if strings.TrimSpace(grounding) == "" {
			return Score{Name: "faithfulness"}, fmt.Errorf("eval: Faithfulness needs grounding (Sample.Traj tool outputs, or Reference)")
		}
		user := fmt.Sprintf("评分标准：被评回答是否完全由「依据材料」支撑，有无臆造/无中生有。分数越高表示越忠实于材料。\n\n依据材料：\n%s\n\n被评回答：\n%s",
			grounding, s.Output)
		return runJudge(ctx, judge, cfg, "faithfulness", gradeSystem(cfg.maxScore), user)
	})
}

// TrajectoryJudge grades a whole agent trajectory against a rubric (tool choice,
// efficiency, no redundant loops). Requires Sample.Traj.
func TrajectoryJudge(judge llm.Model, rubric string, opts ...JudgeOpt) Scorer {
	cfg := applyJudgeOpts(opts)
	return newScorer("trajectory_judge", func(ctx context.Context, s Sample) (Score, error) {
		if s.Traj == nil {
			return Score{Name: "trajectory_judge"}, fmt.Errorf("eval: TrajectoryJudge needs Sample.Traj")
		}
		user := fmt.Sprintf("评分标准：%s\n\n以下是智能体的完整执行轨迹（思考/工具调用/观察/回答）：\n%s",
			rubric, renderTrajectory(s.Traj))
		return runJudge(ctx, judge, cfg, "trajectory_judge", gradeSystem(cfg.maxScore), user)
	})
}

// Pairwise compares two answers head-to-head, running both orders to cancel
// position bias. Score.Value is A's win share in [0,1] (0.5 = tie).
func Pairwise(judge llm.Model, opts ...JudgeOpt) PairScorer {
	return &pairwise{judge: judge, cfg: applyJudgeOpts(opts)}
}

type pairwise struct {
	judge llm.Model
	cfg   judgeConfig
}

func (p *pairwise) Name() string { return "pairwise" }

func (p *pairwise) Compare(ctx context.Context, a, b Sample) (Score, error) {
	// Run both orders: first (A=a, B=b), then swapped (A=b, B=a). A point for `a`
	// when it is the chosen side in either run; tie splits the point.
	r1, reason1, err := p.ask(ctx, a.Input, a.Output, b.Output)
	if err != nil {
		return Score{Name: "pairwise"}, err
	}
	r2, reason2, err := p.ask(ctx, a.Input, b.Output, a.Output)
	if err != nil {
		return Score{Name: "pairwise"}, err
	}
	aWins := winShare(r1, "A") + winShare(r2, "B") // r2 is swapped: B is `a`
	value := aWins / 2.0
	return Score{
		Name:   "pairwise",
		Value:  value,
		Passed: value > 0.5,
		Reason: fmt.Sprintf("正序: %s | 反序: %s", reason1, reason2),
	}, nil
}

// ask runs one pairwise comparison and returns the winner ("A"/"B"/"tie").
func (p *pairwise) ask(ctx context.Context, input, ansA, ansB string) (string, string, error) {
	system := "你是严格的对比评估员。比较两个回答谁更好，只输出 JSON：{\"winner\": \"A\"|\"B\"|\"tie\", \"reason\": \"<简短理由>\"}。不要输出任何其他文字。"
	user := fmt.Sprintf("用户问题：\n%s\n\n回答 A：\n%s\n\n回答 B：\n%s", input, ansA, ansB)
	for attempt := 0; attempt <= p.cfg.retries; attempt++ {
		out, err := callModel(ctx, p.judge, system, user)
		if err != nil {
			return "", "", err
		}
		var v struct {
			Winner string `json:"winner"`
			Reason string `json:"reason"`
		}
		if obj := extractJSON(out); obj != "" {
			if json.Unmarshal([]byte(obj), &v) == nil && v.Winner != "" {
				return strings.ToUpper(strings.TrimSpace(v.Winner)), v.Reason, nil
			}
		}
	}
	return "", "", fmt.Errorf("eval: pairwise judge returned unparseable output")
}

func winShare(winner, side string) float64 {
	switch winner {
	case side:
		return 1
	case "TIE", "tie", "Tie":
		return 0.5
	default:
		return 0
	}
}

// --- judge internals --------------------------------------------------------

type judgeVerdict struct {
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
}

// gradeSystem builds the shared judge system prompt for a 1..maxScore rubric.
func gradeSystem(maxScore float64) string {
	return fmt.Sprintf("你是严格、公正的评估员。请依据给定标准为「被评回答」打分（%d 为最差，%d 为最好），"+
		"只输出 JSON：{\"score\": <%d-%d 的整数>, \"reason\": \"<一句话理由>\"}。不要输出任何其他文字。",
		1, int(maxScore), 1, int(maxScore))
}

// runJudge calls the judge model, parses {score,reason}, normalizes and applies
// the threshold; it re-asks up to cfg.retries on unparseable output.
func runJudge(ctx context.Context, m llm.Model, cfg judgeConfig, name, system, user string) (Score, error) {
	var lastErr error
	for attempt := 0; attempt <= cfg.retries; attempt++ {
		out, err := callModel(ctx, m, system, user)
		if err != nil {
			return Score{Name: name}, err
		}
		obj := extractJSON(out)
		if obj == "" {
			lastErr = fmt.Errorf("no JSON object in output: %q", truncate(out, 120))
			continue
		}
		var v judgeVerdict
		if err := json.Unmarshal([]byte(obj), &v); err != nil {
			lastErr = err
			continue
		}
		max := cfg.maxScore
		if max <= 0 {
			max = 1
		}
		value := clamp01(v.Score / max)
		return Score{Name: name, Value: value, Passed: value >= cfg.threshold, Reason: v.Reason}, nil
	}
	return Score{Name: name}, fmt.Errorf("eval: judge %q produced unparseable output: %w", name, lastErr)
}

// callModel runs one non-streaming model call and returns the final text.
func callModel(ctx context.Context, m llm.Model, system, user string) (string, error) {
	req := &llm.Request{System: system, Messages: []core.Message{core.UserText(user)}}
	var final core.Message
	for resp, err := range m.Generate(ctx, req) {
		if err != nil {
			return "", err
		}
		if resp.Partial {
			continue
		}
		final = resp.Message
	}
	return final.Text(), nil
}

// extractJSON returns the first balanced, valid JSON object found in text,
// tolerating ```json fences, surrounding prose, and stray braces in prose. It
// tries each '{' as a start candidate and returns the first that yields a
// balanced object that also parses as JSON. Empty if none.
func extractJSON(text string) string {
	for i := 0; i < len(text); i++ {
		if text[i] != '{' {
			continue
		}
		if obj := balancedObject(text[i:]); obj != "" && json.Valid([]byte(obj)) {
			return obj
		}
	}
	return ""
}

// balancedObject returns the substring from s[0] (which must be '{') up to its
// matching '}', honoring JSON string quoting and escapes. Empty if unbalanced.
func balancedObject(s string) string {
	depth := 0
	inStr := false
	esc := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case esc:
			esc = false
		case c == '\\' && inStr:
			esc = true
		case c == '"':
			inStr = !inStr
		case inStr:
			// skip content inside strings
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth == 0 {
				return s[:i+1]
			}
		}
	}
	return ""
}

// groundingText gathers the material a faithfulness judge checks against: tool
// outputs from the trajectory if present, else the reference answer.
func groundingText(s Sample) string {
	if s.Traj != nil && len(s.Traj.Tools) > 0 {
		var b strings.Builder
		for _, ep := range s.Traj.Tools {
			fmt.Fprintf(&b, "[工具 %s] %s\n", ep.Result.Name, partsText(ep.Result.Content))
		}
		return b.String()
	}
	return s.Reference
}

// renderTrajectory renders a trajectory as a compact transcript for a judge.
func renderTrajectory(tr *Trajectory) string {
	var b strings.Builder
	for _, m := range tr.Messages {
		for _, p := range m.Parts {
			switch v := p.(type) {
			case core.Text:
				if txt := strings.TrimSpace(v.Text); txt != "" {
					fmt.Fprintf(&b, "%s: %s\n", m.Role, txt)
				}
			case core.ToolCall:
				fmt.Fprintf(&b, "%s → 调用 %s(%s)\n", m.Role, v.Name, string(v.Args))
			case core.ToolResult:
				fmt.Fprintf(&b, "tool ← %s: %s\n", v.Name, partsText(v.Content))
			}
		}
	}
	fmt.Fprintf(&b, "[统计] 步数=%d, tokens=%d\n", tr.Steps, tr.Usage.InputTokens+tr.Usage.OutputTokens)
	return b.String()
}

// partsText concatenates the text parts of a slice of parts.
func partsText(parts []core.Part) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(core.Text); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
