package eval

import (
	"context"
	"strings"
	"testing"

	"github.com/jiujuan/goagent/llm"
	"github.com/jiujuan/goagent/llm/mock"
)

// scriptedJudge returns a mock judge model that scores 5 when the answer under
// review contains `good`, else 2 — and wraps the JSON in prose + a code fence to
// exercise extractJSON's tolerance.
func scriptedJudge(good string) llm.Model {
	return mock.New("mock-judge", func(req *llm.Request) *llm.Response {
		last := req.Messages[len(req.Messages)-1].Text()
		score := 2
		reason := "答案不充分"
		if strings.Contains(last, good) {
			score, reason = 5, "答案准确充分"
		}
		out := "我的评估如下：\n```json\n{\"score\": " + itoa(score) + ", \"reason\": \"" + reason + "\"}\n```"
		return mock.Text(out)
	})
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestRubricJudge(t *testing.T) {
	judge := scriptedJudge("北京")
	sc := Rubric(judge, "回答是否正确", WithMaxScore(5), WithThreshold(0.7))

	good := mustScore(t, sc, Sample{Input: "首都?", Output: "北京是中国的首都"})
	if !good.Passed || good.Value < 0.99 {
		t.Fatalf("expected pass with value 1.0, got %+v", good)
	}
	bad := mustScore(t, sc, Sample{Input: "首都?", Output: "上海"})
	if bad.Passed || bad.Value > 0.5 {
		t.Fatalf("expected fail with value 0.4, got %+v", bad)
	}
}

func TestReferenceJudgeNeedsReference(t *testing.T) {
	sc := Reference(scriptedJudge("x"))
	if _, err := sc.Score(context.Background(), Sample{Output: "y"}); err == nil {
		t.Fatal("expected error when Reference is empty")
	}
}

func TestExtractJSON(t *testing.T) {
	cases := map[string]string{
		`{"score":5}`:                        `{"score":5}`,
		"prefix {\"a\":1} suffix":            `{"a":1}`,
		"```json\n{\"a\":{\"b\":2}}\n```":    `{"a":{"b":2}}`,
		`text with "{" in a string {"ok":1}`: `{"ok":1}`,
		"no json here":                       "",
		`{"s":"a\"b"}`:                       `{"s":"a\"b"}`,
	}
	for in, want := range cases {
		if got := extractJSON(in); got != want {
			t.Fatalf("extractJSON(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPairwiseCancelsPositionBias(t *testing.T) {
	// A judge that always prefers whichever side contains "赢". Running both
	// orders, the answer containing "赢" should win regardless of position.
	judge := mock.New("pair", func(req *llm.Request) *llm.Response {
		u := req.Messages[len(req.Messages)-1].Text()
		// The prompt lists 回答 A then 回答 B; pick the one whose block has 赢.
		ai := strings.Index(u, "回答 A：")
		bi := strings.Index(u, "回答 B：")
		winner := "tie"
		if ai >= 0 && bi >= 0 {
			a, b := u[ai:bi], u[bi:]
			switch {
			case strings.Contains(a, "赢") && !strings.Contains(b, "赢"):
				winner = "A"
			case strings.Contains(b, "赢") && !strings.Contains(a, "赢"):
				winner = "B"
			}
		}
		return mock.Text(`{"winner":"` + winner + `","reason":"含赢者优"}`)
	})

	p := Pairwise(judge)
	sc, err := p.Compare(context.Background(),
		Sample{Input: "比", Output: "我会赢"},
		Sample{Input: "比", Output: "普通答案"})
	if err != nil {
		t.Fatal(err)
	}
	if sc.Value != 1.0 || !sc.Passed {
		t.Fatalf("expected A to win both orders (value 1.0), got %+v", sc)
	}
}

func TestWeightedComposite(t *testing.T) {
	// contains(0/1) weight 0.5, rubric(1.0) weight 0.5 → 0.5 when contains misses.
	judge := scriptedJudge("ok")
	w := Weighted(
		Weight(Contains("退款"), 0.5),
		Weight(Rubric(judge, "好不好"), 0.5),
	)
	// Output has "ok" (rubric→1.0) but not "退款" (contains→0) → 0.5 weighted.
	sc := mustScore(t, w, Sample{Input: "q", Output: "ok 没有关键词"})
	if sc.Value < 0.49 || sc.Value > 0.51 {
		t.Fatalf("expected ~0.5, got %g (%s)", sc.Value, sc.Reason)
	}
	if len(sc.Sub) != 2 {
		t.Fatalf("expected 2 sub-scores, got %d", len(sc.Sub))
	}
	// Sub must be sorted by name for determinism: contains < rubric.
	if sc.Sub[0].Name != "contains" || sc.Sub[1].Name != "rubric" {
		t.Fatalf("sub scores not sorted by name: %v", []string{sc.Sub[0].Name, sc.Sub[1].Name})
	}
}

func TestAllAnyComposite(t *testing.T) {
	pass := Contains("a")
	fail := Contains("z")
	s := Sample{Output: "a b c"}

	if mustScore(t, All(pass, fail), s).Passed {
		t.Fatal("All should fail when one fails")
	}
	if !mustScore(t, All(pass), s).Passed {
		t.Fatal("All should pass when all pass")
	}
	if !mustScore(t, Any(pass, fail), s).Passed {
		t.Fatal("Any should pass when one passes")
	}
	if mustScore(t, Any(fail), s).Passed {
		t.Fatal("Any should fail when none pass")
	}
}
