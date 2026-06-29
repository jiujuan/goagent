package eval

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/jiujuan/goagent/core"
)

func mustScore(t *testing.T, sc Scorer, s Sample) Score {
	t.Helper()
	got, err := sc.Score(context.Background(), s)
	if err != nil {
		t.Fatalf("%s.Score error: %v", sc.Name(), err)
	}
	return got
}

func TestRuleScorers(t *testing.T) {
	tests := []struct {
		name   string
		scorer Scorer
		sample Sample
		want   bool
	}{
		{"exact match trims", ExactMatch("hello"), Sample{Output: "  hello\n"}, true},
		{"exact mismatch", ExactMatch("hello"), Sample{Output: "hellos"}, false},
		{"contains hit", Contains("退款"), Sample{Output: "请在订单页申请退款"}, true},
		{"contains miss", Contains("退款"), Sample{Output: "请联系客服"}, false},
		{"regex hit", Regex(regexp.MustCompile(`\d{4}`)), Sample{Output: "code 2026 ok"}, true},
		{"regex miss", Regex(regexp.MustCompile(`^\d+$`)), Sample{Output: "12a"}, false},
		{"json valid", JSONValid(), Sample{Output: ` {"a":1} `}, true},
		{"json invalid", JSONValid(), Sample{Output: `{a:1}`}, false},
		{"numeric close", NumericClose(42, 0.5), Sample{Output: "约 42.2 元"}, true},
		{"numeric far", NumericClose(42, 0.5), Sample{Output: "约 50 元"}, false},
		{"numeric none", NumericClose(42, 0.5), Sample{Output: "没有数字"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mustScore(t, tt.scorer, tt.sample)
			if got.Passed != tt.want {
				t.Fatalf("Passed=%v want %v (value=%g reason=%q)", got.Passed, tt.want, got.Value, got.Reason)
			}
			if (got.Value == 1.0) != got.Passed {
				t.Fatalf("Value %g inconsistent with Passed %v", got.Value, got.Passed)
			}
		})
	}
}

func TestNoToolError(t *testing.T) {
	ok := Sample{Tool: &ToolEpisode{Result: core.ToolResult{Name: "get_weather", IsError: false}}}
	bad := Sample{Tool: &ToolEpisode{Result: core.ToolResult{Name: "get_weather", IsError: true}}}
	if !mustScore(t, NoToolError{}, ok).Passed {
		t.Fatal("expected pass on non-error tool result")
	}
	if mustScore(t, NoToolError{}, bad).Passed {
		t.Fatal("expected fail on error tool result")
	}
	if _, err := (NoToolError{}).Score(context.Background(), Sample{}); err == nil {
		t.Fatal("expected error when Sample.Tool is nil")
	}
}

func TestTrajectoryScorers(t *testing.T) {
	traj := &Trajectory{Steps: 5, Usage: core.Usage{InputTokens: 1000, OutputTokens: 500}}
	if !mustScore(t, MaxSteps(8), Sample{Traj: traj}).Passed {
		t.Fatal("5 steps should pass MaxSteps(8)")
	}
	if mustScore(t, MaxSteps(3), Sample{Traj: traj}).Passed {
		t.Fatal("5 steps should fail MaxSteps(3)")
	}
	if !mustScore(t, TokenBudget(2000), Sample{Traj: traj}).Passed {
		t.Fatal("1500 tokens should pass TokenBudget(2000)")
	}
	if mustScore(t, TokenBudget(1000), Sample{Traj: traj}).Passed {
		t.Fatal("1500 tokens should fail TokenBudget(1000)")
	}
	if _, err := MaxSteps(1).Score(context.Background(), Sample{}); err == nil {
		t.Fatal("expected error when Sample.Traj is nil")
	}
}

func TestJSONSchema(t *testing.T) {
	schema := json.RawMessage(`{
		"type":"object",
		"required":["city","temp"],
		"properties":{
			"city":{"type":"string"},
			"temp":{"type":"number"},
			"humid":{"type":"integer"}
		}
	}`)
	sc := JSONSchema(schema)

	cases := []struct {
		name   string
		output string
		want   bool
	}{
		{"valid", `{"city":"北京","temp":25.5}`, true},
		{"valid with int", `{"city":"北京","temp":25.5,"humid":40}`, true},
		{"missing required", `{"city":"北京"}`, false},
		{"wrong type", `{"city":"北京","temp":"hot"}`, false},
		{"int not int", `{"city":"北京","temp":25,"humid":40.5}`, false},
		{"not json", `not json`, false},
		{"not object", `[1,2,3]`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mustScore(t, sc, Sample{Output: c.output})
			if got.Passed != c.want {
				t.Fatalf("Passed=%v want %v (reason=%q)", got.Passed, c.want, got.Reason)
			}
		})
	}
}
