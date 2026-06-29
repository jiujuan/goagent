package eval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// rule.go holds deterministic, LLM-free scorers — the cheap, reproducible tier
// that underpins tool evaluation and CI smoke checks. Each is pass/fail (Value
// 1.0 or 0.0) with a Reason that records the observed value, so a failing case
// is self-explanatory in a report.

// numberRe extracts the first (optionally signed/decimal) number from a string.
var numberRe = regexp.MustCompile(`-?\d+(?:\.\d+)?`)

// ExactMatch passes when Output equals want after trimming surrounding
// whitespace (the usual intent when comparing to a short gold string).
func ExactMatch(want string) Scorer {
	return newScorer("exact_match", func(_ context.Context, s Sample) (Score, error) {
		got := strings.TrimSpace(s.Output)
		pass := got == strings.TrimSpace(want)
		return boolScore("exact_match", pass, fmt.Sprintf("got %q, want %q", got, want)), nil
	})
}

// Contains passes when Output contains sub.
func Contains(sub string) Scorer {
	return newScorer("contains", func(_ context.Context, s Sample) (Score, error) {
		pass := strings.Contains(s.Output, sub)
		return boolScore("contains", pass, fmt.Sprintf("substring %q present=%v", sub, pass)), nil
	})
}

// Regex passes when Output matches re.
func Regex(re *regexp.Regexp) Scorer {
	return newScorer("regex", func(_ context.Context, s Sample) (Score, error) {
		if re == nil {
			return Score{Name: "regex"}, errors.New("eval: Regex scorer needs a non-nil pattern")
		}
		pass := re.MatchString(s.Output)
		return boolScore("regex", pass, fmt.Sprintf("pattern %q match=%v", re.String(), pass)), nil
	})
}

// JSONValid passes when Output (trimmed) is well-formed JSON.
func JSONValid() Scorer {
	return newScorer("json_valid", func(_ context.Context, s Sample) (Score, error) {
		pass := json.Valid([]byte(strings.TrimSpace(s.Output)))
		return boolScore("json_valid", pass, fmt.Sprintf("valid_json=%v", pass)), nil
	})
}

// JSONSchema passes when Output is a JSON object whose required keys are present
// and whose values match the declared primitive types. It is a pragmatic subset
// of JSON Schema — enough to validate tool outputs whose schemas come from
// tool.SchemaFor (type / properties / required) without pulling in a full
// validator dependency.
func JSONSchema(schema json.RawMessage) Scorer {
	return newScorer("json_schema", func(_ context.Context, s Sample) (Score, error) {
		var data any
		if err := json.Unmarshal([]byte(strings.TrimSpace(s.Output)), &data); err != nil {
			return boolScore("json_schema", false, "output is not valid JSON: "+err.Error()), nil
		}
		var sch map[string]any
		if err := json.Unmarshal(schema, &sch); err != nil {
			return Score{Name: "json_schema"}, fmt.Errorf("eval: invalid schema: %w", err)
		}
		if err := validateSchema(data, sch); err != nil {
			return boolScore("json_schema", false, err.Error()), nil
		}
		return boolScore("json_schema", true, "matches schema"), nil
	})
}

// NumericClose passes when the first number parsed from Output is within tol of
// want. Useful for arithmetic/aggregation answers where formatting varies.
func NumericClose(want, tol float64) Scorer {
	return newScorer("numeric_close", func(_ context.Context, s Sample) (Score, error) {
		m := numberRe.FindString(s.Output)
		if m == "" {
			return boolScore("numeric_close", false, "no number found in output"), nil
		}
		got, err := strconv.ParseFloat(m, 64)
		if err != nil {
			return boolScore("numeric_close", false, "unparseable number: "+m), nil
		}
		diff := got - want
		if diff < 0 {
			diff = -diff
		}
		pass := diff <= tol
		return boolScore("numeric_close", pass, fmt.Sprintf("got %g, want %g±%g", got, want, tol)), nil
	})
}

// NoToolError passes when the Sample's tool episode did not error. It is a value
// type so callers write eval.NoToolError{}.
type NoToolError struct{}

func (NoToolError) Name() string { return "no_tool_error" }
func (NoToolError) Score(_ context.Context, s Sample) (Score, error) {
	if s.Tool == nil {
		return Score{Name: "no_tool_error"}, errors.New("eval: NoToolError needs Sample.Tool")
	}
	pass := !s.Tool.Result.IsError
	return boolScore("no_tool_error", pass, fmt.Sprintf("tool %q is_error=%v", s.Tool.Result.Name, s.Tool.Result.IsError)), nil
}

// MaxSteps passes when the trajectory used at most n model steps.
func MaxSteps(n int) Scorer {
	return newScorer("max_steps", func(_ context.Context, s Sample) (Score, error) {
		if s.Traj == nil {
			return Score{Name: "max_steps"}, errors.New("eval: MaxSteps needs Sample.Traj")
		}
		pass := s.Traj.Steps <= n
		return boolScore("max_steps", pass, fmt.Sprintf("steps=%d, max=%d", s.Traj.Steps, n)), nil
	})
}

// TokenBudget passes when the trajectory's total tokens stayed within budget.
func TokenBudget(n int) Scorer {
	return newScorer("token_budget", func(_ context.Context, s Sample) (Score, error) {
		if s.Traj == nil {
			return Score{Name: "token_budget"}, errors.New("eval: TokenBudget needs Sample.Traj")
		}
		total := s.Traj.Usage.InputTokens + s.Traj.Usage.OutputTokens
		pass := total <= n
		return boolScore("token_budget", pass, fmt.Sprintf("tokens=%d, budget=%d", total, n)), nil
	})
}

// --- minimal JSON Schema check ---------------------------------------------

// validateSchema checks data against a schema subset: object "required" keys and
// each property's declared "type". Nested objects recurse; unknown keywords are
// ignored. It returns the first violation.
func validateSchema(data any, schema map[string]any) error {
	typ, _ := schema["type"].(string)
	if typ != "" && !typeMatches(typ, data) {
		return fmt.Errorf("expected type %q, got %s", typ, jsonKind(data))
	}
	if typ != "object" && typ != "" {
		return nil
	}
	obj, ok := data.(map[string]any)
	if !ok {
		if typ == "" {
			return nil // no type constraint and not an object: nothing to check
		}
		return fmt.Errorf("expected object, got %s", jsonKind(data))
	}
	if req, ok := schema["required"].([]any); ok {
		for _, r := range req {
			key, _ := r.(string)
			if _, present := obj[key]; !present {
				return fmt.Errorf("missing required key %q", key)
			}
		}
	}
	props, _ := schema["properties"].(map[string]any)
	for key, val := range obj {
		ps, ok := props[key].(map[string]any)
		if !ok {
			continue // property not described; don't constrain it
		}
		if err := validateSchema(val, ps); err != nil {
			return fmt.Errorf("%s.%w", key, err)
		}
	}
	return nil
}

// typeMatches reports whether a decoded JSON value matches a schema type name.
func typeMatches(typ string, v any) bool {
	switch typ {
	case "string":
		_, ok := v.(string)
		return ok
	case "number":
		_, ok := v.(float64)
		return ok
	case "integer":
		f, ok := v.(float64)
		return ok && f == float64(int64(f))
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "object":
		_, ok := v.(map[string]any)
		return ok
	default:
		return true // unknown type keyword: don't fail on it
	}
}

// jsonKind names a decoded JSON value's kind for error messages.
func jsonKind(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%T", v)
	}
}
