package tool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Validate checks raw JSON arguments against a tool's advertised schema,
// covering the keywords this package emits (type, properties, required,
// items; see SchemaFor) plus JSON Schema's array-of-types form. Unknown
// keywords (enum, anyOf, formats, ...) are ignored, so richer third-party
// schemas validate permissively rather than spuriously rejecting calls.
//
// Errors are phrased for the model to self-correct: a JSON-pointer-style
// path plus the expected and observed kinds. A missing or unparsable schema
// is a tool-authoring problem, not a model error, so validation is skipped.
func Validate(schema, args json.RawMessage) error {
	s, ok := parseSchema(schema)
	if !ok {
		return nil
	}
	b := bytes.TrimSpace(args)
	if len(b) == 0 {
		b = []byte(`{}`)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("arguments are not valid JSON: %v", err)
	}
	return checkValue(v, s, "")
}

func parseSchema(schema json.RawMessage) (map[string]any, bool) {
	if len(bytes.TrimSpace(schema)) == 0 {
		return nil, false
	}
	var s map[string]any
	if err := json.Unmarshal(schema, &s); err != nil {
		return nil, false
	}
	return s, true
}

func checkValue(v any, s map[string]any, path string) error {
	if want, has := s["type"]; has {
		if err := checkType(v, want, path); err != nil {
			return err
		}
	}
	switch val := v.(type) {
	case map[string]any:
		if req, ok := s["required"].([]any); ok {
			names := make([]string, 0, len(req))
			for _, r := range req {
				if name, ok := r.(string); ok {
					if _, has := val[name]; !has {
						names = append(names, name)
					}
				}
			}
			if len(names) > 0 {
				sort.Strings(names)
				if len(names) == 1 {
					return fmt.Errorf("%s: required argument %q is missing", loc(path), names[0])
				}
				return fmt.Errorf("%s: required arguments %s are missing", loc(path), quoteList(names))
			}
		}
		if props, ok := s["properties"].(map[string]any); ok {
			for _, key := range sortedKeys(props) {
				sub, ok := props[key].(map[string]any)
				if !ok {
					continue
				}
				if sv, has := val[key]; has {
					if err := checkValue(sv, sub, joinPath(path, key)); err != nil {
						return err
					}
				}
			}
		}
	case []any:
		items, ok := s["items"].(map[string]any)
		if !ok {
			return nil
		}
		for i, el := range val {
			if err := checkValue(el, items, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkType(v any, want any, path string) error {
	types, ok := want.([]any)
	if !ok {
		if t, ok := want.(string); ok {
			types = []any{t}
		}
	}
	names := make([]string, 0, len(types))
	for _, t := range types {
		name, ok := t.(string)
		if !ok {
			continue
		}
		if matchesKind(v, name) {
			return nil
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil // schema type neither a string nor a string array: skip
	}
	return fmt.Errorf("%s: expected %s, got %s", loc(path), strings.Join(names, " or "), kindOf(v))
}

func matchesKind(v any, want string) bool {
	switch want {
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "null":
		return v == nil
	case "integer":
		f, ok := v.(float64)
		return ok && f == math.Trunc(f)
	case "number":
		_, ok := v.(float64)
		return ok
	default:
		return true // unknown type keyword: permissive
	}
}

func kindOf(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case bool:
		return "boolean"
	case float64:
		if t == math.Trunc(t) {
			return "integer"
		}
		return "number"
	}
	return "unexpected value"
}

func loc(path string) string {
	if path == "" {
		return "arguments"
	}
	return `"` + path + `"`
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func quoteList(names []string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = `"` + n + `"`
	}
	return strings.Join(out, ", ")
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
