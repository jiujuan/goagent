package tool_test

import (
	"strings"
	"testing"

	"github.com/jiujuan/goagent/tool"
)

type weatherIn struct {
	City string    `json:"city"`
	Days int       `json:"days,omitempty"`
	Tags []string  `json:"tags,omitempty"`
	Opts *optsIn   `json:"opts,omitempty"`
}

type optsIn struct {
	Unit string `json:"unit"`
}

func check(t *testing.T, schema []byte, args string, wantErr string) {
	t.Helper()
	err := tool.Validate(schema, []byte(args))
	switch {
	case wantErr == "" && err != nil:
		t.Fatalf("Validate(%s) = %v, want nil", args, err)
	case wantErr != "" && err == nil:
		t.Fatalf("Validate(%s) = nil, want error %q", args, wantErr)
	case wantErr != "" && !strings.Contains(err.Error(), wantErr):
		t.Fatalf("Validate(%s) = %v, want it to contain %q", args, err, wantErr)
	}
}

func TestValidateAgainstGeneratedSchema(t *testing.T) {
	s := tool.SchemaFor[weatherIn]()

	check(t, s, `{"city":"Oslo","days":3,"tags":["a","b"],"opts":{"unit":"c"}}`, "")
	check(t, s, `{"city":"Oslo"}`, "")
	check(t, s, `{}`, `required argument "city" is missing`)
	check(t, s, `{"days":2}`, `required argument "city" is missing`)
	check(t, s, `{"city":123}`, `"city": expected string, got integer`)
	check(t, s, `{"city":"x","days":1.5}`, `"days": expected integer, got number`)
	check(t, s, `{"city":"x","tags":["a",2]}`, `"tags[1]": expected string, got integer`)
	check(t, s, `{"city":"x","opts":{}}`, `required argument "unit" is missing`)
	check(t, s, `{"city":"x","opts":{"unit":9}}`, `"opts.unit": expected string, got integer`)
	check(t, s, `{"city":"x",`, "not valid JSON")
	check(t, s, `"a string"`, "arguments: expected object, got string")
	check(t, s, `[]`, "arguments: expected object, got array")
}

func TestValidateEmptyArgsIsObject(t *testing.T) {
	s := tool.SchemaFor[weatherIn]()
	// Providers commonly send "" for no-arg calls; it must read as {}.
	check(t, s, ``, `required argument "city" is missing`)

	type emptyIn struct{}
	es := tool.SchemaFor[emptyIn]()
	check(t, es, ``, "")
	check(t, es, `{}`, "")
}

func TestValidateHandWrittenSchemas(t *testing.T) {
	// Array-of-types form.
	arr := []byte(`{"type":["string","null"]}`)
	check(t, arr, `"x"`, "")
	check(t, arr, `null`, "")
	check(t, arr, `42`, "expected string or null, got integer")

	// number accepts integral floats and fractions.
	num := []byte(`{"type":"number"}`)
	check(t, num, `2`, "")
	check(t, num, `2.5`, "")
	check(t, num, `"2"`, "expected number, got string")

	// Unknown keywords are ignored (permissive for third-party schemas).
	enum := []byte(`{"type":"string","enum":["a","b"],"format":"id"}`)
	check(t, enum, `"zzz"`, "")

	// Missing or unparsable type keywords never reject.
	check(t, []byte(`{"properties":{"x":{"type":"weird"}}}`), `{"x":1}`, "")
	check(t, []byte(`not json`), `{"whatever":true}`, "")
	check(t, nil, `{"whatever":true}`, "")
}

func TestValidateMultipleMissingRequired(t *testing.T) {
	s := []byte(`{"type":"object","required":["b","a"],"properties":{}}`)
	err := tool.Validate(s, []byte(`{}`))
	if err == nil {
		t.Fatal("want error")
	}
	msg := err.Error()
	// Names are sorted for a stable message; both must appear.
	if !strings.Contains(msg, `"a", "b"`) {
		t.Fatalf("message = %q, want sorted names", msg)
	}
}
