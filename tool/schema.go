package tool

import (
	"encoding/json"
	"reflect"
	"strings"
)

// SchemaFor derives a JSON Schema (draft-style object schema) from a Go type.
// It is used by New to describe a tool's parameters to the model. Supported:
// structs (-> object with properties), strings, bools, integers, floats,
// slices/arrays (-> array), and maps (-> object). Field documentation comes
// from a `desc:"..."` struct tag; field names and optionality come from the
// `json` tag (a field without `omitempty` is required).
func SchemaFor[T any]() json.RawMessage {
	m := schemaForType(reflect.TypeFor[T]())
	b, _ := json.Marshal(m)
	return b
}

func schemaForType(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": schemaForType(t.Elem())}
	case reflect.Map:
		return map[string]any{"type": "object"}
	case reflect.Struct:
		return schemaForStruct(t)
	default:
		return map[string]any{}
	}
}

func schemaForStruct(t reflect.Type) map[string]any {
	props := map[string]any{}
	var required []string
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, omitempty := jsonName(f)
		if name == "-" {
			continue
		}
		s := schemaForType(f.Type)
		if d := f.Tag.Get("desc"); d != "" {
			s["description"] = d
		}
		props[name] = s
		if !omitempty {
			required = append(required, name)
		}
	}
	m := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		m["required"] = required
	}
	return m
}

// jsonName resolves a struct field's wire name and whether it is optional,
// honoring the `json` tag.
func jsonName(f reflect.StructField) (name string, omitempty bool) {
	tag := f.Tag.Get("json")
	if tag == "" {
		return f.Name, false
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	if name == "" {
		name = f.Name
	}
	for _, opt := range parts[1:] {
		if opt == "omitempty" {
			omitempty = true
		}
	}
	return name, omitempty
}
