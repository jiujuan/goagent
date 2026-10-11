package core

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

// ErrStateNotCloneable reports data that cannot be copied without changing its
// JSON representation or retaining mutable aliases.
var ErrStateNotCloneable = errors.New("core: state value cannot be cloned")

var (
	timeType            = reflect.TypeFor[time.Time]()
	rawMessageType      = reflect.TypeFor[json.RawMessage]()
	jsonNumberType      = reflect.TypeFor[json.Number]()
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
)

// CloneMessage returns a message whose mutable part data is independent from m.
func CloneMessage(m Message) (Message, error) {
	return cloneMessageAt(m, "message")
}

func cloneMessageAt(m Message, path string) (Message, error) {
	out := Message{Role: m.Role}
	if m.Parts == nil {
		return out, nil
	}
	out.Parts = make([]Part, len(m.Parts))
	for i, part := range m.Parts {
		cloned, err := clonePart(part, fmt.Sprintf("%s.parts[%d]", path, i))
		if err != nil {
			return Message{}, err
		}
		out.Parts[i] = cloned
	}
	return out, nil
}

func clonePart(part Part, path string) (Part, error) {
	switch value := part.(type) {
	case Text:
		return value, nil
	case Thinking:
		return value, nil
	case Image:
		value.Data = cloneBytes(value.Data)
		return value, nil
	case Video:
		value.Data = cloneBytes(value.Data)
		return value, nil
	case ToolCall:
		value.Args = json.RawMessage(cloneBytes(value.Args))
		return value, nil
	case ToolResult:
		if value.Content == nil {
			return value, nil
		}
		content := value.Content
		value.Content = make([]Part, len(content))
		for i, part := range content {
			cloned, err := clonePart(part, fmt.Sprintf("%s.content[%d]", path, i))
			if err != nil {
				return nil, err
			}
			value.Content[i] = cloned
		}
		return value, nil
	default:
		return nil, notCloneable(path, reflect.TypeOf(part), "unsupported message part")
	}
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

// CloneStateData deep-copies checkpointable State data while retaining the
// active Files handle. File snapshots are frozen by the checkpoint layer.
func CloneStateData(state State) (State, error) {
	out := State{Files: state.Files}
	if state.Messages != nil {
		out.Messages = make([]Message, len(state.Messages))
		for i, message := range state.Messages {
			cloned, err := cloneMessageAt(message, fmt.Sprintf("state.messages[%d]", i))
			if err != nil {
				return State{}, err
			}
			out.Messages[i] = cloned
		}
	}
	if state.Todos != nil {
		out.Todos = make([]Todo, len(state.Todos))
		copy(out.Todos, state.Todos)
	}
	if state.KV == nil {
		return out, nil
	}

	value := reflect.ValueOf(state.KV)
	if err := validateCloneValue(value, "state.kv", make(map[cloneVisit]string)); err != nil {
		return State{}, err
	}
	if _, err := json.Marshal(state.KV); err != nil {
		return State{}, notCloneable("state.kv", value.Type(), "JSON encoding failed: %v", err)
	}
	cloned, err := cloneValue(value, "state.kv")
	if err != nil {
		return State{}, err
	}
	out.KV = cloned.Interface().(map[string]any)
	return out, nil
}

type cloneVisit struct {
	typeOf  reflect.Type
	pointer uintptr
}

func validateCloneValue(value reflect.Value, path string, active map[cloneVisit]string) error {
	if !value.IsValid() {
		return nil
	}
	typeOf := value.Type()
	if isWhitelistedJSONType(typeOf) {
		if _, err := json.Marshal(value.Interface()); err != nil {
			return notCloneable(path, typeOf, "JSON encoding failed: %v", err)
		}
		return nil
	}
	if hasCustomJSONCodec(typeOf) {
		return notCloneable(path, typeOf, "custom JSON or text codecs are not supported")
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return validateCloneValue(value.Elem(), path, active)
	case reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		leave, err := enterCloneValue(value, path, active)
		if err != nil {
			return err
		}
		defer leave()
		return validateCloneValue(value.Elem(), path, active)
	case reflect.Map:
		if typeOf.Key().Kind() != reflect.String {
			return notCloneable(path, typeOf, "map keys must be strings")
		}
		if value.IsNil() {
			return nil
		}
		leave, err := enterCloneValue(value, path, active)
		if err != nil {
			return err
		}
		defer leave()
		iter := value.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			if err := validateCloneValue(iter.Value(), path+"["+strconv.Quote(key)+"]", active); err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice:
		if value.IsNil() {
			return nil
		}
		leave, err := enterCloneValue(value, path, active)
		if err != nil {
			return err
		}
		defer leave()
		for i := 0; i < value.Len(); i++ {
			if err := validateCloneValue(value.Index(i), fmt.Sprintf("%s[%d]", path, i), active); err != nil {
				return err
			}
		}
		return nil
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := validateCloneValue(value.Index(i), fmt.Sprintf("%s[%d]", path, i), active); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := typeOf.Field(i)
			fieldPath := path + "." + field.Name
			if !field.IsExported() {
				return notCloneable(fieldPath, typeOf, "unexported field %q cannot be cloned safely", field.Name)
			}
			if err := validateCloneValue(value.Field(i), fieldPath, active); err != nil {
				return err
			}
		}
		return nil
	case reflect.Float32, reflect.Float64:
		floatValue := value.Float()
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return notCloneable(path, typeOf, "non-finite floating-point value")
		}
		return nil
	case reflect.Func, reflect.Chan, reflect.Complex64, reflect.Complex128, reflect.UnsafePointer:
		return notCloneable(path, typeOf, "unsupported kind %s", value.Kind())
	default:
		return nil
	}
}

func enterCloneValue(value reflect.Value, path string, active map[cloneVisit]string) (func(), error) {
	visit := cloneVisit{typeOf: value.Type(), pointer: uintptr(value.UnsafePointer())}
	if previous, ok := active[visit]; ok {
		return nil, notCloneable(path, value.Type(), "cycle references %s", previous)
	}
	active[visit] = path
	return func() { delete(active, visit) }, nil
}

func hasCustomJSONCodec(typeOf reflect.Type) bool {
	for _, codec := range []reflect.Type{jsonMarshalerType, jsonUnmarshalerType, textMarshalerType, textUnmarshalerType} {
		if typeOf.Implements(codec) || (typeOf.Kind() != reflect.Pointer && reflect.PointerTo(typeOf).Implements(codec)) {
			return true
		}
	}
	return false
}

func isWhitelistedJSONType(typeOf reflect.Type) bool {
	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	return typeOf == timeType || typeOf == rawMessageType || typeOf == jsonNumberType
}

func cloneValue(value reflect.Value, path string) (reflect.Value, error) {
	if !value.IsValid() {
		return reflect.Value{}, nil
	}
	if value.Type() == timeType {
		return value, nil
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		cloned, err := cloneValue(value.Elem(), path)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(value.Type()).Elem()
		out.Set(cloned)
		return out, nil
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		cloned, err := cloneValue(value.Elem(), path)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(cloned)
		return out, nil
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			key := iter.Key()
			cloned, err := cloneValue(iter.Value(), path+"["+strconv.Quote(key.String())+"]")
			if err != nil {
				return reflect.Value{}, err
			}
			out.SetMapIndex(key, cloned)
		}
		return out, nil
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			cloned, err := cloneValue(value.Index(i), fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return reflect.Value{}, err
			}
			out.Index(i).Set(cloned)
		}
		return out, nil
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			cloned, err := cloneValue(value.Index(i), fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return reflect.Value{}, err
			}
			out.Index(i).Set(cloned)
		}
		return out, nil
	case reflect.Struct:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if !field.IsExported() {
				return reflect.Value{}, notCloneable(path+"."+field.Name, value.Type(), "unexported field %q cannot be cloned safely", field.Name)
			}
			cloned, err := cloneValue(value.Field(i), path+"."+field.Name)
			if err != nil {
				return reflect.Value{}, err
			}
			out.Field(i).Set(cloned)
		}
		return out, nil
	case reflect.Func, reflect.Chan, reflect.Complex64, reflect.Complex128, reflect.UnsafePointer:
		return reflect.Value{}, notCloneable(path, value.Type(), "unsupported kind %s", value.Kind())
	default:
		return value, nil
	}
}

func notCloneable(path string, typeOf reflect.Type, format string, args ...any) error {
	typeName := "<nil>"
	if typeOf != nil {
		typeName = typeOf.String()
	}
	return fmt.Errorf("%w: %s (%s): %s", ErrStateNotCloneable, path, typeName, fmt.Sprintf(format, args...))
}
