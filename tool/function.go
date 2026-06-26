package tool

import (
	"encoding/json"

	"github.com/jiujuan/goagent/core"
)

// Func is a typed tool handler. In is unmarshaled from the model's JSON
// arguments; Out is marshaled back as the tool result (a string is returned
// verbatim, anything else as JSON).
type Func[In, Out any] func(ctx *Context, in In) (Out, error)

// New builds a Tool from a typed handler, deriving the parameter schema from In
// via reflection. This is the primary way to author tools:
//
//	weather := tool.New("get_weather", "Look up weather",
//	    func(ctx *tool.Context, in struct{ City string `json:"city" desc:"city name"` }) (string, error) {
//	        return "Sunny 25C", nil
//	    })
func New[In, Out any](name, description string, fn Func[In, Out]) Tool {
	return &funcTool[In, Out]{
		name:   name,
		desc:   description,
		schema: SchemaFor[In](),
		fn:     fn,
	}
}

type funcTool[In, Out any] struct {
	name   string
	desc   string
	schema json.RawMessage
	fn     Func[In, Out]
}

func (t *funcTool[In, Out]) Name() string            { return t.name }
func (t *funcTool[In, Out]) Description() string     { return t.desc }
func (t *funcTool[In, Out]) Schema() json.RawMessage { return t.schema }

func (t *funcTool[In, Out]) Call(ctx *Context, args json.RawMessage) (*Result, error) {
	var in In
	if len(args) > 0 {
		if err := json.Unmarshal(args, &in); err != nil {
			return ErrorResult("invalid arguments: " + err.Error()), nil
		}
	}
	out, err := t.fn(ctx, in)
	if err != nil {
		// Tool errors are reported to the model, not propagated as Go errors,
		// so the agent can recover. Infrastructure failures should panic or be
		// surfaced differently.
		return ErrorResult(err.Error()), nil
	}
	return &Result{Content: renderOutput(out)}, nil
}

func renderOutput(out any) []core.Part {
	switch v := out.(type) {
	case string:
		return []core.Part{core.Text{Text: v}}
	case nil:
		return []core.Part{core.Text{Text: ""}}
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return []core.Part{core.Text{Text: err.Error()}}
		}
		return []core.Part{core.Text{Text: string(b)}}
	}
}
