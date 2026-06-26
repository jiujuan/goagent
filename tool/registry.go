package tool

// Schemas converts a list of tools to their provider-neutral schemas. The
// agent turn engine uses it to advertise tools on each model request.
import (
	"github.com/jiujuan/goagent/llm"
)

// Schemas maps tools to the llm.ToolSchema list a Request advertises.
func Schemas(tools []Tool) []llm.ToolSchema {
	if len(tools) == 0 {
		return nil
	}
	out := make([]llm.ToolSchema, len(tools))
	for i, t := range tools {
		out[i] = llm.ToolSchema{
			Name:        t.Name(),
			Description: t.Description(),
			Parameters:  t.Schema(),
		}
	}
	return out
}

// ByName indexes tools by name for dispatch.
func ByName(tools []Tool) map[string]Tool {
	m := make(map[string]Tool, len(tools))
	for _, t := range tools {
		m[t.Name()] = t
	}
	return m
}
