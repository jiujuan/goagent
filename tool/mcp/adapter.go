package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jiujuan/goagent/core"
	"github.com/jiujuan/goagent/tool"
)

// mcpTool adapts one remote MCP tool into a tool.Tool. Its schema is the
// server's inputSchema passed through unchanged; Call forwards the model's
// arguments to tools/call and maps the result back to message parts.
type mcpTool struct {
	name       string          // model-facing name (toolPrefix + remoteName)
	remoteName string          // name sent to the server
	desc       string          // model-facing description
	schema     json.RawMessage // server inputSchema, advertised verbatim
	conn       *conn
	timeout    time.Duration
}

// newMCPTool builds an adapter for a tool descriptor under the given config.
func newMCPTool(c *conn, cfg config, d toolDescriptor) *mcpTool {
	schema := d.InputSchema
	if len(schema) == 0 || string(schema) == "null" {
		// Some servers omit the schema for argument-less tools; advertise a
		// minimal object so providers that require a schema stay happy.
		schema = json.RawMessage(`{"type":"object"}`)
	}
	return &mcpTool{
		name:       cfg.toolPrefix + d.Name,
		remoteName: d.Name,
		desc:       d.Description,
		schema:     schema,
		conn:       c,
		timeout:    cfg.timeout,
	}
}

func (t *mcpTool) Name() string            { return t.name }
func (t *mcpTool) Description() string     { return t.desc }
func (t *mcpTool) Schema() json.RawMessage { return t.schema }

// Call invokes the remote tool. Transport and protocol failures are reported to
// the model as tool errors (not Go errors) so the agent can recover, matching
// the convention used by the other built-in tools.
func (t *mcpTool) Call(ctx *tool.Context, args json.RawMessage) (*tool.Result, error) {
	callCtx := ctx.Context
	if t.timeout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(callCtx, t.timeout)
		defer cancel()
	}

	params := callToolParams{Name: t.remoteName}
	if len(args) > 0 {
		params.Arguments = args
	}

	raw, err := t.conn.call(callCtx, methodToolsCall, params)
	if err != nil {
		return tool.ErrorResult(fmt.Sprintf("mcp: %v", err)), nil
	}

	var res callToolResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return tool.ErrorResult("mcp: invalid tool result: " + err.Error()), nil
	}
	return &tool.Result{Content: contentToParts(res.Content), IsError: res.IsError}, nil
}

// contentToParts maps MCP content blocks to goagent message parts. Text and
// image map to their native parts; audio and resources are rendered as
// descriptive text so they still reach the model. An empty result becomes a
// single empty text part, matching how the typed-tool path renders no output.
func contentToParts(blocks []contentBlock) []core.Part {
	parts := make([]core.Part, 0, len(blocks))
	for _, b := range blocks {
		switch b.Type {
		case "text":
			parts = append(parts, core.Text{Text: b.Text})
		case "image":
			if data, err := base64.StdEncoding.DecodeString(b.Data); err == nil {
				parts = append(parts, core.Image{MIME: b.MimeType, Data: data})
			} else {
				parts = append(parts, core.Text{Text: "[image: " + err.Error() + "]"})
			}
		case "audio":
			parts = append(parts, core.Text{Text: fmt.Sprintf("[audio %s, %d base64 bytes]", b.MimeType, len(b.Data))})
		case "resource":
			parts = append(parts, resourceToPart(b.Resource))
		default:
			if b.Text != "" {
				parts = append(parts, core.Text{Text: b.Text})
			}
		}
	}
	if len(parts) == 0 {
		parts = append(parts, core.Text{Text: ""})
	}
	return parts
}

func resourceToPart(r *resourceContents) core.Part {
	switch {
	case r == nil:
		return core.Text{Text: "[resource]"}
	case r.Text != "":
		return core.Text{Text: r.Text}
	case r.URI != "":
		return core.Text{Text: "[resource: " + r.URI + "]"}
	default:
		return core.Text{Text: "[resource]"}
	}
}
