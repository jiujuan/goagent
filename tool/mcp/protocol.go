// Package mcp is an MCP (Model Context Protocol) client that lets a goagent
// Agent call tools exposed by external MCP servers. It connects to a server,
// performs the standard handshake, lists the server's tools, and adapts each
// one into a tool.Tool you drop straight into agent.Config.Tools — no changes
// to the agent engine required:
//
//	client, err := mcp.Connect(ctx,
//	    mcp.Stdio("npx", "-y", "@modelcontextprotocol/server-filesystem", "/data"),
//	    mcp.WithToolPrefix("fs_"),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	ag := agent.New(agent.Config{
//	    Name:  "assistant",
//	    Model: model,
//	    Tools: client.Tools(), // []tool.Tool, ready to wire
//	})
//
// The client speaks JSON-RPC 2.0 and depends only on the standard library. The
// built-in transport is Stdio (launches a server subprocess); the Transport
// interface is the seam for other transports (e.g. HTTP/SSE) added later.
package mcp

import "encoding/json"

// protocolVersion is the MCP revision this client advertises in initialize.
const protocolVersion = "2024-11-05"

// JSON-RPC method names used by this client.
const (
	methodInitialize  = "initialize"
	methodInitialized = "notifications/initialized"
	methodToolsList   = "tools/list"
	methodToolsCall   = "tools/call"
)

// implementation identifies a client or server by name and version, per the
// MCP "Implementation" object.
type implementation struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// initializeParams is the payload of the initialize request. Capabilities is
// sent as an empty object: this client consumes tools but advertises no
// optional features (sampling, roots, etc.).
type initializeParams struct {
	ProtocolVersion string         `json:"protocolVersion"`
	Capabilities    struct{}       `json:"capabilities"`
	ClientInfo      implementation `json:"clientInfo"`
}

// initializeResult is the server's response to initialize. Capabilities is kept
// raw because this client does not branch on server features today.
type initializeResult struct {
	ProtocolVersion string          `json:"protocolVersion"`
	Capabilities    json.RawMessage `json:"capabilities"`
	ServerInfo      implementation  `json:"serverInfo"`
}

// toolDescriptor is one entry of a tools/list result. InputSchema is the JSON
// Schema for the tool's arguments and is passed through to the model verbatim.
type toolDescriptor struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// listToolsResult is the tools/list response. NextCursor drives pagination.
type listToolsResult struct {
	Tools      []toolDescriptor `json:"tools"`
	NextCursor string           `json:"nextCursor,omitempty"`
}

// callToolParams is the payload of a tools/call request.
type callToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// callToolResult is the tools/call response: a list of content blocks plus an
// isError flag distinguishing a tool-level failure from a successful run.
type callToolResult struct {
	Content []contentBlock `json:"content"`
	IsError bool           `json:"isError,omitempty"`
}

// contentBlock is one piece of a tool result. The set of fields covers the MCP
// text/image/audio/resource content types; Type selects which fields are set.
type contentBlock struct {
	Type     string            `json:"type"`
	Text     string            `json:"text,omitempty"`
	Data     string            `json:"data,omitempty"` // base64 for image/audio
	MimeType string            `json:"mimeType,omitempty"`
	Resource *resourceContents `json:"resource,omitempty"`
}

// resourceContents is the embedded resource of a "resource" content block.
type resourceContents struct {
	URI      string `json:"uri,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	Text     string `json:"text,omitempty"`
	Blob     string `json:"blob,omitempty"` // base64
}
