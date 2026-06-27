package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jiujuan/goagent/tool"
)

// Client is a live connection to one MCP server. After Connect returns, its
// tools are ready via Tools; Close terminates the connection (and, for Stdio,
// the server subprocess).
type Client struct {
	conn       *conn
	serverInfo implementation
	tools      []tool.Tool
}

// config holds client-level settings resolved from Option values.
type config struct {
	clientName    string
	clientVersion string
	timeout       time.Duration
	toolPrefix    string
}

// Option configures Connect.
type Option func(*config)

// WithClientInfo sets the name and version this client reports to the server in
// the initialize handshake.
func WithClientInfo(name, version string) Option {
	return func(c *config) { c.clientName, c.clientVersion = name, version }
}

// WithTimeout bounds the handshake and each individual tool call. Zero disables
// the timeout. Default: 30s.
func WithTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

// WithToolPrefix namespaces every tool name with prefix, both in what the model
// sees and in agent.ByName dispatch. Use it when wiring several MCP servers
// whose tool names could otherwise collide; the prefix is stripped before the
// call reaches the server.
func WithToolPrefix(prefix string) Option {
	return func(c *config) { c.toolPrefix = prefix }
}

// Connect opens a transport to server, performs the MCP handshake, and lists
// the server's tools. On any failure it tears down the transport and returns
// the error.
func Connect(ctx context.Context, server Server, opts ...Option) (*Client, error) {
	cfg := config{clientName: "goagent", clientVersion: "0.1", timeout: 30 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}

	transport, err := server.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("mcp: open transport: %w", err)
	}
	c := newConn(transport)

	hctx := ctx
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		hctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}

	info, err := initialize(hctx, c, cfg)
	if err != nil {
		c.Close()
		return nil, err
	}
	if err := c.notify(methodInitialized, struct{}{}); err != nil {
		c.Close()
		return nil, fmt.Errorf("mcp: initialized notification: %w", err)
	}

	descs, err := listTools(hctx, c)
	if err != nil {
		c.Close()
		return nil, err
	}

	client := &Client{conn: c, serverInfo: info.ServerInfo}
	client.tools = make([]tool.Tool, 0, len(descs))
	for _, d := range descs {
		client.tools = append(client.tools, newMCPTool(c, cfg, d))
	}
	return client, nil
}

// Tools returns the server's tools adapted as tool.Tool, ready to drop into
// agent.Config.Tools. The slice is computed once at Connect.
func (c *Client) Tools() []tool.Tool { return c.tools }

// ServerInfo returns the connected server's reported name and version.
func (c *Client) ServerInfo() (name, version string) {
	return c.serverInfo.Name, c.serverInfo.Version
}

// Close shuts down the connection and the server subprocess. Tool calls made
// after Close fail and report an error to the model.
func (c *Client) Close() error { return c.conn.Close() }

// initialize performs the initialize request and returns the server's reply.
func initialize(ctx context.Context, c *conn, cfg config) (*initializeResult, error) {
	params := initializeParams{
		ProtocolVersion: protocolVersion,
		ClientInfo:      implementation{Name: cfg.clientName, Version: cfg.clientVersion},
	}
	raw, err := c.call(ctx, methodInitialize, params)
	if err != nil {
		return nil, fmt.Errorf("mcp: initialize: %w", err)
	}
	var res initializeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("mcp: initialize result: %w", err)
	}
	return &res, nil
}

// listTools fetches every tool the server exposes, following nextCursor
// pagination to completion.
func listTools(ctx context.Context, c *conn) ([]toolDescriptor, error) {
	var all []toolDescriptor
	cursor := ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := c.call(ctx, methodToolsList, params)
		if err != nil {
			return nil, fmt.Errorf("mcp: tools/list: %w", err)
		}
		var res listToolsResult
		if err := json.Unmarshal(raw, &res); err != nil {
			return nil, fmt.Errorf("mcp: tools/list result: %w", err)
		}
		all = append(all, res.Tools...)
		if res.NextCursor == "" {
			return all, nil
		}
		cursor = res.NextCursor
	}
}
