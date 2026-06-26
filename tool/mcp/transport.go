package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
)

// Transport is one bidirectional JSON-RPC message stream to an MCP server. Each
// Send/Receive carries exactly one JSON-RPC message. The built-in Stdio
// transport implements it over a subprocess; implement Transport yourself to
// add others (e.g. HTTP/SSE).
//
// Receive is called only from a single read loop, so it need not be safe for
// concurrent use; the client serializes Send internally.
type Transport interface {
	// Send writes one JSON-RPC message frame.
	Send(msg json.RawMessage) error
	// Receive reads the next message frame, blocking until one arrives. It
	// returns a non-nil error (e.g. io.EOF) once the stream is closed.
	Receive() (json.RawMessage, error)
	// Close releases the transport (terminating the subprocess for Stdio).
	Close() error
}

// Server opens a Transport to an MCP server. Stdio returns one; custom
// transports can implement it too. Connect calls Open once.
type Server interface {
	Open(ctx context.Context) (Transport, error)
}

// StdioServer launches an MCP server as a subprocess and speaks
// newline-delimited JSON-RPC over its stdin/stdout — the standard transport for
// local MCP servers. Build one with Stdio and configure it with the chainable
// methods.
type StdioServer struct {
	command string
	args    []string
	env     []string
	dir     string
	stderr  io.Writer
}

// Stdio describes an MCP server launched as `command args...`.
func Stdio(command string, args ...string) *StdioServer {
	return &StdioServer{command: command, args: args}
}

// Env sets the subprocess environment (as in os/exec; nil inherits none).
// Pass os.Environ() to inherit the parent's environment.
func (s *StdioServer) Env(env []string) *StdioServer { s.env = env; return s }

// Dir sets the subprocess working directory (empty uses the current one).
func (s *StdioServer) Dir(dir string) *StdioServer { s.dir = dir; return s }

// Stderr routes the subprocess's stderr (server logs) to w. Unset discards it.
func (s *StdioServer) Stderr(w io.Writer) *StdioServer { s.stderr = w; return s }

// Open starts the subprocess and returns a transport over its stdio. The
// process is not tied to ctx; it lives until the returned transport is Closed.
func (s *StdioServer) Open(_ context.Context) (Transport, error) {
	cmd := exec.Command(s.command, s.args...)
	cmd.Env = s.env
	cmd.Dir = s.dir
	cmd.Stderr = s.stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &stdioTransport{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		reader: bufio.NewReader(stdout),
	}, nil
}

// stdioTransport frames JSON-RPC messages as newline-delimited JSON over a
// subprocess's stdin/stdout. Marshaled messages are compact (no embedded
// newlines), so a trailing '\n' is a safe delimiter.
type stdioTransport struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	reader *bufio.Reader
}

func (t *stdioTransport) Send(msg json.RawMessage) error {
	if _, err := t.stdin.Write(msg); err != nil {
		return err
	}
	_, err := t.stdin.Write([]byte{'\n'})
	return err
}

func (t *stdioTransport) Receive() (json.RawMessage, error) {
	for {
		line, err := t.reader.ReadBytes('\n')
		line = bytes.TrimRight(line, "\r\n")
		if len(line) > 0 {
			return json.RawMessage(line), nil
		}
		if err != nil {
			return nil, err
		}
		// Blank line with no error: skip and keep reading.
	}
}

func (t *stdioTransport) Close() error {
	_ = t.stdin.Close()
	if t.cmd.Process != nil {
		_ = t.cmd.Process.Kill()
	}
	return t.cmd.Wait()
}
