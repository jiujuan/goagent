package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
)

// errClosed is returned by call when the connection has been shut down (by
// Close or because the transport reported an error).
var errClosed = errors.New("mcp: connection closed")

// rpcRequest is an outgoing JSON-RPC 2.0 message. A nil ID (omitted on the
// wire) makes it a notification, which expects no response.
type rpcRequest struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// rpcMessage is an incoming JSON-RPC 2.0 message. Responses carry an ID with
// either Result or Error set; server-initiated requests/notifications (which
// this client does not service) are recognized and ignored.
type rpcMessage struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

// rpcError is the JSON-RPC error object.
type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("mcp rpc error %d: %s", e.Code, e.Message)
}

// rpcResult is delivered to a waiting caller by the read loop.
type rpcResult struct {
	result json.RawMessage
	err    *rpcError
}

// conn multiplexes JSON-RPC requests over a single Transport. A background read
// loop dispatches each response to the call waiting on its id, so call is safe
// for concurrent use — the agent's turn engine invokes tools concurrently.
type conn struct {
	t       Transport
	writeMu sync.Mutex // serializes Transport.Send

	mu      sync.Mutex
	nextID  int64
	pending map[string]chan rpcResult
	closed  chan struct{}
	once    sync.Once
}

// newConn wraps a transport and starts its read loop.
func newConn(t Transport) *conn {
	c := &conn{
		t:       t,
		pending: map[string]chan rpcResult{},
		closed:  make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// readLoop reads messages until the transport errors, routing each response to
// its pending caller by id and discarding anything else.
func (c *conn) readLoop() {
	for {
		raw, err := c.t.Receive()
		if err != nil {
			c.shutdown()
			return
		}
		var msg rpcMessage
		if json.Unmarshal(raw, &msg) != nil || len(msg.ID) == 0 {
			// Malformed, or a server-initiated request/notification we do not
			// service. Ignore and keep reading.
			continue
		}
		key := string(msg.ID)
		c.mu.Lock()
		ch := c.pending[key]
		delete(c.pending, key)
		c.mu.Unlock()
		if ch != nil {
			ch <- rpcResult{result: msg.Result, err: msg.Error}
		}
	}
}

// call sends a request and waits for its response, honoring ctx cancellation
// and connection shutdown.
func (c *conn) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := atomic.AddInt64(&c.nextID, 1)
	idRaw := json.RawMessage(strconv.FormatInt(id, 10))
	key := string(idRaw)

	req := rpcRequest{Version: "2.0", ID: idRaw, Method: method}
	if params != nil {
		p, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		req.Params = p
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	ch := make(chan rpcResult, 1)
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return nil, errClosed
	default:
	}
	c.pending[key] = ch
	c.mu.Unlock()

	if err := c.send(body); err != nil {
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
		return nil, ctx.Err()
	case <-c.closed:
		return nil, errClosed
	case res := <-ch:
		if res.err != nil {
			return nil, res.err
		}
		return res.result, nil
	}
}

// notify sends a notification (a request without an id), which expects no
// response.
func (c *conn) notify(method string, params any) error {
	req := rpcRequest{Version: "2.0", Method: method}
	if params != nil {
		p, err := json.Marshal(params)
		if err != nil {
			return err
		}
		req.Params = p
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return c.send(body)
}

// send writes one framed message, serialized against concurrent callers.
func (c *conn) send(body []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.t.Send(body)
}

// shutdown marks the connection closed and unblocks every waiting call. It is
// idempotent: both Close and a read-loop error route through here.
func (c *conn) shutdown() {
	c.once.Do(func() { close(c.closed) })
}

// Close shuts down the connection and the underlying transport.
func (c *conn) Close() error {
	c.shutdown()
	return c.t.Close()
}
