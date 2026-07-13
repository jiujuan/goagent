package middleware

import (
	"context"
	"errors"
	"iter"
	"sync"
	"time"

	"github.com/jiujuan/goagent/llm"
)

// CircuitState is the breaker's lifecycle state.
type CircuitState int

const (
	StateClosed   CircuitState = iota // normal: calls pass through
	StateOpen                         // tripped: calls fail fast
	StateHalfOpen                     // probing: a single call is allowed through
)

func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned (without invoking the wrapped model) while the
// breaker is open. llm.IsRetryable treats it as retryable, so a FallbackModel
// wrapping the breaker fails over to a backup immediately.
var ErrCircuitOpen = errors.New("llm: circuit breaker open")

// CircuitOptions configures CircuitBreaker.
type CircuitOptions struct {
	// FailureThreshold is the number of consecutive failures that trips the
	// breaker from closed to open (default 5).
	FailureThreshold int
	// OpenTimeout is how long the breaker stays open before allowing a
	// half-open probe (default 30s).
	OpenTimeout time.Duration
	// SuccessThreshold is the number of consecutive successful probes in
	// half-open required to close the breaker (default 1).
	SuccessThreshold int
	// IsFailure classifies which errors count against the breaker's health.
	// Defaults to llm.IsRetryable, so client errors (4xx) and cancellation do
	// not trip the breaker — only genuine provider unhealth does.
	IsFailure func(error) bool
	// OnStateChange, if set, is called on every state transition (outside the
	// internal lock).
	OnStateChange func(from, to CircuitState)
	// Now is the clock, injectable for tests (default time.Now).
	Now func() time.Time
}

// CircuitBreaker wraps a single llm.Model with a classic three-state breaker.
// It is a MODEL DECORATOR (like RetryModel). When the wrapped model fails
// repeatedly the breaker opens and subsequent calls fail fast with
// ErrCircuitOpen — sparing the caller repeated timeouts and shielding a sick
// provider from load. After OpenTimeout it half-opens to probe recovery.
//
// Layer it inside FallbackModel and around RetryModel:
//
//	primary := middleware.CircuitBreaker(
//	    middleware.RetryModel(real, middleware.RetryOptions{MaxAttempts: 2}),
//	    middleware.CircuitOptions{FailureThreshold: 5, OpenTimeout: 30 * time.Second})
//	model := middleware.FallbackModel(middleware.FallbackOptions{}, primary, backup)
func CircuitBreaker(m llm.Model, o CircuitOptions) llm.Model {
	if o.FailureThreshold < 1 {
		o.FailureThreshold = 5
	}
	if o.OpenTimeout <= 0 {
		o.OpenTimeout = 30 * time.Second
	}
	if o.SuccessThreshold < 1 {
		o.SuccessThreshold = 1
	}
	if o.IsFailure == nil {
		o.IsFailure = llm.IsRetryable
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &circuit{inner: m, opts: o}
}

type circuit struct {
	inner llm.Model
	opts  CircuitOptions

	mu            sync.Mutex
	state         CircuitState
	consecFail    int
	consecSuccess int
	openedAt      time.Time
	probing       bool // a half-open probe is in flight
}

func (c *circuit) Name() string { return c.inner.Name() }

func (c *circuit) Generate(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
	return func(yield func(*llm.Response, error) bool) {
		if !c.allow() {
			yield(nil, ErrCircuitOpen)
			return
		}
		var termErr error
		for resp, err := range c.inner.Generate(ctx, req) {
			if err != nil {
				termErr = err
				yield(nil, err)
				break
			}
			if !yield(resp, nil) {
				c.record(nil) // consumer stopped after good output: a success
				return
			}
		}
		c.record(termErr)
	}
}

// allow decides admission and performs the open→half-open transition when the
// cooldown has elapsed. It returns false when the call must fail fast.
func (c *circuit) allow() bool {
	c.mu.Lock()
	var change *transition
	defer func() {
		c.mu.Unlock()
		change.fire(c.opts.OnStateChange)
	}()

	switch c.state {
	case StateClosed:
		return true
	case StateOpen:
		if c.opts.Now().Sub(c.openedAt) >= c.opts.OpenTimeout {
			change = c.set(StateHalfOpen)
			c.consecSuccess = 0
			c.probing = true
			return true
		}
		return false
	case StateHalfOpen:
		if c.probing {
			return false // only one probe at a time
		}
		c.probing = true
		return true
	}
	return true
}

// record folds a call's outcome into the breaker's state. err == nil is a
// success; a non-nil err counts as a failure only if IsFailure says so —
// otherwise it is inconclusive and leaves the health counters untouched.
func (c *circuit) record(err error) {
	c.mu.Lock()
	var change *transition
	defer func() {
		c.mu.Unlock()
		change.fire(c.opts.OnStateChange)
	}()

	success := err == nil
	failure := !success && c.opts.IsFailure(err)

	switch c.state {
	case StateClosed:
		switch {
		case success:
			c.consecFail = 0
		case failure:
			c.consecFail++
			if c.consecFail >= c.opts.FailureThreshold {
				change = c.open()
			}
		}
	case StateHalfOpen:
		c.probing = false
		switch {
		case success:
			c.consecSuccess++
			if c.consecSuccess >= c.opts.SuccessThreshold {
				change = c.close()
			}
		case failure:
			change = c.open()
		}
		// inconclusive (non-failure error): stay half-open, allow another probe.
	case StateOpen:
		// Calls are denied while open; nothing to record.
	}
}

func (c *circuit) open() *transition {
	t := c.set(StateOpen)
	c.openedAt = c.opts.Now()
	c.consecFail = 0
	c.consecSuccess = 0
	c.probing = false
	return t
}

func (c *circuit) close() *transition {
	t := c.set(StateClosed)
	c.consecFail = 0
	c.consecSuccess = 0
	c.probing = false
	return t
}

// set records a pending state transition (fired after the lock is released).
func (c *circuit) set(to CircuitState) *transition {
	if c.state == to {
		return nil
	}
	from := c.state
	c.state = to
	return &transition{from: from, to: to}
}

type transition struct{ from, to CircuitState }

func (t *transition) fire(cb func(from, to CircuitState)) {
	if t != nil && cb != nil {
		cb(t.from, t.to)
	}
}
