package llm

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// StatusError is a provider HTTP error carrying the response status code, so
// resilience decorators (retry, fallback, circuit breaker) can classify
// failures without string-matching the message. Providers return it for
// non-2xx responses; its Error() string is kept stable for backward
// compatibility ("<provider>: status <code>: <body>"). Context-length
// overflows are returned as *ContextLengthError, which embeds this type, so
// new code should use errors.As rather than a direct type assertion.
type StatusError struct {
	Provider string
	Code     int
	Body     string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s: status %d: %s", e.Provider, e.Code, e.Body)
}

// Retryable reports whether the status is worth retrying or failing over to
// another provider: 408 (request timeout), 429 (rate limited), and any 5xx.
// Other 4xx codes are caller errors (bad request, auth, not found) that will
// fail identically elsewhere, so they are not retried.
func (e *StatusError) Retryable() bool {
	return e.Code == 408 || e.Code == 429 || e.Code >= 500
}

// ContextLengthError is a StatusError for a 400/413 response whose body
// reports that the request exceeded the provider's context window. It embeds
// the original StatusError, so Error() and errors.As(err, *StatusError) keep
// working, but overrides Retryable: resending the same request would fail
// identically. The agent loop's context-overflow recovery handles it instead.
type ContextLengthError struct {
	*StatusError
	// Limit and Requested are the provider-reported token budget and request
	// size. They stay zero when the response wording could not be parsed.
	Limit     int
	Requested int
}

func (e *ContextLengthError) Unwrap() error { return e.StatusError }

func (e *ContextLengthError) Retryable() bool { return false }

// NewStatusError builds the error for a provider HTTP failure. A 400 or 413
// whose body identifies a context-length overflow becomes a
// *ContextLengthError so callers can recover rather than retry; every other
// response stays a plain *StatusError. Classification happens here once, so
// callers never need to inspect a response body.
func NewStatusError(provider string, code int, body string) error {
	se := &StatusError{Provider: provider, Code: code, Body: body}
	if code != 400 && code != 413 {
		return se
	}
	limit, requested, matched := parseContextOverflow(body)
	if !matched {
		return se
	}
	return &ContextLengthError{StatusError: se, Limit: limit, Requested: requested}
}

// IsContextLength reports whether err carries a typed context-length overflow.
func IsContextLength(err error) bool {
	var cle *ContextLengthError
	return errors.As(err, &cle)
}

var (
	openAILimitPattern      = regexp.MustCompile(`maximum context length is\s+(\d+)\s+tokens`)
	openAIRequestedPattern  = regexp.MustCompile(`(?:resulted in|requested)\s+(\d+)\s+tokens`)
	anthropicTooLongPattern = regexp.MustCompile(`prompt is too long:\s*(\d+)\s*tokens\s*>\s*(\d+)\s*maximum`)
	anthropicExceedPattern  = regexp.MustCompile(`exceed context limit:\s*(\d+)\s*\+\s*(\d+)\s*>\s*(\d+)`)
)

// parseContextOverflow classifies a response body and extracts token counts
// where the provider uses a documented wording. Generic compatibility markers
// only classify: their numbers are not standardized, so they are left at zero
// rather than guessing from unrelated numbers in the body.
func parseContextOverflow(body string) (limit, requested int, matched bool) {
	lower := strings.ToLower(body)
	switch {
	case strings.Contains(lower, "prompt is too long") || strings.Contains(lower, "exceed context limit"):
		if m := anthropicTooLongPattern.FindStringSubmatch(lower); m != nil {
			requested = atoi(m[1])
			limit = atoi(m[2])
			return limit, requested, true
		}
		if m := anthropicExceedPattern.FindStringSubmatch(lower); m != nil {
			requested = atoi(m[1]) + atoi(m[2])
			limit = atoi(m[3])
			return limit, requested, true
		}
		return 0, 0, true
	case strings.Contains(lower, "context_length_exceeded") || strings.Contains(lower, "maximum context length"):
		if m := openAILimitPattern.FindStringSubmatch(lower); m != nil {
			limit = atoi(m[1])
		}
		if m := openAIRequestedPattern.FindStringSubmatch(lower); m != nil {
			requested = atoi(m[1])
		}
		return limit, requested, true
	case strings.Contains(lower, "too many tokens"),
		strings.Contains(lower, "input is too long"),
		strings.Contains(lower, "context window exceeded"),
		strings.Contains(lower, "request too large"):
		return 0, 0, true
	}
	return 0, 0, false
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// IsRetryable is the shared default classifier for the retry, fallback and
// circuit-breaker decorators. A *StatusError defers to its Retryable(); a
// cancelled or deadline-exceeded context is never retryable (the same context
// would fail again); every other error (network, decode, …) is treated as a
// transient failure worth retrying or failing over.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Checked before errors.As reaches the embedded *StatusError: a 429/5xx
	// encoded overflow still must not be resent, and its Retryable() would
	// otherwise report true.
	if IsContextLength(err) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Retryable()
	}
	return true
}
