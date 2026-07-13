package llm

import (
	"context"
	"errors"
	"fmt"
)

// StatusError is a provider HTTP error carrying the response status code, so
// resilience decorators (retry, fallback, circuit breaker) can classify
// failures without string-matching the message. Providers return it for
// non-2xx responses; its Error() string is kept stable for backward
// compatibility ("<provider>: status <code>: <body>").
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

// IsRetryable is the shared default classifier for the retry, fallback and
// circuit-breaker decorators. A *StatusError defers to its Retryable(); a
// cancelled or deadline-exceeded context is never retryable (the same context
// would fail again); every other error (network, decode, …) is treated as a
// transient failure worth retrying or failing over.
func IsRetryable(err error) bool {
	if err == nil {
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
