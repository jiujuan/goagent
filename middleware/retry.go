package middleware

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/jiujuan/goagent/llm"
)

// RetryOptions configures the Retry middleware.
type RetryOptions struct {
	// MaxAttempts is the total number of attempts (default 3).
	MaxAttempts int
	// BaseDelay is the first backoff delay (default 200ms); it doubles each
	// attempt up to MaxDelay.
	BaseDelay time.Duration
	// MaxDelay caps the backoff (default 10s).
	MaxDelay time.Duration
	// Retryable decides whether an error is worth retrying (default: any
	// non-context error).
	Retryable func(error) bool
}

// Retry retries a failed model call with exponential backoff. To keep streaming
// safe, it only retries when the error occurs BEFORE any response has been
// yielded; once partial output has streamed, the error is propagated (a retry
// would duplicate already-delivered content).
func Retry(opts *RetryOptions) Middleware {
	max := 3
	base := 200 * time.Millisecond
	maxDelay := 10 * time.Second
	retryable := defaultRetryable
	if opts != nil {
		if opts.MaxAttempts > 0 {
			max = opts.MaxAttempts
		}
		if opts.BaseDelay > 0 {
			base = opts.BaseDelay
		}
		if opts.MaxDelay > 0 {
			maxDelay = opts.MaxDelay
		}
		if opts.Retryable != nil {
			retryable = opts.Retryable
		}
	}

	return func(next llm.Model) llm.Model {
		return Wrap(next, func(ctx context.Context, req *llm.Request) iter.Seq2[*llm.Response, error] {
			return func(yield func(*llm.Response, error) bool) {
				delay := base
				var lastErr error
				for attempt := 0; attempt < max; attempt++ {
					if attempt > 0 {
						if !sleep(ctx, delay) {
							yield(nil, ctx.Err())
							return
						}
						delay = min(delay*2, maxDelay)
					}

					yielded := false
					failed := false
					for resp, err := range next.Generate(ctx, req) {
						if err != nil {
							if !yielded && retryable(err) {
								lastErr = err
								failed = true
								break // retry from scratch
							}
							// Already streamed, or non-retryable: propagate.
							yield(resp, err)
							return
						}
						yielded = true
						if !yield(resp, nil) {
							return
						}
					}
					if !failed {
						return // success
					}
				}
				yield(nil, lastErr)
			}
		})
	}
}

// sleep waits for d or until ctx is done; returns false if ctx was cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// defaultRetryable retries any error except context cancellation/deadline.
func defaultRetryable(err error) bool {
	return err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}
