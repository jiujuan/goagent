package agent

import "context"

// newRunCleanupContext preserves execution values while giving cleanup its own
// bounded lifetime. Create it once at the cleanup entry point and pass it down.
func newRunCleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(parent), writeGuardReleaseTimeout)
}
