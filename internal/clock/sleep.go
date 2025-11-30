// Package clock provides helpers for time-related operations.
package clock

import (
	"context"
	"time"
)

// SleepWithContext waits for the duration or returns early if the context is canceled.
func SleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
