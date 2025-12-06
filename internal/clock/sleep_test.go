package clock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSleepWithContext(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T) (context.Context, time.Duration)
		wantErr   error
		expectMin time.Duration
		expectMax time.Duration
	}{
		{
			name: "waits for duration when context active",
			setup: func(_ *testing.T) (context.Context, time.Duration) {
				return context.Background(), 15 * time.Millisecond
			},
			wantErr:   nil,
			expectMin: 15 * time.Millisecond,
		},
		{
			name: "returns when context canceled",
			setup: func(t *testing.T) (context.Context, time.Duration) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				time.AfterFunc(5*time.Millisecond, cancel)
				return ctx, 200 * time.Millisecond
			},
			wantErr:   context.Canceled,
			expectMax: 60 * time.Millisecond,
		},
		{
			name: "honors deadline exceeded",
			setup: func(t *testing.T) (context.Context, time.Duration) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				t.Cleanup(cancel)
				return ctx, 200 * time.Millisecond
			},
			wantErr:   context.DeadlineExceeded,
			expectMax: 60 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, duration := tt.setup(t)

			start := time.Now()
			err := SleepWithContext(ctx, duration)
			elapsed := time.Since(start)

			if tt.wantErr == nil && err != nil {
				t.Fatalf("SleepWithContext() unexpected error: %v", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("SleepWithContext() error = %v, want %v", err, tt.wantErr)
			}

			if tt.expectMin > 0 && elapsed < tt.expectMin {
				t.Fatalf("SleepWithContext() returned too early: elapsed %v, expected at least %v", elapsed, tt.expectMin)
			}
			if tt.expectMax > 0 && elapsed > tt.expectMax {
				t.Fatalf("SleepWithContext() returned too late: elapsed %v, expected under %v", elapsed, tt.expectMax)
			}
		})
	}
}
