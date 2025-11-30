package workerpool

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
)

func TestProcess(t *testing.T) {
	type args[T any] struct {
		ctx         context.Context
		workerCount int
		items       []T
	}
	type testCase[T any] struct {
		name         string
		args         args[T]
		wantErr      bool
		expectCancel bool
	}
	tests := []testCase[int]{
		{
			name: "success processes all items",
			args: args[int]{
				ctx:         context.Background(),
				workerCount: 2,
				items:       []int{1, 2, 3, 4},
			},
		},
		{
			name: "error cancels workers and calls onCancel",
			args: args[int]{
				ctx:         context.Background(),
				workerCount: 3,
				items:       []int{1, 2, 3},
			},
			wantErr:      true,
			expectCancel: true,
		},
		{
			name: "context canceled returns canceled error",
			args: args[int]{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				}(),
				workerCount: 2,
				items:       []int{1, 2},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var processed int32
			var canceled int32

			// Bind per-test functions
			process := func(_ context.Context, v int) error {
				if tt.name == "error cancels workers and calls onCancel" && v == 2 {
					return errors.New("boom")
				}
				atomic.AddInt32(&processed, safeIntToInt32(t, v))
				return nil
			}
			onCancel := func() {
				atomic.AddInt32(&canceled, 1)
			}

			err := Process(tt.args.ctx, tt.args.workerCount, tt.args.items, process, onCancel)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Process() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.expectCancel && canceled == 0 {
				t.Fatalf("expected onCancel to be invoked")
			}
			if !tt.expectCancel && canceled != 0 {
				t.Fatalf("unexpected onCancel invocation")
			}

			switch tt.name {
			case "success processes all items":
				if processed != 10 { // 1+2+3+4
					t.Fatalf("expected processed sum 10, got %d", processed)
				}
			case "error cancels workers and calls onCancel":
				if processed != 1 && processed != 4 { // depends on scheduling, but should not process all items
					t.Fatalf("unexpected processed value: %d", processed)
				}
			case "context canceled returns canceled error":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context.Canceled, got %v", err)
				}
			}
		})
	}
}

func safeIntToInt32(t *testing.T, v int) int32 {
	if v < 0 || v > math.MaxInt32 {
		t.Fatalf("value %d outside int32 range", v)
	}
	return int32(v)
}
