package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestProcess(t *testing.T) {
	type args[T any] struct {
		ctx         context.Context
		workerCount int
		items       []T
		process     func(context.Context, T) error
		onCancel    func()
	}
	type testCase[T any] struct {
		name          string
		args          args[T]
		wantErr       bool
		expectCancel  bool
		expectHandled func(t *testing.T)
	}
	tests := []testCase[int]{
		{
			name: "success processes all items",
			args: args[int]{
				ctx:         context.Background(),
				workerCount: 2,
				items:       []int{1, 2, 3, 4},
			},
			expectHandled: func(t *testing.T) {
				// verified via closure below
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
			process := func(ctx context.Context, v int) error {
				switch tt.name {
				case "error cancels workers and calls onCancel":
					if v == 2 {
						return errors.New("boom")
					}
				}
				atomic.AddInt32(&processed, int32(v))
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
