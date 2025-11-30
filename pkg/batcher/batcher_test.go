package batcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestBatcher_FlushOnSize(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushed atomic.Int32
	var batches [][]int
	var mu sync.Mutex

	b := New(zap.NewNop(), func(_ context.Context, items []int) error {
		mu.Lock()
		defer mu.Unlock()
		flushed.Add(int32(len(items)))
		// copy to avoid reuse
		cp := make([]int, len(items))
		copy(cp, items)
		batches = append(batches, cp)
		return nil
	}, 3, time.Second, 1000)

	b.Start(ctx)
	defer b.Stop()

	for i := 0; i < 5; i++ {
		if err := b.Add(ctx, i); err != nil {
			t.Fatalf("Add error: %v", err)
		}
	}
	// Wait a moment to allow background flush.
	time.Sleep(100 * time.Millisecond)

	if flushed.Load() != 3 {
		t.Fatalf("expected first flush of 3 items, got %d", flushed.Load())
	}
	mu.Lock()
	if len(batches) != 1 || len(batches[0]) != 3 {
		t.Fatalf("unexpected batches: %+v", batches)
	}
	mu.Unlock()
}

func TestBatcher_FlushOnInterval(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushed atomic.Int32

	b := New(zap.NewNop(), func(_ context.Context, items []int) error {
		flushed.Add(int32(len(items)))
		return nil
	}, 5, 50*time.Millisecond, 1000)

	b.Start(ctx)
	defer b.Stop()

	if err := b.Add(ctx, 1); err != nil {
		t.Fatalf("Add error: %v", err)
	}

	time.Sleep(120 * time.Millisecond)

	if flushed.Load() != 1 {
		t.Fatalf("expected flush after interval, got %d", flushed.Load())
	}
}

func TestBatcher_ContextCancelStops(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	b := New(zap.NewNop(), func(_ context.Context, items []int) error {
		return nil
	}, 2, time.Second, 1000)

	b.Start(ctx)
	cancel()                          // stop run loop via context
	time.Sleep(20 * time.Millisecond) // let goroutine exit
	b.Stop()                          // close stop channel

	// Add should still accept but will get canceled due to stop channel.
	err := b.Add(context.Background(), 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on stopped batcher, got %v", err)
	}
}

func TestBatcher_FlushErrorLoggedButContinues(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	b := New(zap.NewNop(), func(_ context.Context, items []int) error {
		calls.Add(1)
		if calls.Load() == 1 {
			return errors.New("flush failed")
		}
		return nil
	}, 1, time.Second, 1000)

	b.Start(ctx)
	defer b.Stop()

	if err := b.Add(ctx, 1); err != nil {
		t.Fatalf("Add error: %v", err)
	}
	if err := b.Add(ctx, 2); err != nil {
		t.Fatalf("Add error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if calls.Load() != 2 {
		t.Fatalf("expected two flush attempts, got %d", calls.Load())
	}
}
