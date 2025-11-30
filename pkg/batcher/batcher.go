// Package batcher provides a generic buffered batch processor with rate limiting.
package batcher

import (
	"context"
	"sync"
	"time"

	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

// Batcher buffers items and flushes them either by size or interval.
type Batcher[T any] struct {
	flushCallback func(context.Context, []T) error
	itemsCh       chan T
	flushSize     int
	flushInterval time.Duration
	rl            ratelimit.Limiter
	logger        *zap.Logger

	wg   sync.WaitGroup
	stop chan struct{}
}

// New constructs a Batcher.
func New[T any](logger *zap.Logger, flushCallback func(context.Context, []T) error, flushSize int, flushInterval time.Duration, rps int) *Batcher[T] {
	return &Batcher[T]{
		logger:        logger,
		flushCallback: flushCallback,
		itemsCh:       make(chan T, flushSize*2),
		flushSize:     flushSize,
		flushInterval: flushInterval,
		rl:            ratelimit.New(rps),
		stop:          make(chan struct{}),
	}
}

// Start begins the background flushing loop.
func (b *Batcher[T]) Start(ctx context.Context) {
	b.wg.Add(1)
	go b.run(ctx)
}

// Stop stops the background flushing loop.
func (b *Batcher[T]) Stop() {
	close(b.stop)
	b.wg.Wait()
}

// Add queues an item for batching, respecting context cancellation.
func (b *Batcher[T]) Add(ctx context.Context, item T) error {
	select {
	case <-b.stop:
		return context.Canceled
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.itemsCh <- item:
		return nil
	}
}

func (b *Batcher[T]) run(ctx context.Context) {
	defer b.wg.Done()

	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	buf := make([]T, 0, b.flushSize)

	flush := func() {
		if len(buf) == 0 {
			return
		}

		b.rl.Take()
		err := b.flushCallback(ctx, buf)
		if err != nil {
			b.logger.Error("batch not flushed", zap.Error(err))
		} else {
			b.logger.Debug("batch flushed", zap.Int("size", len(buf)))
		}
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return

		case <-b.stop:
			flush()
			return

		case item := <-b.itemsCh:
			buf = append(buf, item)
			if len(buf) >= b.flushSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}
