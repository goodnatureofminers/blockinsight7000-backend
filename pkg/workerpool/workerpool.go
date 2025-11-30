// Package workerpool provides simple concurrent processing utilities.
package workerpool

import (
	"context"
	"sync"
)

// Process runs a worker pool over the provided work items, invoking process for each.
// If process returns an error, the pool cancels the context and stops further work.
func Process[T any](
	ctx context.Context,
	workerCount int,
	items []T,
	process func(context.Context, T) error,
	onCancel func(),
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tasks := make(chan T, workerCount)
	errs := make(chan error, workerCount)
	wg := sync.WaitGroup{}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-tasks:
					if !ok {
						return
					}
					if err := process(ctx, item); err != nil {
						select {
						case errs <- err:
						default:
						}
						if onCancel != nil {
							onCancel()
						}
						cancel()
						return
					}
				}
			}
		}()
	}

	go func() {
		for _, item := range items {
			select {
			case <-ctx.Done():
				close(tasks)
				return
			case tasks <- item:
			}
		}
		close(tasks)
	}()

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return nil
}
