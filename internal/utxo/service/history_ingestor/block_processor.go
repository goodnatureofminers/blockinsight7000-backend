package history_ingestor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"go.uber.org/zap"
)

type blockProcessor struct {
	workerCount   int
	source        HistorySource
	blockWriter   BlockWriter
	metrics       HistoryIngesterMetrics
	logger        *zap.Logger
	cancelBatcher func()
}

func (p *blockProcessor) SetCancelBatcher(cancel func()) {
	p.cancelBatcher = cancel
}

func (p *blockProcessor) Process(ctx context.Context, heights []uint64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tasks := make(chan uint64, p.workerCount)
	errs := make(chan error, p.workerCount)
	wg := sync.WaitGroup{}
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case h, ok := <-tasks:
					if !ok {
						return
					}
					if err := p.processHeight(ctx, h); err != nil {
						select {
						case errs <- err:
						default:
						}
						if p.cancelBatcher != nil {
							p.cancelBatcher()
						}
						cancel()
						return
					}
				}
			}
		}()
	}

	go func() {
		for _, h := range heights {
			select {
			case <-ctx.Done():
				close(tasks)
				return
			case tasks <- h:
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

func (p *blockProcessor) processHeight(ctx context.Context, height uint64) error {
	started := time.Now()
	block, err := p.source.FetchBlock(ctx, height)
	if err != nil {
		p.observeHeight(err, height, started)
		if p.logger != nil {
			p.logger.Error("fetch block failed", zap.Uint64("height", height), zap.Error(err))
		}
		return fmt.Errorf("fetch block height %d: %w", height, err)
	}

	defer p.observeHeight(err, height, started)

	err = p.blockWriter.WriteBlock(ctx, model.InsertBlock{
		Block:   block.Block,
		Txs:     block.Txs,
		Outputs: block.Outputs,
	})
	if err != nil {
		if p.logger != nil {
			p.logger.Error("write block failed", zap.Uint64("height", height), zap.Error(err))
		}
		return fmt.Errorf("write block height %d: %w", height, err)
	}
	return nil
}

func (p *blockProcessor) observeHeight(err error, height uint64, started time.Time) {
	if p.metrics == nil {
		return
	}
	p.metrics.ObserveProcessHeight(err, height, started)
}
