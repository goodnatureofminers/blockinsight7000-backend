package ingester

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/workerpool"
	"go.uber.org/zap"
)

type historyBlockProcessor struct {
	workerCount int
	source      HistorySource
	blockWriter BlockWriter
	metrics     HistoryIngesterMetrics
	logger      *zap.Logger
	cancel      func()
}

func (p *historyBlockProcessor) SetCancel(cancel func()) {
	p.cancel = cancel
}

func (p *historyBlockProcessor) Process(ctx context.Context, heights []uint64) error {
	return workerpool.Process(ctx, p.workerCount, heights, p.processHeight, p.cancel)
}

func (p *historyBlockProcessor) processHeight(ctx context.Context, height uint64) (err error) {
	started := time.Now()
	defer func() {
		p.observeHeight(err, height, started)
	}()

	block, err := p.source.FetchBlock(ctx, height)
	if err != nil {
		if p.logger != nil {
			p.logger.Error("fetch block failed", zap.Uint64("height", height), zap.Error(err))
		}
		return fmt.Errorf("fetch block height %d: %w", height, err)
	}

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

func (p *historyBlockProcessor) observeHeight(err error, height uint64, started time.Time) {
	if p.metrics == nil {
		return
	}
	p.metrics.ObserveProcessHeight(err, height, started)
}
