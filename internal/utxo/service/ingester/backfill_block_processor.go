// Package ingester contains services for blockchain ingestion.
package ingester

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/workerpool"
	"go.uber.org/zap"
)

type backfillBlockProcessor struct {
	workerCount int
	source      BackfillSource
	blockWriter BlockWriter
	metrics     BackfillIngesterMetrics
	logger      *zap.Logger
	cancel      func()
}

func (p *backfillBlockProcessor) SetCancel(cancel func()) {
	p.cancel = cancel
}

func (p *backfillBlockProcessor) Process(ctx context.Context, heights []uint64) error {
	return workerpool.Process(ctx, p.workerCount, heights, p.processHeight, p.cancel)
}

func (p *backfillBlockProcessor) processHeight(ctx context.Context, height uint64) (err error) {
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

	if err = p.blockWriter.WriteBlock(ctx, model.InsertBlock{
		Block:  block.Block,
		Inputs: block.Inputs,
	}); err != nil {
		if p.logger != nil {
			p.logger.Error("write block failed", zap.Uint64("height", height), zap.Error(err))
		}
		return fmt.Errorf("write block height %d: %w", height, err)
	}

	return nil
}

func (p *backfillBlockProcessor) observeHeight(err error, height uint64, started time.Time) {
	if p.metrics == nil {
		return
	}
	p.metrics.ObserveProcessHeight(err, height, started)
}
