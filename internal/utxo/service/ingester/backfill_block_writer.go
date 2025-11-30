package ingester

import (
	"context"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/batcher"
	"go.uber.org/zap"
)

type backfillBlockWriter struct {
	repo         ClickhouseRepository
	logger       *zap.Logger
	blockBatcher *batcher.Batcher[model.InsertBlock]
}

func newBackfillBlockWriter(repo ClickhouseRepository, logger *zap.Logger) *backfillBlockWriter {
	w := &backfillBlockWriter{
		repo:   repo,
		logger: logger,
	}

	w.blockBatcher = batcher.New[model.InsertBlock](
		logger.Named("blockBatcher"),
		w.flush,
		blockBatcherCapacity,
		blockBatcherFlushInterval,
		blockBatcherWorkerCount,
	)
	return w
}

func (w *backfillBlockWriter) Start(ctx context.Context) {
	w.blockBatcher.Start(ctx)
}

func (w *backfillBlockWriter) Stop() {
	w.blockBatcher.Stop()
}

func (w *backfillBlockWriter) WriteBlock(ctx context.Context, b model.InsertBlock) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return w.blockBatcher.Add(ctx, b)
}

func (w *backfillBlockWriter) flush(ctx context.Context, insertBlocks []model.InsertBlock) error {
	blocks := make([]model.Block, 0, len(insertBlocks))
	inputs := make([]model.TransactionInput, 0, len(insertBlocks))

	for _, block := range insertBlocks {
		blocks = append(blocks, block.Block)
		inputs = append(inputs, block.Inputs...)
		if len(inputs) >= inputFlushThreshold {
			if err := w.repo.InsertTransactionInputs(ctx, inputs); err != nil {
				return err
			}
			w.logger.Debug("InsertTransactionInputs", zap.Int("count", len(inputs)))
			inputs = inputs[:0]
		}
	}

	if err := w.repo.InsertTransactionInputs(ctx, inputs); err != nil {
		return err
	}

	return w.repo.InsertBlocks(ctx, blocks)
}
