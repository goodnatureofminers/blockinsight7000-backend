package history_ingestor

import (
	"context"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/batcher"
	"go.uber.org/zap"
)

const (
	blockBatcherCapacity      = 500
	blockBatcherFlushInterval = 30 * time.Second
	blockBatcherWorkerCount   = 1
)

type blockWriter struct {
	repo         ClickhouseRepository
	logger       *zap.Logger
	blockBatcher *batcher.Batcher[model.InsertBlock]
}

func newBlockWriter(repo ClickhouseRepository, logger *zap.Logger) *blockWriter {
	w := &blockWriter{
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

func (w *blockWriter) Start(ctx context.Context) {
	w.blockBatcher.Start(ctx)
}

func (w *blockWriter) Stop() {
	w.blockBatcher.Stop()
}

func (w *blockWriter) WriteBlock(ctx context.Context, b model.InsertBlock) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return w.blockBatcher.Add(ctx, b)
}

func (w *blockWriter) flush(ctx context.Context, insertBlocks []model.InsertBlock) error {
	blocks := make([]model.Block, 0, len(insertBlocks))
	txs := make([]model.Transaction, 0, len(insertBlocks))
	outputs := make([]model.TransactionOutput, 0, len(insertBlocks))

	for _, block := range insertBlocks {
		blocks = append(blocks, block.Block)
		txs = append(txs, block.Txs...)
		if len(txs) >= transactionFlushThreshold {
			if err := w.repo.InsertTransactions(ctx, txs); err != nil {
				return err
			}
			w.logger.Debug("InsertTransactions", zap.Int("count", len(txs)))
			txs = txs[:0]
		}

		outputs = append(outputs, block.Outputs...)
		if len(outputs) >= outputFlushThreshold {
			if err := w.repo.InsertTransactionOutputs(ctx, outputs); err != nil {
				return err
			}
			w.logger.Debug("InsertTransactionOutputs", zap.Int("count", len(outputs)))
			outputs = outputs[:0]
		}
	}

	if err := w.repo.InsertTransactions(ctx, txs); err != nil {
		return err
	}
	if err := w.repo.InsertTransactionOutputs(ctx, outputs); err != nil {
		return err
	}

	return w.repo.InsertBlocks(ctx, blocks)
}
