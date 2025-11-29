package chain

import (
	"context"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// HistorySource provides block data required for historical ingestion (blocks, txs, outputs).
type HistorySource interface {
	LatestHeight(ctx context.Context) (uint64, error)
	FetchBlock(ctx context.Context, height uint64) (*HistoryBlock, error)
}

// BackfillSource provides block data required for backfill ingestion (blocks, inputs).
type BackfillSource interface {
	LatestHeight(ctx context.Context) (uint64, error)
	FetchBlock(ctx context.Context, height uint64) (*BackfillBlock, error)
}

type HistoryBlock struct {
	Block   model.Block
	Txs     []model.Transaction
	Outputs []model.TransactionOutput
}

type BackfillBlock struct {
	Block  model.Block
	Inputs []model.TransactionInput
}
