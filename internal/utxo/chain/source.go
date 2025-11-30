// Package chain defines interfaces and structs shared between UTXO ingestion components.
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

// HistoryBlock wraps a block and its transactions/outputs fetched from a history source.
type HistoryBlock struct {
	Block   model.Block
	Txs     []model.Transaction
	Outputs []model.TransactionOutput
}

// BackfillBlock wraps a block and its resolved inputs fetched from a backfill source.
type BackfillBlock struct {
	Block  model.Block
	Inputs []model.TransactionInput
}
