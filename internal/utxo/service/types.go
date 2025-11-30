package service

import (
	"context"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

// ClickhouseRepository describes the persistence operations the ingesters need.
type ClickhouseRepository interface {
	RandomMissingBlockHeights(ctx context.Context, coin model.Coin, network model.Network, maxHeight, limit uint64) ([]uint64, error)
	MaxContiguousBlockHeight(ctx context.Context, coin model.Coin, network model.Network) (uint64, error)
	RandomUnprocessedBlockHeights(ctx context.Context, coin model.Coin, network model.Network, maxHeight, limit uint64) ([]uint64, error)
	InsertBlocks(ctx context.Context, blocks []model.Block) error
	InsertTransactions(ctx context.Context, txs []model.Transaction) error
	InsertTransactionOutputs(ctx context.Context, outputs []model.TransactionOutput) error
	InsertTransactionInputs(ctx context.Context, inputs []model.TransactionInput) error
}
