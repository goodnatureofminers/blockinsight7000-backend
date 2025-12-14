package ingester

import (
	"context"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

type (
	// HeightFetcher provides missing block heights to process.
	HeightFetcher interface {
		Fetch(ctx context.Context) ([]uint64, error)
	}
	// BlockProcessor processes batches of block heights.
	BlockProcessor interface {
		Process(ctx context.Context, heights []uint64) error
		SetCancel(cancel func())
	}
	// BlockWriter persists processed blocks and related data.
	BlockWriter interface {
		Start(ctx context.Context)
		Stop()
		WriteBlock(ctx context.Context, b model.InsertBlock) error
	}
	// BackfillIngesterMetrics captures metrics for the backfill ingester.
	BackfillIngesterMetrics interface {
		ObserveFetchMissing(err error, started time.Time)
		ObserveProcessBatch(err error, heights int, started time.Time)
		ObserveProcessHeight(err error, height uint64, started time.Time)
	}

	// HistoryIngesterMetrics captures metrics for the history ingester.
	HistoryIngesterMetrics interface {
		ObserveFetchMissing(err error, started time.Time)
		ObserveProcessBatch(err error, heights int, started time.Time)
		ObserveProcessHeight(err error, height uint64, started time.Time)
	}
	// FollowerIngesterMetrics captures metrics for the follower ingester.
	FollowerIngesterMetrics interface {
		ObserveFetchMissing(err error, started time.Time)
		ObserveProcessBatch(err error, heights int, started time.Time)
	}

	// HistorySource provides historical block data.
	HistorySource interface {
		LatestHeight(ctx context.Context) (uint64, error)
		FetchBlock(ctx context.Context, height uint64) (*chain.HistoryBlock, error)
	}
	// BackfillSource provides backfill block data.
	BackfillSource interface {
		LatestHeight(ctx context.Context) (uint64, error)
		FetchBlock(ctx context.Context, height uint64) (*chain.BackfillBlock, error)
	}
	// ClickhouseRepository persists blockchain entities to ClickHouse.
	ClickhouseRepository interface {
		MaxBlockHeight(ctx context.Context, coin model.Coin, network model.Network) (uint64, error)
		MaxContiguousBlockHeightByStatuses(ctx context.Context, coin model.Coin, network model.Network, statuses []model.BlockStatus) (uint64, error)
		RandomMissingBlockHeights(ctx context.Context, coin model.Coin, network model.Network, maxHeight, limit uint64) ([]uint64, error)
		RandomBlockHeightsByStatus(ctx context.Context, coin model.Coin, network model.Network, status model.BlockStatus, maxHeight, limit uint64) ([]uint64, error)
		RandomUnprocessedBlockHeights(ctx context.Context, coin model.Coin, network model.Network, maxHeight, limit uint64) ([]uint64, error)
		InsertBlocks(ctx context.Context, blocks []model.Block) error
		InsertTransactions(ctx context.Context, txs []model.Transaction) error
		InsertTransactionOutputs(ctx context.Context, outputs []model.TransactionOutput) error
		InsertTransactionInputs(ctx context.Context, inputs []model.TransactionInput) error
	}
)
