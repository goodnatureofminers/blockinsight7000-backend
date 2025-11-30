package history_ingestor

import (
	"context"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

type (
	HeightFetcher interface {
		FetchMissing(ctx context.Context) ([]uint64, error)
	}
	BlockProcessor interface {
		Process(ctx context.Context, heights []uint64) error
		SetCancelBatcher(cancel func())
	}
	BlockWriter interface {
		Start(ctx context.Context)
		Stop()
		WriteBlock(ctx context.Context, b model.InsertBlock) error
	}

	HistoryIngesterMetrics interface {
		ObserveFetchMissing(err error, started time.Time)
		ObserveProcessBatch(err error, heights int, started time.Time)
		ObserveProcessHeight(err error, height uint64, started time.Time)
	}

	HistorySource interface {
		LatestHeight(ctx context.Context) (uint64, error)
		FetchBlock(ctx context.Context, height uint64) (*chain.HistoryBlock, error)
	}
	ClickhouseRepository interface {
		RandomMissingBlockHeights(ctx context.Context, coin model.Coin, network model.Network, maxHeight, limit uint64) ([]uint64, error)
		InsertBlocks(ctx context.Context, blocks []model.Block) error
		InsertTransactions(ctx context.Context, txs []model.Transaction) error
		InsertTransactionOutputs(ctx context.Context, outputs []model.TransactionOutput) error
	}
)
