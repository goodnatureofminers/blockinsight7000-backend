package ingester

import (
	"context"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

type historyHeightFetcher struct {
	source     HistorySource
	repository ClickhouseRepository
	coin       model.Coin
	network    model.Network
	limit      uint64
}

func (f *historyHeightFetcher) Fetch(ctx context.Context) ([]uint64, error) {
	latest, err := f.source.LatestHeight(ctx)
	if err != nil {
		return nil, err
	}
	return f.repository.RandomMissingBlockHeights(ctx, f.coin, f.network, latest, f.limit)
}
