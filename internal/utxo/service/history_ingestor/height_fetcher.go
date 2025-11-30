package history_ingestor

import (
	"context"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

type heightFetcher struct {
	source     HistorySource
	repository ClickhouseRepository
	coin       model.Coin
	network    model.Network
	limit      uint64
}

func (f *heightFetcher) FetchMissing(ctx context.Context) ([]uint64, error) {
	latest, err := f.source.LatestHeight(ctx)
	if err != nil {
		return nil, err
	}
	return f.repository.RandomMissingBlockHeights(ctx, f.coin, f.network, latest, f.limit)
}
