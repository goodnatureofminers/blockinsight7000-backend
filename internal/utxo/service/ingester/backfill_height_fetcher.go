package ingester

import (
	"context"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

type backfillHeightFetcher struct {
	repository ClickhouseRepository
	coin       model.Coin
	network    model.Network
	limit      uint64
}

func (f *backfillHeightFetcher) Fetch(ctx context.Context) ([]uint64, error) {
	maxHeight, err := f.repository.MaxContiguousBlockHeight(ctx, f.coin, f.network)
	if err != nil {
		return nil, err
	}

	return f.repository.RandomUnprocessedBlockHeights(ctx, f.coin, f.network, maxHeight, f.limit)
}
