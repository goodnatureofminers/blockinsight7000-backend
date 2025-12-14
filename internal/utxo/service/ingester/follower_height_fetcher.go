package ingester

import (
	"context"
	"sort"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

type followerHeightFetcher struct {
	source     HistorySource
	repository ClickhouseRepository
	coin       model.Coin
	network    model.Network
	limit      uint64
}

func (f *followerHeightFetcher) Fetch(ctx context.Context) ([]uint64, error) {
	latest, err := f.source.LatestHeight(ctx)
	if err != nil {
		return nil, err
	}

	heights, err := f.repository.RandomMissingBlockHeights(ctx, f.coin, f.network, latest, f.limit)
	if err != nil {
		return nil, err
	}

	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	return heights, nil
}
