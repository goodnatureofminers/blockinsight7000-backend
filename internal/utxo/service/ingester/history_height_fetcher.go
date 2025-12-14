package ingester

import (
	"context"
	"sort"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

type historyHeightFetcher struct {
	repository ClickhouseRepository
	coin       model.Coin
	network    model.Network
	limit      uint64
}

func (f *historyHeightFetcher) Fetch(ctx context.Context) ([]uint64, error) {
	maxHeight, err := f.repository.MaxBlockHeight(ctx, f.coin, f.network)
	if err != nil {
		return nil, err
	}

	for chunkStart := uint64(0); chunkStart <= maxHeight; chunkStart += historyChunkSize {
		chunkEnd := chunkStart + historyChunkSize - 1
		if chunkEnd > maxHeight {
			chunkEnd = maxHeight
		}

		heights, err := f.repository.RandomBlockHeightsByStatus(ctx, f.coin, f.network, model.BlockNew, chunkEnd, f.limit)
		if err != nil {
			return nil, err
		}
		if len(heights) == 0 {
			continue
		}

		sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
		return heights, nil
	}

	return nil, nil
}
