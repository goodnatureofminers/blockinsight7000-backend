package clickhouse

import (
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestMaxContiguousBlockHeightByStatus() {
	now := time.Now().UTC().Truncate(time.Second)
	blocks := []model.Block{
		newBlock(model.BlockProcessed, 0, "a", now),
		newBlock(model.BlockProcessed, 1, "b", now.Add(time.Second)),
		newBlock(model.BlockUnprocessed, 2, "c", now.Add(2*time.Second)),
		newBlock(model.BlockNew, 3, "d", now.Add(3*time.Second)),
		newBlock(model.BlockNew, 5, "e", now.Add(4*time.Second)),
	}
	s.seedBlocks(blocks)

	s.metrics.EXPECT().Observe("max_contiguous_block_height_by_status", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	height, err := s.repo.MaxContiguousBlockHeightByStatuses(s.testCtx, model.BTC, model.Mainnet, []model.BlockStatus{model.BlockUnprocessed, model.BlockProcessed})
	s.Require().NoError(err)
	s.Equal(uint64(2), height)
}
