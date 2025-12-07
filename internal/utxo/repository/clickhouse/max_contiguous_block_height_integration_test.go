package clickhouse

import (
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestMaxContiguousBlockHeight() {
	now := time.Now().UTC().Truncate(time.Second)
	blocks := []model.Block{
		newBlock(model.BlockUnprocessed, 0, "a", now),
		newBlock(model.BlockProcessed, 1, "b", now.Add(time.Second)),
		newBlock(model.BlockProcessed, 2, "c", now.Add(2*time.Second)),
		newBlock(model.BlockUnprocessed, 4, "d", now.Add(3*time.Second)),
	}
	s.seedBlocks(blocks)

	s.metrics.EXPECT().Observe("max_contiguous_block_height", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	height, err := s.repo.MaxContiguousBlockHeight(s.testCtx, model.BTC, model.Mainnet)
	s.Require().NoError(err)
	s.Equal(uint64(2), height)
}
