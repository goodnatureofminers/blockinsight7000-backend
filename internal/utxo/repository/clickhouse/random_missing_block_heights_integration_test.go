package clickhouse

import (
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestRandomMissingBlockHeights() {
	now := time.Now().UTC().Truncate(time.Second)
	blocks := []model.Block{
		newBlock(model.BlockUnprocessed, 0, "a", now),
		newBlock(model.BlockProcessed, 1, "b", now.Add(time.Second)),
		newBlock(model.BlockProcessed, 2, "c", now.Add(2*time.Second)),
		newBlock(model.BlockUnprocessed, 4, "d", now.Add(3*time.Second)),
	}
	s.seedBlocks(blocks)

	s.metrics.EXPECT().Observe("random_missing_block_heights", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	missing, err := s.repo.RandomMissingBlockHeights(s.testCtx, model.BTC, model.Mainnet, 5, 2)
	s.Require().NoError(err)
	s.ElementsMatch([]uint64{3, 5}, missing)
}

func (s *RepositorySuite) TestRandomMissingBlockHeightsAllPresent() {
	now := time.Now().UTC().Truncate(time.Second)
	blocks := []model.Block{
		newBlock(model.BlockProcessed, 0, "a", now),
		newBlock(model.BlockProcessed, 1, "b", now.Add(time.Second)),
		newBlock(model.BlockProcessed, 2, "c", now.Add(2*time.Second)),
		newBlock(model.BlockProcessed, 3, "d", now.Add(3*time.Second)),
	}
	s.seedBlocks(blocks)

	s.metrics.EXPECT().Observe("random_missing_block_heights", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	missing, err := s.repo.RandomMissingBlockHeights(s.testCtx, model.BTC, model.Mainnet, 3, 5)
	s.Require().NoError(err)
	s.Empty(missing)
}
