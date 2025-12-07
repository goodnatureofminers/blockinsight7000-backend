package clickhouse

import (
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestRandomUnprocessedBlockHeights() {
	now := time.Now().UTC().Truncate(time.Second)
	blocks := []model.Block{
		newBlock(model.BlockUnprocessed, 0, "a", now),
		newBlock(model.BlockProcessed, 1, "b", now.Add(time.Second)),
		newBlock(model.BlockProcessed, 2, "c", now.Add(2*time.Second)),
		newBlock(model.BlockUnprocessed, 4, "d", now.Add(3*time.Second)),
	}
	s.seedBlocks(blocks)

	s.metrics.EXPECT().Observe("random_unprocessed_block_heights", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	unprocessed, err := s.repo.RandomUnprocessedBlockHeights(s.testCtx, model.BTC, model.Mainnet, 5, 5)
	s.Require().NoError(err)
	s.ElementsMatch([]uint64{0, 4}, unprocessed)
}

func (s *RepositorySuite) TestRandomUnprocessedBlockHeightsWithLowMaxHeight() {
	now := time.Now().UTC().Truncate(time.Second)
	blocks := []model.Block{
		newBlock(model.BlockUnprocessed, 5, "a", now),
		newBlock(model.BlockProcessed, 6, "b", now.Add(time.Second)),
	}
	s.seedBlocks(blocks)

	s.metrics.EXPECT().Observe("random_unprocessed_block_heights", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	unprocessed, err := s.repo.RandomUnprocessedBlockHeights(s.testCtx, model.BTC, model.Mainnet, 1, 5)
	s.Require().NoError(err)
	s.Empty(unprocessed)
}

func (s *RepositorySuite) TestRandomUnprocessedBlockHeightsAllProcessed() {
	now := time.Now().UTC().Truncate(time.Second)
	blocks := []model.Block{
		newBlock(model.BlockProcessed, 0, "a", now),
		newBlock(model.BlockProcessed, 1, "b", now.Add(time.Second)),
		newBlock(model.BlockProcessed, 2, "c", now.Add(2*time.Second)),
	}
	s.seedBlocks(blocks)

	s.metrics.EXPECT().Observe("random_unprocessed_block_heights", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	unprocessed, err := s.repo.RandomUnprocessedBlockHeights(s.testCtx, model.BTC, model.Mainnet, 5, 5)
	s.Require().NoError(err)
	s.Empty(unprocessed)
}
