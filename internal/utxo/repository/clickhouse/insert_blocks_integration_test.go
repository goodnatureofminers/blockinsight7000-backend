package clickhouse

import (
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestInsertBlocks() {
	now := time.Now().UTC().Truncate(time.Second)
	blocks := []model.Block{
		newBlock(model.BlockProcessed, 0, "a", now),
		newBlock(model.BlockProcessed, 1, "b", now.Add(time.Second)),
	}

	s.metrics.EXPECT().Observe("insert_blocks", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	s.Require().NoError(s.repo.InsertBlocks(s.testCtx, blocks))
	s.Equal(uint64(len(blocks)), s.countRows("utxo_blocks"))
}

func (s *RepositorySuite) TestInsertBlocksUpdatesStatusWithArgMax() {
	now := time.Now().UTC().Truncate(time.Second)

	processed := newBlock(model.BlockProcessed, 0, "a", now)
	unprocessed := processed
	unprocessed.Status = model.BlockUnprocessed

	s.metrics.EXPECT().Observe("insert_blocks", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(2)

	s.Require().NoError(s.repo.InsertBlocks(s.testCtx, []model.Block{unprocessed}))

	time.Sleep(time.Second)

	s.Require().NoError(s.repo.InsertBlocks(s.testCtx, []model.Block{processed}))

	rows, err := s.repo.conn.Query(s.testCtx, `
SELECT argMax(status, updated_at)
FROM utxo_blocks
WHERE coin = ? AND network = ? AND height = ?`, model.BTC, model.Mainnet, processed.Height)
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(rows.Close())
	}()

	var status string
	s.Require().True(rows.Next())
	s.Require().NoError(rows.Scan(&status))
	s.Equal(string(model.BlockProcessed), status)
}

func (s *RepositorySuite) TestInsertBlocksUpdatesStatusWithArgMaxProcessedToUnprocessed() {
	now := time.Now().UTC().Truncate(time.Second)

	processed := newBlock(model.BlockProcessed, 1, "b", now)
	unprocessed := processed
	unprocessed.Status = model.BlockUnprocessed

	s.metrics.EXPECT().Observe("insert_blocks", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(2)

	s.Require().NoError(s.repo.InsertBlocks(s.testCtx, []model.Block{processed}))

	time.Sleep(time.Second)

	s.Require().NoError(s.repo.InsertBlocks(s.testCtx, []model.Block{unprocessed}))

	rows, err := s.repo.conn.Query(s.testCtx, `
SELECT argMax(status, updated_at)
FROM utxo_blocks
WHERE coin = ? AND network = ? AND height = ?`, model.BTC, model.Mainnet, processed.Height)
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(rows.Close())
	}()

	var status string
	s.Require().True(rows.Next())
	s.Require().NoError(rows.Scan(&status))
	s.Equal(string(model.BlockUnprocessed), status)
}
