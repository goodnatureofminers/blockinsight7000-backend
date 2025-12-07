package clickhouse

import (
	"strings"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestInsertTransactions() {
	now := time.Now().UTC().Truncate(time.Second)
	txs := []model.Transaction{
		{
			Coin:        model.BTC,
			Network:     model.Mainnet,
			TxID:        strings.Repeat("b", 64),
			BlockHeight: 2,
			Timestamp:   now,
			Size:        250,
			VSize:       200,
			Version:     1,
			LockTime:    0,
			InputCount:  1,
			OutputCount: 2,
		},
	}

	s.metrics.EXPECT().Observe("insert_transactions", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	s.Require().NoError(s.repo.InsertTransactions(s.testCtx, txs))
	s.Equal(uint64(len(txs)), s.countRows("utxo_transactions"))
}
