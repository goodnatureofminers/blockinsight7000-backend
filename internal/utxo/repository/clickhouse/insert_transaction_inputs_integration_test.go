package clickhouse

import (
	"strings"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestInsertTransactionInputs() {
	now := time.Now().UTC().Truncate(time.Second)
	txID := strings.Repeat("b", 64)

	s.seedTransactions([]model.Transaction{
		{
			Coin:        model.BTC,
			Network:     model.Mainnet,
			TxID:        txID,
			BlockHeight: 2,
			Timestamp:   now,
			Size:        250,
			VSize:       200,
			Version:     1,
			LockTime:    0,
			InputCount:  1,
			OutputCount: 2,
		},
	})

	s.metrics.EXPECT().Observe("insert_transaction_inputs", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)
	inputs := []model.TransactionInput{
		{
			Coin:         model.BTC,
			Network:      model.Mainnet,
			BlockHeight:  2,
			BlockTime:    now,
			TxID:         txID,
			Index:        0,
			PrevTxID:     strings.Repeat("c", 64),
			PrevVout:     1,
			Sequence:     123,
			IsCoinbase:   false,
			Value:        42,
			ScriptSigHex: "0014abcd",
			ScriptSigAsm: "OP_HASH",
			Witness:      []string{"0014abcd"},
			Addresses:    []string{"addr-input"},
		},
	}
	s.Require().NoError(s.repo.InsertTransactionInputs(s.testCtx, inputs))

	s.Equal(uint64(len(inputs)), s.countRows("utxo_transaction_inputs"))
}
