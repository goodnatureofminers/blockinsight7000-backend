package clickhouse

import (
	"strings"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestInsertTransactionOutputs() {
	outputs := []model.TransactionOutput{
		{
			Coin:        model.BTC,
			Network:     model.Mainnet,
			BlockHeight: 1,
			BlockTime:   time.Now().UTC().Add(-time.Hour).Truncate(time.Second),
			TxID:        strings.Repeat("a", 64),
			Index:       0,
			Value:       100,
			ScriptType:  "pubkeyhash",
			ScriptHex:   "76a914...88ac",
			ScriptAsm:   "OP_DUP OP_HASH160",
			Addresses:   []string{"addr1"},
		},
		{
			Coin:        model.BTC,
			Network:     model.Mainnet,
			BlockHeight: 1,
			BlockTime:   time.Now().UTC().Add(-time.Hour).Truncate(time.Second),
			TxID:        strings.Repeat("a", 64),
			Index:       1,
			Value:       250,
			ScriptType:  "nulldata",
			ScriptHex:   "6a24aa",
			ScriptAsm:   "OP_RETURN",
			Addresses:   []string{"addr2", "addr3"},
		},
	}

	s.metrics.EXPECT().Observe("insert_transaction_outputs", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	s.Require().NoError(s.repo.InsertTransactionOutputs(s.testCtx, outputs))
	s.Equal(uint64(len(outputs)), s.countRows("utxo_transaction_outputs"))
}
