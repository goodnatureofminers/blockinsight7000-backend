package clickhouse

import (
	"strings"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (s *RepositorySuite) TestTransactionOutputs() {
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
	s.seedTransactionOutputs(outputs)

	s.metrics.EXPECT().Observe("transaction_outputs", model.BTC, model.Mainnet, gomock.Nil(), gomock.Any()).Times(1)

	got, err := s.repo.TransactionOutputs(s.testCtx, model.BTC, model.Mainnet, strings.Repeat("a", 64))
	s.Require().NoError(err)
	s.Require().Len(got, len(outputs))

	for i, expected := range outputs {
		actual := got[i]
		s.Equal(expected.Index, actual.Index)
		s.Equal(expected.Value, actual.Value)
		s.Equal(expected.ScriptType, actual.ScriptType)
		s.Equal(strings.Join(expected.Addresses, ","), strings.Join(actual.Addresses, ","))
	}
}
