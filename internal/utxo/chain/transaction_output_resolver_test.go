package chain

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestTransactionOutputResolver_Resolve_Fetches(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockClickhouseRepository(ctrl)
	coin := model.Coin("btc")
	network := model.Network("mainnet")
	resolver := NewTransactionOutputResolver(repo, coin, network)

	repoOutputs := []model.TransactionOutput{
		{TxID: "tx2", Index: 1, Value: 200, Coin: coin, Network: network},
	}
	repo.EXPECT().
		TransactionOutputs(gomock.Any(), coin, network, "tx2").
		Return(repoOutputs, nil).
		Times(1)

	got, err := resolver.Resolve(context.Background(), "tx2")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if len(got) != len(repoOutputs) || got[0].Coin != coin || got[0].Network != network || got[0].Value != repoOutputs[0].Value {
		t.Fatalf("Resolve returned unexpected outputs: %+v", got)
	}
}

func TestTransactionOutputResolver_ResolveBatch_DedupAndFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockClickhouseRepository(ctrl)
	coin := model.Coin("btc")
	network := model.Network("mainnet")
	resolver := NewTransactionOutputResolver(repo, coin, network)

	repoOutputs := []model.TransactionOutput{
		{TxID: "fetch", Index: 1, Value: 100, Coin: coin, Network: network},
	}
	repo.EXPECT().
		TransactionOutputsByTxIDs(gomock.Any(), coin, network, []string{"cached", "fetch"}).
		Return(map[string][]model.TransactionOutput{
			"cached": {{TxID: "cached", Index: 0, Value: 42}},
			"fetch":  repoOutputs,
		}, nil).
		Times(1)

	got, err := resolver.ResolveBatch(context.Background(), []string{"cached", "fetch", "cached"})
	if err != nil {
		t.Fatalf("ResolveBatch returned error: %v", err)
	}

	if len(got) != 2 || got["cached"][0].Value != 42 || got["fetch"][0].Coin != coin || got["fetch"][0].Network != network {
		t.Fatalf("ResolveBatch returned wrong outputs: %+v", got)
	}
}

func TestTransactionOutputResolver_ResolveBatch_ChunksRequests(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(func() {
		transactionOutputResolverBatchSize = 1000
		ctrl.Finish()
	})

	// Force chunk size to 1 to validate chunking behavior.
	transactionOutputResolverBatchSize = 1

	repo := NewMockClickhouseRepository(ctrl)
	coin := model.Coin("btc")
	network := model.Network("mainnet")

	gomock.InOrder(
		repo.EXPECT().
			TransactionOutputsByTxIDs(gomock.Any(), coin, network, []string{"tx1"}).
			Return(map[string][]model.TransactionOutput{
				"tx1": {{TxID: "tx1", Index: 0, Value: 1, Coin: coin, Network: network}},
			}, nil),
		repo.EXPECT().
			TransactionOutputsByTxIDs(gomock.Any(), coin, network, []string{"tx2"}).
			Return(map[string][]model.TransactionOutput{
				"tx2": {{TxID: "tx2", Index: 1, Value: 2, Coin: coin, Network: network}},
			}, nil),
	)

	resolver := &TransactionOutputResolver{
		repo:    repo,
		coin:    coin,
		network: network,
	}

	got, err := resolver.ResolveBatch(context.Background(), []string{"tx1", "tx2"})
	if err != nil {
		t.Fatalf("ResolveBatch returned error: %v", err)
	}

	if len(got) != 2 || got["tx1"][0].Value != 1 || got["tx2"][0].Network != network {
		t.Fatalf("ResolveBatch returned unexpected map: %+v", got)
	}
}
