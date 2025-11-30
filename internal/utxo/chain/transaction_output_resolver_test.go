package chain

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestTransactionOutputResolver_Resolve_LocalCacheHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockClickhouseRepository(ctrl)
	coin := model.Coin("btc")
	network := model.Network("mainnet")
	cached := []model.TransactionOutput{
		{TxID: "tx1", Index: 0, Value: 100},
	}
	resolver := &TransactionOutputResolver{
		repo:    repo,
		coin:    coin,
		network: network,
		local:   map[string][]model.TransactionOutput{"tx1": cached},
	}

	got, err := resolver.Resolve(context.Background(), "tx1")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if len(got) != len(cached) || got[0].Value != cached[0].Value {
		t.Fatalf("Resolve returned unexpected outputs: %+v", got)
	}
}

func TestTransactionOutputResolver_Resolve_FetchesAndCaches(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockClickhouseRepository(ctrl)
	coin := model.Coin("btc")
	network := model.Network("mainnet")
	resolver := NewTransactionOutputResolver(repo, coin, network)

	repoOutputs := []model.TransactionOutput{
		{TxID: "tx2", Index: 1, Value: 200},
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

	// Second call should hit cache and not call repo again.
	gotCached, err := resolver.Resolve(context.Background(), "tx2")
	if err != nil {
		t.Fatalf("Resolve (cached) returned error: %v", err)
	}
	if len(gotCached) != len(repoOutputs) {
		t.Fatalf("Resolve (cached) returned unexpected outputs: %+v", gotCached)
	}
}
