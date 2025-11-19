package service

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/model"
)

func TestOutputResolverSeedAndLocal(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockBTCRepository(ctrl)
	resolver := NewBTCOutputResolver(repo, "network")

	expected := []model.BTCTransactionOutput{
		{TxID: "tx", Index: 0, Value: 1},
	}
	resolver.Seed("tx", expected)

	got, ok := resolver.Local("tx")
	if !ok {
		t.Fatalf("expected cache hit")
	}
	if len(got) != len(expected) || got[0].Value != expected[0].Value {
		t.Fatalf("unexpected outputs: %#v", got)
	}
}

func TestOutputResolverResolveFetchesAndCaches(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockBTCRepository(ctrl)
	resolver := NewBTCOutputResolver(repo, "network")

	expected := []model.BTCTransactionOutput{
		{TxID: "tx", Index: 0, Value: 42},
	}
	ctx := context.Background()

	repo.EXPECT().
		TransactionOutputs(ctx, "network", "tx").
		Return(expected, nil).
		Times(1)

	// First call should query repository.
	got, err := resolver.Resolve(ctx, "tx")
	if err != nil {
		t.Fatalf("resolve returned error: %v", err)
	}
	if len(got) != 1 || got[0].Value != expected[0].Value {
		t.Fatalf("unexpected outputs: %#v", got)
	}

	// Second call should hit cache only.
	gotCached, err := resolver.Resolve(ctx, "tx")
	if err != nil {
		t.Fatalf("resolve returned error on cached call: %v", err)
	}
	if len(gotCached) != 1 || gotCached[0].Value != expected[0].Value {
		t.Fatalf("unexpected outputs from cache: %#v", gotCached)
	}
}

func TestOutputResolverResolvePropagatesRepoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockBTCRepository(ctrl)
	resolver := NewBTCOutputResolver(repo, "network")

	ctx := context.Background()
	expectedErr := errors.New("boom")

	repo.EXPECT().
		TransactionOutputs(ctx, "network", "tx").
		Return(nil, expectedErr)

	if _, err := resolver.Resolve(ctx, "tx"); !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestOutputResolverResolveFailsOnMissingOutputs(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	repo := NewMockBTCRepository(ctrl)
	resolver := NewBTCOutputResolver(repo, "network")

	ctx := context.Background()

	repo.EXPECT().
		TransactionOutputs(ctx, "network", "tx").
		Return([]model.BTCTransactionOutput{}, nil)

	if _, err := resolver.Resolve(ctx, "tx"); err == nil {
		t.Fatal("expected error for missing outputs")
	}
}
