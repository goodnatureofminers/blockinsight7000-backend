package service

import (
	"context"
	"fmt"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// transactionOutputResolver caches outputs during block processing to reduce ClickHouse lookups.
type transactionOutputResolver struct {
	repo    ClickhouseRepository
	coin    model.Coin
	network model.Network
	local   map[string][]model.TransactionOutput
}

func newTransactionOutputResolver(repo ClickhouseRepository, coin model.Coin, network model.Network) *transactionOutputResolver {
	return &transactionOutputResolver{
		repo:    repo,
		coin:    coin,
		network: network,
		local:   make(map[string][]model.TransactionOutput),
	}
}

func (r *transactionOutputResolver) Seed(txid string, outputs []model.TransactionOutput) {
	r.local[txid] = outputs
}

func (r *transactionOutputResolver) Resolve(ctx context.Context, txid string) ([]model.TransactionOutput, error) {
	if outputs, ok := r.local[txid]; ok {
		return outputs, nil
	}

	outputs, err := r.repo.TransactionOutputs(ctx, r.coin, r.network, txid)
	if err != nil {
		return nil, fmt.Errorf("query outputs for tx %s: %w", txid, err)
	}

	for i := range outputs {
		outputs[i].Coin = r.coin
		outputs[i].Network = r.network
	}
	r.local[txid] = outputs
	return outputs, nil
}
