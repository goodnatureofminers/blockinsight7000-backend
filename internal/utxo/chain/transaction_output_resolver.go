package chain

import (
	"context"
	"fmt"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// TransactionOutputResolverFactory creates new per-block resolvers.
type TransactionOutputResolverFactory struct {
	repo    ClickhouseRepository
	coin    model.Coin
	network model.Network
}

// NewTransactionOutputResolverFactory constructs a resolver factory bound to a repository and network.
func NewTransactionOutputResolverFactory(repo ClickhouseRepository, coin model.Coin, network model.Network) *TransactionOutputResolverFactory {
	return &TransactionOutputResolverFactory{
		repo:    repo,
		coin:    coin,
		network: network,
	}
}

// New creates a TransactionOutputResolver with the factory settings.
func (f *TransactionOutputResolverFactory) New() *TransactionOutputResolver {
	return NewTransactionOutputResolver(f.repo, f.coin, f.network)
}

// TransactionOutputResolver caches outputs during block processing to reduce ClickHouse lookups.
type TransactionOutputResolver struct {
	repo    ClickhouseRepository
	coin    model.Coin
	network model.Network
	local   map[string][]model.TransactionOutput
}

// NewTransactionOutputResolver constructs a TransactionOutputResolver for a specific network.
func NewTransactionOutputResolver(repo ClickhouseRepository, coin model.Coin, network model.Network) *TransactionOutputResolver {
	return &TransactionOutputResolver{
		repo:    repo,
		coin:    coin,
		network: network,
		local:   make(map[string][]model.TransactionOutput),
	}
}

// Seed primes the local cache with outputs for a given transaction.
func (r *TransactionOutputResolver) Seed(txid string, outputs []model.TransactionOutput) {
	r.local[txid] = outputs
}

// Resolve returns outputs for a transaction, consulting the cache first.
func (r *TransactionOutputResolver) Resolve(ctx context.Context, txid string) ([]model.TransactionOutput, error) {
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
