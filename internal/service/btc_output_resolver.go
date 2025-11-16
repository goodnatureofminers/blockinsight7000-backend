package service

import (
	"context"
	"fmt"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/model"
)

// BTCOutputResolver caches transaction outputs during ingestion so we can compute input values.
type BTCOutputResolver struct {
	repo    BTCRepository
	node    string
	network string
	cache   map[string][]model.BTCTransactionOutput
}

// NewBTCOutputResolver constructs the resolver for a given node/network scope.
func NewBTCOutputResolver(repo BTCRepository, node, network string) *BTCOutputResolver {
	return &BTCOutputResolver{
		repo:    repo,
		node:    node,
		network: network,
		cache:   make(map[string][]model.BTCTransactionOutput),
	}
}

// Seed allows pre-populating resolver cache with freshly processed outputs.
func (r *BTCOutputResolver) Seed(txid string, outputs []model.BTCTransactionOutput) {
	r.cache[txid] = outputs
}

// Local returns cached outputs when available.
func (r *BTCOutputResolver) Local(txid string) ([]model.BTCTransactionOutput, bool) {
	outputs, ok := r.cache[txid]
	return outputs, ok
}

// Resolve returns outputs either from cache or by querying ClickHouse.
func (r *BTCOutputResolver) Resolve(ctx context.Context, txid string) ([]model.BTCTransactionOutput, error) {
	if outputs, ok := r.cache[txid]; ok {
		return outputs, nil
	}

	outputs, err := r.repo.TransactionOutputs(ctx, r.node, r.network, txid)
	if err != nil {
		return nil, err
	}
	if len(outputs) == 0 {
		return nil, fmt.Errorf("transaction %s outputs not found in storage", txid)
	}

	r.cache[txid] = outputs
	return outputs, nil
}
