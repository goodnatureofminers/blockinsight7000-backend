package chain

import (
	"context"
	"fmt"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// TransactionOutputResolver fetches outputs during block processing to reduce ClickHouse lookups.
type TransactionOutputResolver struct {
	repo    ClickhouseRepository
	coin    model.Coin
	network model.Network
}

// transactionOutputResolverBatchSize controls how many txids are fetched in one repository call.
// It is a var to allow overriding in tests.
var transactionOutputResolverBatchSize = 1000

// NewTransactionOutputResolver constructs a TransactionOutputResolver for a specific network.
func NewTransactionOutputResolver(repo ClickhouseRepository, coin model.Coin, network model.Network) *TransactionOutputResolver {
	return &TransactionOutputResolver{
		repo:    repo,
		coin:    coin,
		network: network,
	}
}

// Resolve returns outputs for a transaction, consulting the cache first.
func (r *TransactionOutputResolver) Resolve(ctx context.Context, txid string) ([]model.TransactionOutputLookup, error) {
	outputs, err := r.repo.TransactionOutputsLookupByTxIDs(ctx, r.coin, r.network, []string{txid})
	if err != nil {
		return nil, fmt.Errorf("query outputs for tx %s: %w", txid, err)
	}

	return outputs[txid], nil
}

// ResolveBatch returns outputs for many transactions, consulting the cache first and reusing results across the batch.
func (r *TransactionOutputResolver) ResolveBatch(ctx context.Context, txids []string) (map[string][]model.TransactionOutputLookup, error) {
	result := make(map[string][]model.TransactionOutputLookup, len(txids))

	seen := make(map[string]struct{}, len(txids))
	missing := make([]string, 0, len(txids))

	for _, txid := range txids {
		if _, dup := seen[txid]; dup {
			continue
		}
		seen[txid] = struct{}{}
		missing = append(missing, txid)
	}

	if len(missing) > 0 {
		size := transactionOutputResolverBatchSize
		if size <= 0 {
			size = 1000
		}
		for start := 0; start < len(missing); start += size {
			end := start + size
			if end > len(missing) {
				end = len(missing)
			}

			fromRepo, err := r.repo.TransactionOutputsLookupByTxIDs(ctx, r.coin, r.network, missing[start:end])
			if err != nil {
				return nil, fmt.Errorf("query outputs for txids: %w", err)
			}
			for txid, outputs := range fromRepo {
				result[txid] = outputs
			}
			for _, txid := range missing[start:end] {
				if _, ok := result[txid]; !ok {
					result[txid] = nil
				}
			}
		}
	}

	return result, nil
}
