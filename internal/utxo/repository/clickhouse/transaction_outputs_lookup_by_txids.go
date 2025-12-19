package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// TransactionOutputsLookupByTxIDs returns outputs for multiple transactions from ClickHouse.
func (r *Repository) TransactionOutputsLookupByTxIDs(ctx context.Context, coin model.Coin, network model.Network, txids []string) (map[string][]model.TransactionOutputLookup, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("transaction_outputs_lookup_by_txids", coin, network, err, start)
	}()

	result := make(map[string][]model.TransactionOutputLookup, len(txids))
	if len(txids) == 0 {
		return result, nil
	}

	const query = `
SELECT
	txid,
	output_index,
	anyLast(value) AS value,
    anyLast(addresses) AS addresses
FROM utxo_transaction_outputs_lookup
WHERE coin = ? AND network = ? AND txid IN ?
GROUP BY
    txid,
    output_index
ORDER BY output_index ASC
SETTINGS max_threads = 1`

	rows, err := r.conn.Query(ctx, query, coin, network, txids)
	if err != nil {
		return nil, fmt.Errorf("query transaction outputs by txids: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close rows: %w", cerr)
		}
	}()

	for rows.Next() {
		var (
			txid   string
			output model.TransactionOutputLookup
		)
		if err = rows.Scan(
			&txid,
			&output.Index,
			&output.Value,
			&output.Addresses,
		); err != nil {
			return nil, fmt.Errorf("scan transaction output: %w", err)
		}

		output.Coin = coin
		output.Network = network
		output.TxID = txid

		result[txid] = append(result[txid], output)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transaction outputs: %w", err)
	}

	return result, nil
}
