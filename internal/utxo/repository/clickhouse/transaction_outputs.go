package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// TransactionOutputs returns outputs for a transaction from ClickHouse.
func (r *Repository) TransactionOutputs(ctx context.Context, coin model.Coin, network model.Network, txid string) ([]model.TransactionOutput, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("transaction_outputs", coin, network, err, start)
	}()

	const query = `
SELECT
	block_height,
	block_timestamp,
	output_index,
	value,
	script_type,
	script_hex,
	script_asm,
	addresses
FROM utxo_transaction_outputs
WHERE coin = ? AND network = ? AND txid = CAST(? AS FixedString(64))
ORDER BY output_index ASC`

	rows, err := r.conn.Query(ctx, query, coin, network, txid)
	if err != nil {
		return nil, fmt.Errorf("query transaction outputs: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close rows: %w", cerr)
		}
	}()

	var outputs []model.TransactionOutput
	for rows.Next() {
		var output model.TransactionOutput
		output.Coin = coin
		output.Network = network
		output.TxID = txid
		if err = rows.Scan(
			&output.BlockHeight,
			&output.BlockTime,
			&output.Index,
			&output.Value,
			&output.ScriptType,
			&output.ScriptHex,
			&output.ScriptAsm,
			&output.Addresses,
		); err != nil {
			return nil, fmt.Errorf("scan transaction output: %w", err)
		}

		outputs = append(outputs, output)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transaction outputs: %w", err)
	}

	return outputs, nil
}

// TransactionOutputsByTxIDs returns outputs for multiple transactions from ClickHouse.
func (r *Repository) TransactionOutputsByTxIDs(ctx context.Context, coin model.Coin, network model.Network, txids []string) (map[string][]model.TransactionOutput, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("transaction_outputs_by_txids", coin, network, err, start)
	}()

	result := make(map[string][]model.TransactionOutput, len(txids))
	if len(txids) == 0 {
		return result, nil
	}

	const query = `
SELECT
	txid,
	output_index,
	value,
	addresses
FROM utxo_transaction_outputs
WHERE coin = ? AND network = ? AND txid IN ?
ORDER BY txid ASC, output_index ASC`

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
			output model.TransactionOutput
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
