package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

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
WHERE coin = ? AND network = ? AND txid = ?
ORDER BY output_index ASC`

	rows, err := r.conn.Query(ctx, query, coin, network, txid)
	if err != nil {
		return nil, fmt.Errorf("query transaction outputs: %w", err)
	}
	defer rows.Close()

	var outputs []model.TransactionOutput
	for rows.Next() {
		var output model.TransactionOutput
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
