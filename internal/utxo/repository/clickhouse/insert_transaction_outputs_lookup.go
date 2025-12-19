package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// InsertTransactionOutputsLookup stores transaction outputs in ClickHouse.
func (r *Repository) InsertTransactionOutputsLookup(ctx context.Context, outputs []model.TransactionOutput) error {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("insert_transaction_outputs_lookup", firstCoin(outputs), firstNetwork(outputs), err, start)
	}()

	if len(outputs) == 0 {
		return nil
	}

	const query = `INSERT INTO utxo_transaction_outputs_lookup (
    coin,
	network,
	txid,
	output_index,
	value,
	addresses
) VALUES`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare transaction outputs batch: %w", err)
	}

	for _, output := range outputs {
		if err = batch.Append(
			string(output.Coin),
			string(output.Network),
			output.TxID,
			output.Index,
			output.Value,
			output.Addresses,
		); err != nil {
			return fmt.Errorf("append transaction output: %w", err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("insert transaction outputs: %w", err)
	}
	return nil
}
