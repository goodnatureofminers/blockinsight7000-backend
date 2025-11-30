package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// InsertTransactions stores transactions in ClickHouse.
func (r *Repository) InsertTransactions(ctx context.Context, txs []model.Transaction) error {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("insert_transactions", firstCoin(txs), firstNetwork(txs), err, start)
	}()

	if len(txs) == 0 {
		return nil
	}

	const query = `
INSERT INTO utxo_transactions (
	coin,
	network,
	txid,
	block_height,
	timestamp,
	size,
	vsize,
	version,
	locktime,
	input_count,
output_count
) VALUES`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare transactions batch: %w", err)
	}

	for _, tx := range txs {
		if err = batch.Append(
			string(tx.Coin),
			string(tx.Network),
			tx.TxID,
			tx.BlockHeight,
			tx.Timestamp,
			tx.Size,
			tx.VSize,
			tx.Version,
			tx.LockTime,
			tx.InputCount,
			tx.OutputCount,
		); err != nil {
			return fmt.Errorf("append transaction: %w", err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("insert transactions: %w", err)
	}
	return nil
}
