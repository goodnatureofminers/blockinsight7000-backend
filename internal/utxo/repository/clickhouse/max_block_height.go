package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// MaxBlockHeight returns the maximum height stored for a coin/network.
func (r *Repository) MaxBlockHeight(ctx context.Context, coin model.Coin, network model.Network) (uint64, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("max_block_height", coin, network, err, start)
	}()

	const query = `
SELECT coalesce(max(height), toUInt64(0)) AS max_height
FROM utxo_blocks
WHERE coin = ? AND network = ?`

	rows, err := r.conn.Query(ctx, query, coin, network)
	if err != nil {
		return 0, fmt.Errorf("query max block height: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close rows: %w", closeErr)
		}
	}()

	var height uint64
	if !rows.Next() {
		return 0, fmt.Errorf("max block height not found")
	}

	if err = rows.Scan(&height); err != nil {
		return 0, fmt.Errorf("scan max block height: %w", err)
	}
	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate max block height: %w", err)
	}

	return height, nil
}
