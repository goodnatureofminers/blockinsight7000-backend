package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// MaxContiguousBlockHeight returns the maximum contiguous height for a coin/network.
func (r *Repository) MaxContiguousBlockHeight(ctx context.Context, coin model.Coin, network model.Network) (uint64, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("max_contiguous_block_height", coin, network, err, start)
	}()

	const query = `WITH data AS (
    SELECT
        height,
        row_number() OVER (ORDER BY height) - 1 AS rn
    FROM utxo_blocks
    WHERE coin = ? and network =  ?
    group by height
)
SELECT max(height) AS max_contiguous_height
FROM data
WHERE rn = height limit 1;`

	rows, err := r.conn.Query(ctx, query, coin, network)
	if err != nil {
		return 0, fmt.Errorf("query max contiguous block height: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close rows: %w", closeErr)
		}
	}()

	var height uint64
	if !rows.Next() {
		return 0, fmt.Errorf("not found max contiguous block height")
	}

	if err = rows.Scan(&height); err != nil {
		return 0, fmt.Errorf("scan max contiguous block height: %w", err)
	}

	return height, nil
}
