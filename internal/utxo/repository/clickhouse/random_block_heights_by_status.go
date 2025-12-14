package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// RandomBlockHeightsByStatus returns random heights whose latest status matches the provided value up to maxHeight.
func (r *Repository) RandomBlockHeightsByStatus(ctx context.Context, coin model.Coin, network model.Network, status model.BlockStatus, maxHeight, limit uint64) ([]uint64, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("random_block_heights_by_status", coin, network, err, start)
	}()

	if limit == 0 {
		return nil, nil
	}

	const query = `
WITH latest AS (
	SELECT height, argMax(status, updated_at) AS status
	FROM utxo_blocks
	WHERE coin = ? AND network = ?
	GROUP BY height
)
SELECT height
FROM latest
WHERE status = ? AND height <= ?
ORDER BY rand()
LIMIT ?;`

	rows, err := r.conn.Query(ctx, query, coin, network, status, maxHeight, limit)
	if err != nil {
		return nil, fmt.Errorf("query random block heights by status: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close rows: %w", closeErr)
		}
	}()

	var heights []uint64
	for rows.Next() {
		var height uint64
		if err = rows.Scan(&height); err != nil {
			return nil, fmt.Errorf("scan random block height: %w", err)
		}
		heights = append(heights, height)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate random block heights by status: %w", err)
	}

	return heights, nil
}
