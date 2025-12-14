package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// MaxContiguousBlockHeightByStatuses returns the maximum contiguous height for a coin/network with any of the given statuses.
func (r *Repository) MaxContiguousBlockHeightByStatuses(ctx context.Context, coin model.Coin, network model.Network, statuses []model.BlockStatus) (uint64, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("max_contiguous_block_height_by_status", coin, network, err, start)
	}()

	if len(statuses) == 0 {
		return 0, fmt.Errorf("statuses is required")
	}

	statusPlaceholders := strings.Repeat("?,", len(statuses))
	statusPlaceholders = statusPlaceholders[:len(statusPlaceholders)-1]

	query := fmt.Sprintf(`WITH data AS (
    SELECT
        height,
        row_number() OVER (ORDER BY height) - 1 AS rn
    FROM (
        SELECT height
        FROM utxo_blocks
        WHERE coin = ? and network =  ?
        GROUP BY height
        HAVING argMax(status, updated_at) IN (%s)
    )
)
SELECT max(height) AS max_contiguous_height
FROM data
WHERE rn = height LIMIT 1;`, statusPlaceholders)

	args := []any{coin, network}
	for _, s := range statuses {
		args = append(args, s)
	}

	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("query max contiguous block height by status: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close rows: %w", closeErr)
		}
	}()

	var height uint64
	if !rows.Next() {
		return 0, fmt.Errorf("not found max contiguous block height by status")
	}

	if err = rows.Scan(&height); err != nil {
		return 0, fmt.Errorf("scan max contiguous block height by status: %w", err)
	}
	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate max contiguous block height by status: %w", err)
	}

	return height, nil
}
