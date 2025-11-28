package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (r *Repository) RandomUnprocessedBlockHeights(ctx context.Context, coin model.Coin, network model.Network, maxHeight, limit uint64) ([]uint64, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("random_unprocessed_block_heights", coin, network, err, start)
	}()

	if limit == 0 {
		return nil, nil
	}

	const query = `
with a as (
SELECT height, argMax(status, updated_at) as status
FROM utxo_blocks
WHERE 
	coin = ?
	AND network = ?
	AND height <= ?
group by height)
select height from a where status = 'unprocessed'
ORDER BY rand()
LIMIT ?;`

	rows, err := r.conn.Query(ctx, query, coin, network, maxHeight, limit)
	if err != nil {
		return nil, fmt.Errorf("query random unprocessed block heights: %w", err)
	}
	defer rows.Close()

	var heights []uint64
	for rows.Next() {
		var height uint64
		if err = rows.Scan(&height); err != nil {
			return nil, fmt.Errorf("scan random unprocessed block height: %w", err)
		}
		heights = append(heights, height)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate random unprocessed block heights: %w", err)
	}

	return heights, nil
}
