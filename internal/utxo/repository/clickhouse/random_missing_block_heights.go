package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func (r *Repository) RandomMissingBlockHeights(ctx context.Context, coin model.Coin, network model.Network, maxHeight, limit uint64) ([]uint64, error) {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("random_missing_block_heights", coin, network, err, start)
	}()

	if limit == 0 {
		return nil, nil
	}

	const query = `
WITH toUInt64(?) AS mx
SELECT number AS height
FROM numbers(mx + 1) AS m
LEFT ANTI JOIN (
	SELECT height
	FROM utxo_blocks
	WHERE coin = ? AND network = ? AND height <= mx
) AS b ON b.height = m.number
WHERE m.number <= mx
ORDER BY rand()
LIMIT ?`

	rows, err := r.conn.Query(ctx, query, maxHeight, coin, network, limit)
	if err != nil {
		return nil, fmt.Errorf("query random missing block heights: %w", err)
	}
	defer rows.Close()

	var heights []uint64
	for rows.Next() {
		var height uint64
		if err = rows.Scan(&height); err != nil {
			return nil, fmt.Errorf("scan random missing block height: %w", err)
		}
		heights = append(heights, height)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate random missing block heights: %w", err)
	}

	return heights, nil
}
