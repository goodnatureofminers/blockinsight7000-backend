package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// InsertBlocks stores block rows in ClickHouse.
func (r *Repository) InsertBlocks(ctx context.Context, blocks []model.Block) error {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("insert_blocks", firstCoin(blocks), firstNetwork(blocks), err, start)
	}()

	if len(blocks) == 0 {
		return nil
	}

	const query = `
INSERT INTO utxo_blocks (
	coin,
	network,
	height,
	hash,
	timestamp,
	version,
	merkleroot,
	bits,
	nonce,
	difficulty,
	size,
	tx_count,
    status
) VALUES`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare blocks batch: %w", err)
	}

	for _, block := range blocks {
		if err = batch.Append(
			string(block.Coin),
			string(block.Network),
			block.Height,
			block.Hash,
			block.Timestamp,
			block.Version,
			block.MerkleRoot,
			block.Bits,
			block.Nonce,
			block.Difficulty,
			block.Size,
			block.TXCount,
			string(block.Status),
		); err != nil {
			return fmt.Errorf("append block: %w", err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("insert blocks: %w", err)
	}
	return nil
}

func firstCoin[T any](items []T) model.Coin {
	if len(items) == 0 {
		return ""
	}

	switch v := any(items[0]).(type) {
	case model.Block:
		return v.Coin
	case model.Transaction:
		return v.Coin
	case model.TransactionInput:
		return v.Coin
	case model.TransactionOutput:
		return v.Coin
	default:
		return ""
	}
}

func firstNetwork[T any](items []T) model.Network {
	if len(items) == 0 {
		return ""
	}

	switch v := any(items[0]).(type) {
	case model.Block:
		return v.Network
	case model.Transaction:
		return v.Network
	case model.TransactionInput:
		return v.Network
	case model.TransactionOutput:
		return v.Network
	default:
		return ""
	}
}
