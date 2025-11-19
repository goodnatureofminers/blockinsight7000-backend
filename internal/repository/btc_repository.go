package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/model"
)

type BTCRepository struct {
	conn clickhouse.Conn
}

func NewBTCRepository(dsn string) (*BTCRepository, error) {
	if dsn == "" {
		return nil, errors.New("clickhouse dsn is required")
	}

	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}

	return &BTCRepository{conn: conn}, nil
}

func (r *BTCRepository) BlocksCount(ctx context.Context, network string) (uint64, error) {
	const query = `
SELECT max(height) as height
FROM btc_blocks
WHERE network = ?`

	rows, err := r.conn.Query(ctx, query, network)
	if err != nil {
		return 0, fmt.Errorf("query blocks count: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, nil
	}
	var height uint64
	if err := rows.Scan(&height); err != nil {
		return 0, fmt.Errorf("scan blocks count: %w", err)
	}
	return height, nil
}

func (r *BTCRepository) MinBlockHeight(ctx context.Context, network string) (uint64, bool, error) {
	const query = `
SELECT toUInt64(min(height)) as height, count() as cnt
FROM btc_blocks
WHERE network = ?`

	rows, err := r.conn.Query(ctx, query, network)
	if err != nil {
		return 0, false, fmt.Errorf("query min block height: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, false, nil
	}

	var height uint64
	var cnt uint64
	if err := rows.Scan(&height, &cnt); err != nil {
		return 0, false, fmt.Errorf("scan min block height: %w", err)
	}
	if cnt == 0 {
		return 0, false, nil
	}
	return height, true, nil
}

func (r *BTCRepository) MaxBlockHeight(ctx context.Context, network string) (uint64, bool, error) {
	const query = `
SELECT toUInt64(max(height)) as height, count() as cnt
FROM btc_blocks
WHERE network = ?`

	rows, err := r.conn.Query(ctx, query, network)
	if err != nil {
		return 0, false, fmt.Errorf("query max block height: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, false, nil
	}

	var height uint64
	var cnt uint64
	if err := rows.Scan(&height, &cnt); err != nil {
		return 0, false, fmt.Errorf("scan max block height: %w", err)
	}
	if cnt == 0 {
		return 0, false, nil
	}
	return height, true, nil
}

func (r *BTCRepository) InsertBlocks(ctx context.Context, blocks []model.BTCBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	const query = `
INSERT INTO btc_blocks (
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
	tx_count
) VALUES`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare blocks batch: %w", err)
	}

	for _, block := range blocks {
		if err := batch.Append(
			block.Network,
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
		); err != nil {
			return fmt.Errorf("append block: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert blocks: %w", err)
	}
	return nil
}

func (r *BTCRepository) InsertTransactions(ctx context.Context, txs []model.BTCTransaction) error {
	if len(txs) == 0 {
		return nil
	}

	const query = `
INSERT INTO btc_transactions (
	network,
	txid,
	block_height,
	timestamp,
	size,
	vsize,
	version,
	locktime,
	fee,
	input_count,
	output_count
) VALUES`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare transactions batch: %w", err)
	}

	for _, tx := range txs {
		if err := batch.Append(
			tx.Network,
			tx.TxID,
			tx.BlockHeight,
			tx.Timestamp,
			tx.Size,
			tx.VSize,
			tx.Version,
			tx.LockTime,
			tx.Fee,
			tx.InputCount,
			tx.OutputCount,
		); err != nil {
			return fmt.Errorf("append transaction: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert transactions: %w", err)
	}
	return nil
}

func (r *BTCRepository) InsertTransactionInputs(ctx context.Context, inputs []model.BTCTransactionInput) error {
	if len(inputs) == 0 {
		return nil
	}

	const query = `
INSERT INTO btc_transaction_inputs (
	network,
	block_height,
	block_timestamp,
	txid,
	input_index,
	prev_txid,
	prev_vout,
	sequence,
	is_coinbase,
	value,
	script_sig_hex,
	script_sig_asm,
	witness,
	addresses
) VALUES`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare transaction inputs batch: %w", err)
	}

	for _, input := range inputs {
		if err := batch.Append(
			input.Network,
			input.BlockHeight,
			input.BlockTime,
			input.TxID,
			input.Index,
			input.PrevTxID,
			input.PrevVout,
			input.Sequence,
			input.IsCoinbase,
			input.Value,
			input.ScriptSigHex,
			input.ScriptSigAsm,
			input.Witness,
			input.Addresses,
		); err != nil {
			return fmt.Errorf("append transaction input: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert transaction inputs: %w", err)
	}
	return nil
}

func (r *BTCRepository) InsertTransactionOutputs(ctx context.Context, outputs []model.BTCTransactionOutput) error {
	if len(outputs) == 0 {
		return nil
	}

	const query = `
INSERT INTO btc_transaction_outputs (
	network,
	block_height,
	block_timestamp,
	txid,
	output_index,
	value,
	script_type,
	script_hex,
	script_asm,
	addresses
) VALUES`

	batch, err := r.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare transaction outputs batch: %w", err)
	}

	for _, output := range outputs {
		if err := batch.Append(
			output.Network,
			output.BlockHeight,
			output.BlockTime,
			output.TxID,
			output.Index,
			output.Value,
			output.ScriptType,
			output.ScriptHex,
			output.ScriptAsm,
			output.Addresses,
		); err != nil {
			return fmt.Errorf("append transaction output: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert transaction outputs: %w", err)
	}
	return nil
}

func (r *BTCRepository) TransactionOutputs(ctx context.Context, network, txid string) ([]model.BTCTransactionOutput, error) {
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
FROM btc_transaction_outputs
WHERE network = ? AND txid = ?
ORDER BY output_index ASC`

	rows, err := r.conn.Query(ctx, query, network, txid)
	if err != nil {
		return nil, fmt.Errorf("query transaction outputs: %w", err)
	}
	defer rows.Close()

	var outputs []model.BTCTransactionOutput
	for rows.Next() {
		var output model.BTCTransactionOutput
		output.Network = network
		output.TxID = txid
		if err := rows.Scan(
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transaction outputs: %w", err)
	}

	return outputs, nil
}
