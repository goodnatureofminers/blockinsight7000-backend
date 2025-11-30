package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// InsertTransactionInputs stores transaction inputs in ClickHouse.
func (r *Repository) InsertTransactionInputs(ctx context.Context, inputs []model.TransactionInput) error {
	start := time.Now()
	var err error
	defer func() {
		r.metrics.Observe("insert_transaction_inputs", firstCoin(inputs), firstNetwork(inputs), err, start)
	}()

	if len(inputs) == 0 {
		return nil
	}

	const query = `
INSERT INTO utxo_transaction_inputs (
	coin,
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
		if err = batch.Append(
			string(input.Coin),
			string(input.Network),
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

	if err = batch.Send(); err != nil {
		return fmt.Errorf("insert transaction inputs: %w", err)
	}
	return nil
}
