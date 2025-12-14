package clickhouse

import (
	"context"
	"fmt"
	"sort"
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

	partitions := groupInputsByPartition(inputs)
	keys := make([]inputPartitionKey, 0, len(partitions))
	for key := range partitions {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].coin != keys[j].coin {
			return keys[i].coin < keys[j].coin
		}
		if keys[i].network != keys[j].network {
			return keys[i].network < keys[j].network
		}
		return keys[i].yyyymm < keys[j].yyyymm
	})

	for _, key := range keys {
		if err = r.insertTransactionInputsBatch(ctx, partitions[key]); err != nil {
			return err
		}
	}

	return nil
}

type inputPartitionKey struct {
	coin    model.Coin
	network model.Network
	yyyymm  int
}

func groupInputsByPartition(inputs []model.TransactionInput) map[inputPartitionKey][]model.TransactionInput {
	partitioned := make(map[inputPartitionKey][]model.TransactionInput, len(inputs))
	for _, input := range inputs {
		t := input.BlockTime.UTC()
		key := inputPartitionKey{
			coin:    input.Coin,
			network: input.Network,
			yyyymm:  t.Year()*100 + int(t.Month()),
		}
		partitioned[key] = append(partitioned[key], input)
	}
	return partitioned
}

func (r *Repository) insertTransactionInputsBatch(ctx context.Context, inputs []model.TransactionInput) error {
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
