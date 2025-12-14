// Package bitcoin implements Bitcoin-specific chain logic.
package bitcoin

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/safe"
)

// BackfillSource implements chain.BackfillSource for Bitcoin.
type BackfillSource struct {
	resolver        TransactionOutputResolver
	rpc             RPCClient
	outputConverter OutputConverter
	network         model.Network
}

// NewBackfillSource creates a BackfillSource for Bitcoin.
func NewBackfillSource(resolver TransactionOutputResolver, outputConverter OutputConverter, rpc RPCClient, network model.Network) (*BackfillSource, error) {
	return &BackfillSource{
		resolver:        resolver,
		rpc:             rpc,
		outputConverter: outputConverter,
		network:         network,
	}, nil
}

// LatestHeight returns the latest block height available from the node.
func (s *BackfillSource) LatestHeight(_ context.Context) (uint64, error) {
	count, err := s.rpc.GetBlockCount()
	if err != nil {
		return 0, err
	}
	height, err := safe.Uint64(count)
	if err != nil {
		return 0, fmt.Errorf("block count overflow: %w", err)
	}
	return height, nil
}

// FetchBlock retrieves a block with full transaction details at the given height.
func (s *BackfillSource) FetchBlock(ctx context.Context, height uint64) (*chain.BackfillBlock, error) {
	if height > math.MaxInt64 {
		return nil, fmt.Errorf("block height %d exceeds rpc limit", height)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	hash, err := s.rpc.GetBlockHash(int64(height))
	if err != nil {
		return nil, fmt.Errorf("get block hash at height %d: %w", height, err)
	}
	src, err := s.rpc.GetBlockVerboseTx(hash)
	if err != nil {
		return nil, fmt.Errorf("get block %s: %w", hash, err)
	}

	block, err := BuildBlockFromVerbose(*src, s.network, model.BlockProcessed)
	if err != nil {
		return nil, err
	}

	blockTxIDs := make(map[string]struct{}, len(src.Tx))
	for _, tx := range src.Tx {
		blockTxIDs[tx.Txid] = struct{}{}
	}

	// Prefetch previous outputs for all transactions referenced by this block, excluding
	// those produced inside the same block (we seed those below as we iterate).
	prevTxIDs := make([]string, 0)
	for _, tx := range src.Tx {
		for _, vin := range tx.Vin {
			if vin.IsCoinBase() {
				continue
			}
			if _, inBlock := blockTxIDs[vin.Txid]; inBlock {
				continue
			}
			prevTxIDs = append(prevTxIDs, vin.Txid)
		}
	}

	resolvedOutputs, err := s.resolver.ResolveBatch(ctx, prevTxIDs)
	if err != nil {
		return nil, fmt.Errorf("resolve prev outputs for block %d: %w", block.Height, err)
	}

	inputs := make([]model.TransactionInput, 0)

	for _, tx := range src.Tx {
		if len(tx.Vin) > math.MaxUint32 {
			return nil, fmt.Errorf("tx %s vin count overflow: %d", tx.Txid, len(tx.Vin))
		}
		if len(tx.Vout) > math.MaxUint32 {
			return nil, fmt.Errorf("tx %s vout count overflow: %d", tx.Txid, len(tx.Vout))
		}
		if tx.Size < 0 {
			return nil, fmt.Errorf("tx %s negative size: %d", tx.Txid, tx.Size)
		}
		if tx.Vsize < 0 {
			return nil, fmt.Errorf("tx %s negative vsize: %d", tx.Txid, tx.Vsize)
		}

		outputs, err := s.outputConverter.Convert(tx, block.Height, block.Timestamp)
		if err != nil {
			return nil, err
		}
		resolvedOutputs[tx.Txid] = outputs

		txInputs, err := s.convertInputs(ctx, tx, block.Height, block.Timestamp, resolvedOutputs)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, txInputs...)
	}

	return &chain.BackfillBlock{
		Block:  block,
		Inputs: inputs,
	}, nil
}

func (s *BackfillSource) convertInputs(
	ctx context.Context,
	tx btcjson.TxRawResult,
	blockHeight uint64,
	blockTime time.Time,
	resolved map[string][]model.TransactionOutput,
) ([]model.TransactionInput, error) {
	inputs := make([]model.TransactionInput, 0, len(tx.Vin))

	for idx, vin := range tx.Vin {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		index, err := safe.Uint32(idx)
		if err != nil {
			return nil, fmt.Errorf("tx %s input index overflow: %w", tx.Txid, err)
		}

		scriptHex := ""
		scriptAsm := ""
		if vin.ScriptSig != nil {
			scriptHex = vin.ScriptSig.Hex
			scriptAsm = vin.ScriptSig.Asm
		}

		input := model.TransactionInput{
			Coin:         model.BTC,
			Network:      s.network,
			BlockHeight:  blockHeight,
			BlockTime:    blockTime,
			TxID:         tx.Txid,
			Index:        index,
			PrevTxID:     vin.Txid,
			PrevVout:     vin.Vout,
			Sequence:     vin.Sequence,
			IsCoinbase:   vin.IsCoinBase(),
			ScriptSigHex: scriptHex,
			ScriptSigAsm: scriptAsm,
			Witness:      append([]string(nil), vin.Witness...),
		}

		if !vin.IsCoinBase() {
			prevOutputs, ok := resolved[vin.Txid]
			if !ok {
				return nil, fmt.Errorf("prev outputs for tx %s not prefetched", vin.Txid)
			}
			if int(vin.Vout) >= len(prevOutputs) {
				return nil, fmt.Errorf("input references missing vout %d in tx %s", vin.Vout, vin.Txid)
			}
			prevOut := prevOutputs[vin.Vout]
			input.Value = prevOut.Value
			input.Addresses = append([]string(nil), prevOut.Addresses...)
		}

		inputs = append(inputs, input)
	}

	return inputs, nil
}
