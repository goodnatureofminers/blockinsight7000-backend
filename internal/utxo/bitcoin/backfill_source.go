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
	resolverFactory TransactionOutputResolverFactory
	rpc             *RPCClient
	decoder         *scriptDecoder
	coin            model.Coin
	network         model.Network
}

// NewBackfillSource creates a BackfillSource for Bitcoin.
func NewBackfillSource(resolverFactory TransactionOutputResolverFactory, rpc *RPCClient, coin model.Coin, network model.Network) (*BackfillSource, error) {
	decoder, err := newScriptDecoder(network)
	if err != nil {
		return nil, err
	}
	return &BackfillSource{resolverFactory: resolverFactory, rpc: rpc, decoder: decoder, coin: coin, network: network}, nil
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

	block, err := s.convertBlockHeader(*src)
	if err != nil {
		return nil, err
	}

	resolver := s.resolverFactory.New()
	inputs := make([]model.TransactionInput, 0)

	for _, tx := range src.Tx {
		if len(tx.Vin) > math.MaxUint16 {
			return nil, fmt.Errorf("tx %s vin count overflow: %d", tx.Txid, len(tx.Vin))
		}
		if len(tx.Vout) > math.MaxUint16 {
			return nil, fmt.Errorf("tx %s vout count overflow: %d", tx.Txid, len(tx.Vout))
		}
		if tx.Size < 0 {
			return nil, fmt.Errorf("tx %s negative size: %d", tx.Txid, tx.Size)
		}
		if tx.Vsize < 0 {
			return nil, fmt.Errorf("tx %s negative vsize: %d", tx.Txid, tx.Vsize)
		}

		outputs, err := s.convertOutputs(tx, block.Height, block.Timestamp)
		if err != nil {
			return nil, err
		}
		resolver.Seed(tx.Txid, outputs)

		txInputs, err := s.convertInputs(ctx, resolver, tx, block.Height, block.Timestamp)
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

func (s *BackfillSource) convertBlockHeader(src btcjson.GetBlockVerboseTxResult) (model.Block, error) {
	bits, err := ParseBits(src.Bits)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d bits parse: %w", src.Height, err)
	}
	timestamp := time.Unix(src.Time, 0).UTC()
	height, err := safe.Uint32(src.Height)
	if err != nil {
		return model.Block{}, fmt.Errorf("block height %d overflow: %w", src.Height, err)
	}
	version, err := safe.Uint32(src.Version)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d version overflow: %w", src.Height, err)
	}
	size, err := safe.Uint32(src.Size)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d size overflow: %w", src.Height, err)
	}
	txCount, err := safe.Uint32(len(src.Tx))
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d tx count overflow: %w", src.Height, err)
	}

	return model.Block{
		Coin:       s.coin,
		Network:    s.network,
		Height:     uint64(height),
		Hash:       src.Hash,
		Timestamp:  timestamp,
		Version:    version,
		MerkleRoot: src.MerkleRoot,
		Bits:       bits,
		Nonce:      src.Nonce,
		Difficulty: src.Difficulty,
		Size:       size,
		TXCount:    txCount,
		Status:     model.BlockProcessed,
	}, nil
}

func (s *BackfillSource) convertOutputs(tx btcjson.TxRawResult, blockHeight uint64, blockTime time.Time) ([]model.TransactionOutput, error) {
	outputs := make([]model.TransactionOutput, 0, len(tx.Vout))
	for idx, vout := range tx.Vout {
		if vout.Value < 0 {
			return nil, fmt.Errorf("tx %s output %d negative value: %f", tx.Txid, idx, vout.Value)
		}

		index, err := safe.Uint32(idx)
		if err != nil {
			return nil, fmt.Errorf("tx %s output index overflow: %w", tx.Txid, err)
		}

		value, err := BtcToSatoshis(vout.Value)
		if err != nil {
			return nil, fmt.Errorf("tx %s output %d safe value: %w", tx.Txid, idx, err)
		}
		addresses, err := s.decoder.decodeAddresses(vout)
		if err != nil {
			return nil, fmt.Errorf("decode addresses for tx %s output %d: %w", tx.Txid, idx, err)
		}

		outputs = append(outputs, model.TransactionOutput{
			Coin:        s.coin,
			Network:     s.network,
			BlockHeight: blockHeight,
			BlockTime:   blockTime,
			TxID:        tx.Txid,
			Index:       index,
			Value:       value,
			ScriptType:  vout.ScriptPubKey.Type,
			ScriptHex:   vout.ScriptPubKey.Hex,
			ScriptAsm:   vout.ScriptPubKey.Asm,
			Addresses:   addresses,
		})
	}
	return outputs, nil
}

func (s *BackfillSource) convertInputs(
	ctx context.Context,
	resolver *chain.TransactionOutputResolver,
	tx btcjson.TxRawResult,
	blockHeight uint64,
	blockTime time.Time,
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
			Coin:         s.coin,
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
			prevOutputs, err := resolver.Resolve(ctx, vin.Txid)
			if err != nil {
				return nil, fmt.Errorf("resolve prev outputs for tx %s: %w", vin.Txid, err)
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
