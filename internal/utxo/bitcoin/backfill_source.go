package bitcoin

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

// BackfillSource implements chain.BackfillSource for Bitcoin.
type BackfillSource struct {
	repo    chain.ClickhouseRepository
	rpc     *RpcClient
	decoder *scriptDecoder
	coin    model.Coin
	network model.Network
}

func NewBackfillSource(repo chain.ClickhouseRepository, rpc *RpcClient, coin model.Coin, network model.Network) (*BackfillSource, error) {
	decoder, err := newScriptDecoder(network)
	if err != nil {
		return nil, err
	}
	return &BackfillSource{repo: repo, rpc: rpc, decoder: decoder, coin: coin, network: network}, nil
}

func (s *BackfillSource) LatestHeight(_ context.Context) (uint64, error) {
	count, err := s.rpc.GetBlockCount()
	return uint64(count), err
}

func (s *BackfillSource) FetchBlock(ctx context.Context, height uint64) (*chain.BackfillBlock, error) {
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

	resolver := chain.NewTransactionOutputResolver(s.repo, s.coin, s.network)
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
	if src.Height > math.MaxUint32 {
		return model.Block{}, fmt.Errorf("block height %d exceeds uint32", src.Height)
	}
	if src.Size < 0 {
		return model.Block{}, fmt.Errorf("block %d negative size: %d", src.Height, src.Size)
	}

	timestamp := time.Unix(src.Time, 0).UTC()
	return model.Block{
		Coin:       s.coin,
		Network:    s.network,
		Height:     uint64(src.Height),
		Hash:       src.Hash,
		Timestamp:  timestamp,
		Version:    uint32(src.Version),
		MerkleRoot: src.MerkleRoot,
		Bits:       bits,
		Nonce:      src.Nonce,
		Difficulty: src.Difficulty,
		Size:       uint32(src.Size),
		TXCount:    uint32(len(src.Tx)),
		Status:     model.BlockProcessed,
	}, nil
}

func (s *BackfillSource) convertOutputs(tx btcjson.TxRawResult, blockHeight uint64, blockTime time.Time) ([]model.TransactionOutput, error) {
	outputs := make([]model.TransactionOutput, 0, len(tx.Vout))
	for idx, vout := range tx.Vout {
		if vout.Value < 0 {
			return nil, fmt.Errorf("tx %s output %d negative value: %f", tx.Txid, idx, vout.Value)
		}

		value, err := BtcToSatoshis(vout.Value)
		if err != nil {
			return nil, fmt.Errorf("tx %s output %d convert value: %w", tx.Txid, idx, err)
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
			Index:       uint32(idx),
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
			Index:        uint32(idx),
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
