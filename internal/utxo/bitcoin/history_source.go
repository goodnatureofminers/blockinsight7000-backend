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

// HistorySource implements chain.HistorySource for Bitcoin.
type HistorySource struct {
	rpc     *RPCClient
	decoder *scriptDecoder
	coin    model.Coin
	network model.Network
}

// NewHistorySource creates a HistorySource for Bitcoin.
func NewHistorySource(rpc *RPCClient, coin model.Coin, network model.Network) (*HistorySource, error) {
	decoder, err := newScriptDecoder(network)
	if err != nil {
		return nil, err
	}
	return &HistorySource{rpc: rpc, decoder: decoder, coin: coin, network: network}, nil
}

// LatestHeight returns the latest block height from the node.
func (s *HistorySource) LatestHeight(_ context.Context) (uint64, error) {
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

// FetchBlock retrieves a block with transactions/outputs at the given height.
func (s *HistorySource) FetchBlock(ctx context.Context, height uint64) (*chain.HistoryBlock, error) {
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

	txs := make([]model.Transaction, 0, len(src.Tx))
	outputs := make([]model.TransactionOutput, 0)

	for _, tx := range src.Tx {
		inputCount := len(tx.Vin)
		if inputCount > math.MaxUint16 {
			return nil, fmt.Errorf("tx %s vin count overflow: %d", tx.Txid, inputCount)
		}
		outputCount := len(tx.Vout)
		if outputCount > math.MaxUint16 {
			return nil, fmt.Errorf("tx %s vout count overflow: %d", tx.Txid, outputCount)
		}

		size, err := safe.Uint32(tx.Size)
		if err != nil {
			return nil, fmt.Errorf("tx %s size overflow: %w", tx.Txid, err)
		}
		vsize, err := safe.Uint32(tx.Vsize)
		if err != nil {
			return nil, fmt.Errorf("tx %s vsize overflow: %w", tx.Txid, err)
		}
		version, err := safe.Uint32(tx.Version)
		if err != nil {
			return nil, fmt.Errorf("tx %s version overflow: %w", tx.Txid, err)
		}

		txs = append(txs, model.Transaction{
			Coin:        s.coin,
			Network:     s.network,
			TxID:        tx.Txid,
			BlockHeight: block.Height,
			Timestamp:   block.Timestamp,
			Size:        size,
			VSize:       vsize,
			Version:     version,
			LockTime:    tx.LockTime,
			InputCount:  uint16(inputCount),
			OutputCount: uint16(outputCount),
		})

		txOutputs, err := s.convertOutputs(tx, block.Height, block.Timestamp)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, txOutputs...)
	}

	return &chain.HistoryBlock{
		Block:   block,
		Txs:     txs,
		Outputs: outputs,
	}, nil
}

func (s *HistorySource) convertBlockHeader(src btcjson.GetBlockVerboseTxResult) (model.Block, error) {
	bits, err := ParseBits(src.Bits)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d bits parse: %w", src.Height, err)
	}
	timestamp := time.Unix(src.Time, 0).UTC()
	height, err := safe.Uint32(src.Height)
	if err != nil {
		return model.Block{}, fmt.Errorf("block height %d overflow: %w", src.Height, err)
	}
	size, err := safe.Uint32(src.Size)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d size overflow: %w", src.Height, err)
	}
	version, err := safe.Uint32(src.Version)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d version overflow: %w", src.Height, err)
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
		Status:     model.BlockUnprocessed,
	}, nil
}

func (s *HistorySource) convertOutputs(tx btcjson.TxRawResult, blockHeight uint64, blockTime time.Time) ([]model.TransactionOutput, error) {
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
