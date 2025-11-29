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

// HistorySource implements chain.HistorySource for Bitcoin.
type HistorySource struct {
	rpc     *RpcClient
	decoder *scriptDecoder
	coin    model.Coin
	network model.Network
}

func NewHistorySource(rpc *RpcClient, coin model.Coin, network model.Network) (*HistorySource, error) {
	decoder, err := newScriptDecoder(network)
	if err != nil {
		return nil, err
	}
	return &HistorySource{rpc: rpc, decoder: decoder, coin: coin, network: network}, nil
}

func (s *HistorySource) LatestHeight(_ context.Context) (uint64, error) {
	count, err := s.rpc.GetBlockCount()
	return uint64(count), err
}

func (s *HistorySource) FetchBlock(ctx context.Context, height uint64) (*chain.HistoryBlock, error) {
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

		txs = append(txs, model.Transaction{
			Coin:        s.coin,
			Network:     s.network,
			TxID:        tx.Txid,
			BlockHeight: block.Height,
			Timestamp:   block.Timestamp,
			Size:        uint32(tx.Size),
			VSize:       uint32(tx.Vsize),
			Version:     tx.Version,
			LockTime:    tx.LockTime,
			InputCount:  uint16(len(tx.Vin)),
			OutputCount: uint16(len(tx.Vout)),
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
		Status:     model.BlockUnprocessed,
	}, nil
}

func (s *HistorySource) convertOutputs(tx btcjson.TxRawResult, blockHeight uint64, blockTime time.Time) ([]model.TransactionOutput, error) {
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
