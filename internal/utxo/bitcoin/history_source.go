package bitcoin

import (
	"context"
	"fmt"
	"math"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/safe"
)

// HistorySource implements chain.HistorySource for Bitcoin.
type HistorySource struct {
	rpc             RPCClient
	outputConverter OutputConverter
	network         model.Network
}

// NewHistorySource creates a HistorySource for Bitcoin.
func NewHistorySource(outputConverter OutputConverter, rpc RPCClient, network model.Network) (*HistorySource, error) {
	return &HistorySource{
		rpc:             rpc,
		outputConverter: outputConverter,
		network:         network,
	}, nil
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

	block, err := BuildBlockFromVerbose(*src, s.network, model.BlockUnprocessed)
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
			Coin:        model.BTC,
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

		txOutputs, err := s.outputConverter.Convert(tx, block.Height, block.Timestamp)
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
