package service

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
	"go.uber.org/zap"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/model"
)

// BTCHistoryBatchConfig defines batch sizes used when persisting data to ClickHouse.
type BTCHistoryBatchConfig struct {
	BlockBatchSize  int
	TxBatchSize     int
	OutputBatchSize int
	InputBatchSize  int
}

// DefaultBTCHistoryBatchConfig returns sane default batch sizes.
func DefaultBTCHistoryBatchConfig() BTCHistoryBatchConfig {
	return BTCHistoryBatchConfig{
		BlockBatchSize:  100,
		TxBatchSize:     500,
		OutputBatchSize: 1000,
		InputBatchSize:  1000,
	}
}

// BTCHistorySyncService orchestrates block ingestion into ClickHouse.
type BTCHistorySyncService struct {
	repo    BTCRepository
	rpc     BTCRpcClient
	logger  *zap.Logger
	network string
	batch   BTCHistoryBatchConfig
	decoder *scriptDecoder
}

// NewBTCHistorySyncService builds the history sync service with the provided dependencies.
func NewBTCHistorySyncService(
	repo BTCRepository,
	rpc BTCRpcClient,
	network string,
	logger *zap.Logger,
	batch BTCHistoryBatchConfig,
) (*BTCHistorySyncService, error) {
	if batch.BlockBatchSize <= 0 || batch.TxBatchSize <= 0 || batch.OutputBatchSize <= 0 || batch.InputBatchSize <= 0 {
		batch = DefaultBTCHistoryBatchConfig()
	}
	decoder, err := newScriptDecoder(network)
	if err != nil {
		return nil, err
	}
	return &BTCHistorySyncService{
		repo:    repo,
		rpc:     rpc,
		logger:  logger,
		network: network,
		batch:   batch,
		decoder: decoder,
	}, nil
}

// Run performs the backfill until the latest block height.
func (s *BTCHistorySyncService) Run(ctx context.Context) error {
	maxHeight, exists, err := s.repo.MaxBlockHeight(ctx, s.network)
	if err != nil {
		return err
	}

	latestHeight, err := s.rpc.GetBlockCount()
	if err != nil {
		return fmt.Errorf("get latest block height: %w", err)
	}
	if latestHeight < 0 {
		return fmt.Errorf("negative latest height %d", latestHeight)
	}

	targetHeight := uint64(latestHeight)
	var startHeight uint64

	switch {
	case !exists:
		startHeight = 0
		s.logger.Info("starting history backfill from genesis",
			zap.String("network", s.network),
			zap.Uint64("target_height", targetHeight))
	default:
		if maxHeight >= targetHeight {
			s.logger.Info("history already ingested up to latest height",
				zap.String("network", s.network),
				zap.Uint64("height", maxHeight))
			return nil
		}
		startHeight = maxHeight + 1
		s.logger.Info("resuming history backfill",
			zap.String("network", s.network),
			zap.Uint64("start_height", startHeight),
			zap.Uint64("target_height", targetHeight))
	}

	blockBatch := make([]model.BTCBlock, 0, s.batch.BlockBatchSize)
	txBatch := make([]model.BTCTransaction, 0, s.batch.TxBatchSize)
	outputBatch := make([]model.BTCTransactionOutput, 0, s.batch.OutputBatchSize)
	inputBatch := make([]model.BTCTransactionInput, 0, s.batch.InputBatchSize)

	resolver := NewBTCOutputResolver(s.repo, s.network)

	flush := func() error {
		if err := s.flushBatches(ctx, blockBatch, txBatch, inputBatch, outputBatch); err != nil {
			return err
		}
		blockBatch = blockBatch[:0]
		txBatch = txBatch[:0]
		inputBatch = inputBatch[:0]
		outputBatch = outputBatch[:0]
		return nil
	}

	for h := startHeight; h <= targetHeight; h++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		blockModel, txModels, inputs, outputs, err := s.processHeight(ctx, resolver, h)
		if err != nil {
			return err
		}

		blockBatch = append(blockBatch, blockModel)
		txBatch = append(txBatch, txModels...)
		inputBatch = append(inputBatch, inputs...)
		outputBatch = append(outputBatch, outputs...)

		if len(blockBatch) >= s.batch.BlockBatchSize ||
			len(txBatch) >= s.batch.TxBatchSize ||
			len(inputBatch) >= s.batch.InputBatchSize ||
			len(outputBatch) >= s.batch.OutputBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if len(blockBatch) > 0 || len(txBatch) > 0 || len(inputBatch) > 0 || len(outputBatch) > 0 {
		if err := flush(); err != nil {
			return err
		}
	}

	s.logger.Info("history backfill complete",
		zap.String("network", s.network),
		zap.Uint64("last_height", targetHeight))

	return nil
}

func (s *BTCHistorySyncService) processHeight(
	ctx context.Context,
	resolver *BTCOutputResolver,
	height uint64,
) (model.BTCBlock, []model.BTCTransaction, []model.BTCTransactionInput, []model.BTCTransactionOutput, error) {
	block, err := s.fetchBlockByHeight(ctx, height)
	if err != nil {
		return model.BTCBlock{}, nil, nil, nil, err
	}

	blockModel, txModels, inputs, outputs, err := s.convertRPCBlock(ctx, resolver, block)
	if err != nil {
		return model.BTCBlock{}, nil, nil, nil, err
	}

	s.logger.Info("ingested block",
		zap.Uint64("height", height),
		zap.String("network", s.network),
		zap.Int("transactions", len(txModels)))

	return blockModel, txModels, inputs, outputs, nil
}

func (s *BTCHistorySyncService) fetchBlockByHeight(ctx context.Context, height uint64) (*btcjson.GetBlockVerboseTxResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if height > math.MaxInt64 {
		return nil, fmt.Errorf("height %d exceeds rpc limit", height)
	}
	hash, err := s.rpc.GetBlockHash(int64(height))
	if err != nil {
		return nil, fmt.Errorf("get block hash at height %d: %w", height, err)
	}
	block, err := s.rpc.GetBlockVerboseTx(hash)
	if err != nil {
		return nil, fmt.Errorf("get block %s: %w", hash, err)
	}
	return block, nil
}

func (s *BTCHistorySyncService) convertRPCBlock(
	ctx context.Context,
	resolver *BTCOutputResolver,
	src *btcjson.GetBlockVerboseTxResult,
) (model.BTCBlock, []model.BTCTransaction, []model.BTCTransactionInput, []model.BTCTransactionOutput, error) {
	var block model.BTCBlock
	bits, err := parseBits(src.Bits)
	if err != nil {
		return block, nil, nil, nil, fmt.Errorf("block %d bits parse: %w", src.Height, err)
	}
	if src.Height > math.MaxUint32 {
		return block, nil, nil, nil, fmt.Errorf("block height %d exceeds uint32", src.Height)
	}
	if src.Size < 0 {
		return block, nil, nil, nil, fmt.Errorf("block %d negative size: %d", src.Height, src.Size)
	}

	timestamp := time.Unix(src.Time, 0).UTC()
	block = model.BTCBlock{
		Network:    s.network,
		Height:     uint32(src.Height),
		Hash:       src.Hash,
		Timestamp:  timestamp,
		Version:    uint32(src.Version),
		MerkleRoot: src.MerkleRoot,
		Bits:       bits,
		Nonce:      src.Nonce,
		Difficulty: src.Difficulty,
		Size:       uint32(src.Size),
		TXCount:    uint32(len(src.Tx)),
	}

	totalOutputs := 0
	totalInputs := 0
	for _, tx := range src.Tx {
		totalOutputs += len(tx.Vout)
		totalInputs += len(tx.Vin)
	}

	outputs := make([]model.BTCTransactionOutput, 0, totalOutputs)
	for _, tx := range src.Tx {
		txOutputs, err := s.convertOutputs(tx, block.Height, timestamp)
		if err != nil {
			return block, nil, nil, nil, err
		}
		resolver.Seed(tx.Txid, txOutputs)
		outputs = append(outputs, txOutputs...)
	}

	txs := make([]model.BTCTransaction, 0, len(src.Tx))
	inputs := make([]model.BTCTransactionInput, 0, totalInputs)
	for _, tx := range src.Tx {

		if len(tx.Vin) > math.MaxUint16 {
			return block, nil, nil, nil, fmt.Errorf("tx %s vin count overflow: %d", tx.Txid, len(tx.Vin))
		}
		if len(tx.Vout) > math.MaxUint16 {
			return block, nil, nil, nil, fmt.Errorf("tx %s vout count overflow: %d", tx.Txid, len(tx.Vout))
		}
		if tx.Size < 0 {
			return block, nil, nil, nil, fmt.Errorf("tx %s negative size: %d", tx.Txid, tx.Size)
		}
		if tx.Vsize < 0 {
			return block, nil, nil, nil, fmt.Errorf("tx %s negative vsize: %d", tx.Txid, tx.Vsize)
		}

		txInputs, inputTotal, err := convertInputs(ctx, resolver, tx, s.network, block.Height, timestamp)
		if err != nil {
			return block, nil, nil, nil, err
		}

		txOutputs, ok := resolver.Local(tx.Txid)
		if !ok {
			return block, nil, nil, nil, fmt.Errorf("outputs for tx %s not prepared", tx.Txid)
		}
		outputTotal := sumOutputValues(txOutputs)

		var fee uint64
		if !isCoinbaseTx(tx) {
			if inputTotal < outputTotal {
				return block, nil, nil, nil, fmt.Errorf("tx %s inputs total %d is less than outputs total %d", tx.Txid, inputTotal, outputTotal)
			}
			fee = inputTotal - outputTotal
		}

		inputs = append(inputs, txInputs...)

		txs = append(txs, model.BTCTransaction{
			Network:     s.network,
			TxID:        tx.Txid,
			BlockHeight: block.Height,
			Timestamp:   timestamp,
			Size:        uint32(tx.Size),
			VSize:       uint32(tx.Vsize),
			Version:     tx.Version,
			LockTime:    tx.LockTime,
			Fee:         fee,
			InputCount:  uint16(len(tx.Vin)),
			OutputCount: uint16(len(tx.Vout)),
		})
	}

	return block, txs, inputs, outputs, nil
}

func (s *BTCHistorySyncService) convertOutputs(tx btcjson.TxRawResult, blockHeight uint32, blockTime time.Time) ([]model.BTCTransactionOutput, error) {
	outputs := make([]model.BTCTransactionOutput, 0, len(tx.Vout))
	for idx, vout := range tx.Vout {
		if vout.Value < 0 {
			return nil, fmt.Errorf("tx %s output %d negative value: %f", tx.Txid, idx, vout.Value)
		}

		value, err := btcToSatoshis(vout.Value)
		if err != nil {
			return nil, fmt.Errorf("tx %s output %d convert value: %w", tx.Txid, idx, err)
		}

		addresses, err := s.decoder.decodeAddresses(vout)
		if err != nil {
			return nil, fmt.Errorf("decode addresses for tx %s output %d: %w", tx.Txid, idx, err)
		}

		outputs = append(outputs, model.BTCTransactionOutput{
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

func convertInputs(
	ctx context.Context,
	resolver *BTCOutputResolver,
	tx btcjson.TxRawResult,
	network string,
	blockHeight uint32,
	blockTime time.Time,
) ([]model.BTCTransactionInput, uint64, error) {
	inputs := make([]model.BTCTransactionInput, 0, len(tx.Vin))
	var total uint64

	for idx, vin := range tx.Vin {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}

		scriptHex := ""
		scriptAsm := ""
		if vin.ScriptSig != nil {
			scriptHex = vin.ScriptSig.Hex
			scriptAsm = vin.ScriptSig.Asm
		}

		input := model.BTCTransactionInput{
			Network:      network,
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
				return nil, 0, fmt.Errorf("resolve prev outputs for tx %s: %w", vin.Txid, err)
			}
			if int(vin.Vout) >= len(prevOutputs) {
				return nil, 0, fmt.Errorf("input references missing vout %d in tx %s", vin.Vout, vin.Txid)
			}
			prevOut := prevOutputs[vin.Vout]
			input.Value = prevOut.Value
			input.Addresses = append([]string(nil), prevOut.Addresses...)
			total += prevOut.Value
		}

		inputs = append(inputs, input)
	}

	return inputs, total, nil
}

func (s *BTCHistorySyncService) flushBatches(
	ctx context.Context,
	blocks []model.BTCBlock,
	txs []model.BTCTransaction,
	inputs []model.BTCTransactionInput,
	outputs []model.BTCTransactionOutput,
) error {
	if len(blocks) > 0 {
		if err := s.repo.InsertBlocks(ctx, blocks); err != nil {
			return fmt.Errorf("batch insert blocks: %w", err)
		}
	}
	if len(txs) > 0 {
		if err := s.repo.InsertTransactions(ctx, txs); err != nil {
			return fmt.Errorf("batch insert transactions: %w", err)
		}
	}
	if len(outputs) > 0 {
		if err := s.repo.InsertTransactionOutputs(ctx, outputs); err != nil {
			return fmt.Errorf("batch insert outputs: %w", err)
		}
	}
	if len(inputs) > 0 {
		if err := s.repo.InsertTransactionInputs(ctx, inputs); err != nil {
			return fmt.Errorf("batch insert inputs: %w", err)
		}
	}
	return nil
}

func parseBits(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(parsed), nil
}

func sumOutputValues(outputs []model.BTCTransactionOutput) uint64 {
	var total uint64
	for _, out := range outputs {
		total += out.Value
	}
	return total
}

func btcToSatoshis(value float64) (uint64, error) {
	amt, err := btcutil.NewAmount(value)
	if err != nil {
		return 0, err
	}
	if amt < 0 {
		return 0, fmt.Errorf("negative amount: %d", amt)
	}
	return uint64(amt), nil
}

func isCoinbaseTx(tx btcjson.TxRawResult) bool {
	switch len(tx.Vin) {
	case 0:
		return false
	case 1:
		return tx.Vin[0].IsCoinBase()
	default:
		return false
	}
}
