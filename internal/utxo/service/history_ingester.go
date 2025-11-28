package service

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/pkg/batcher"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utils"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"go.uber.org/zap"
)

const (
	defaultWorkerCount        = 50
	randomMissingHeightsLimit = 10000

	transactionFlushThreshold = 1000
	outputFlushThreshold      = 1000

	blockBatcherCapacity      = 500
	blockBatcherFlushInterval = 30 * time.Second
	blockBatcherWorkerCount   = 1

	idleSleepDuration      = time.Minute
	postBatchSleepDuration = 5 * time.Second
)

type HistoryIngesterService struct {
	repo         ClickhouseRepository
	rpc          RpcClient
	logger       *zap.Logger
	coin         model.Coin
	network      model.Network
	decoder      *scriptDecoder
	blockBatcher *batcher.Batcher[model.InsertBlock]
	workerCount  int
}

func NewHistoryIngesterService(
	repo ClickhouseRepository,
	rpc RpcClient,
	coin model.Coin,
	network model.Network,
	logger *zap.Logger,
) (*HistoryIngesterService, error) {

	decoder, err := newScriptDecoder(network)
	if err != nil {
		return nil, err
	}

	return &HistoryIngesterService{
		repo:        repo,
		rpc:         rpc,
		logger:      logger,
		coin:        coin,
		network:     network,
		decoder:     decoder,
		workerCount: defaultWorkerCount,
		blockBatcher: batcher.New[model.InsertBlock](
			logger.Named("blockBatcher"),
			func(ctx context.Context, insertBlocks []model.InsertBlock) error {
				blocks := make([]model.Block, 0, len(insertBlocks))
				txs := make([]model.Transaction, 0, len(insertBlocks))
				outputs := make([]model.TransactionOutput, 0, len(insertBlocks))
				for _, block := range insertBlocks {
					blocks = append(blocks, block.Block)
					txs = append(txs, block.Txs...)
					if len(txs) >= transactionFlushThreshold {
						err := repo.InsertTransactions(ctx, txs)
						if err != nil {
							return err
						}
						logger.Debug("InsertTransactions")
						txs = make([]model.Transaction, 0, len(insertBlocks))
					}
					outputs = append(outputs, block.Outputs...)
					if len(outputs) >= outputFlushThreshold {
						err = repo.InsertTransactionOutputs(ctx, outputs)
						if err != nil {
							return err
						}
						logger.Debug("InsertTransactionOutputs")
						outputs = make([]model.TransactionOutput, 0, len(insertBlocks))
					}
				}

				err := repo.InsertTransactions(ctx, txs)
				if err != nil {
					return err
				}
				err = repo.InsertTransactionOutputs(ctx, outputs)
				if err != nil {
					return err
				}

				return repo.InsertBlocks(ctx, blocks)
			},
			blockBatcherCapacity,
			blockBatcherFlushInterval,
			blockBatcherWorkerCount,
		),
	}, nil
}

func (s *HistoryIngesterService) Run(ctx context.Context) error {
	s.blockBatcher.Start(ctx)
	defer s.blockBatcher.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		latestHeight, err := s.rpc.GetBlockCount()
		if err != nil {
			return err
		}
		heights, err := s.repo.RandomMissingBlockHeights(ctx, s.coin, s.network, uint64(latestHeight), randomMissingHeightsLimit)
		if err != nil {
			return err
		}

		if len(heights) == 0 {
			s.logger.Info("no missing block heights; going idle", zap.Duration("sleep", idleSleepDuration))
			if err := utils.SleepWithContext(ctx, idleSleepDuration); err != nil {
				return err
			}
			continue
		}

		s.logger.Info("starting sync batch", zap.Int("height_count", len(heights)))

		err = s.processHeightsWithWorkers(ctx, heights)
		if err != nil {
			return err
		}
		s.logger.Info("completed sync batch", zap.Duration("sleep", postBatchSleepDuration))
		if err := utils.SleepWithContext(ctx, postBatchSleepDuration); err != nil {
			return err
		}
	}
}

func (s *HistoryIngesterService) processHeightsWithWorkers(ctx context.Context, heights []uint64) error {
	tasks := make(chan uint64)
	errs := make(chan error, s.workerCount)
	wg := sync.WaitGroup{}

	for w := 0; w < s.workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case h, ok := <-tasks:
					if !ok {
						return
					}
					if err := s.processBlock(ctx, h); err != nil {
						errs <- err
						return
					}
				}
			}
		}()
	}

	go func() {
		for _, h := range heights {
			select {
			case <-ctx.Done():
				return
			case tasks <- h:
			}
		}
		close(tasks)
	}()

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *HistoryIngesterService) processBlock(
	_ context.Context,
	height uint64,
) error {
	hash, err := s.rpc.GetBlockHash(int64(height))
	if err != nil {
		return fmt.Errorf("get block hash at height %d: %w", height, err)
	}
	src, err := s.rpc.GetBlockVerboseTx(hash)
	if err != nil {
		return fmt.Errorf("get block %s: %w", hash, err)
	}
	var block model.Block
	bits, err := utils.ParseBits(src.Bits)
	if err != nil {
		return fmt.Errorf("block %d bits parse: %w", src.Height, err)
	}
	if src.Height > math.MaxUint32 {
		return fmt.Errorf("block height %d exceeds uint32", src.Height)
	}
	if src.Size < 0 {
		return fmt.Errorf("block %d negative size: %d", src.Height, src.Size)
	}

	timestamp := time.Unix(src.Time, 0).UTC()
	block = model.Block{
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
	}

	totalOutputs := 0
	for _, tx := range src.Tx {
		totalOutputs += len(tx.Vout)
	}

	outputs := make([]model.TransactionOutput, 0, totalOutputs)
	for _, tx := range src.Tx {
		txOutputs, err := s.convertOutputs(tx, block.Height, timestamp)
		if err != nil {
			return err
		}
		outputs = append(outputs, txOutputs...)
	}
	txs := make([]model.Transaction, 0, len(src.Tx))

	for _, tx := range src.Tx {

		if len(tx.Vin) > math.MaxUint16 {
			return fmt.Errorf("tx %s vin count overflow: %d", tx.Txid, len(tx.Vin))
		}
		if len(tx.Vout) > math.MaxUint16 {
			return fmt.Errorf("tx %s vout count overflow: %d", tx.Txid, len(tx.Vout))
		}
		if tx.Size < 0 {
			return fmt.Errorf("tx %s negative size: %d", tx.Txid, tx.Size)
		}
		if tx.Vsize < 0 {
			return fmt.Errorf("tx %s negative vsize: %d", tx.Txid, tx.Vsize)
		}

		txs = append(txs, model.Transaction{
			Coin:        s.coin,
			Network:     s.network,
			TxID:        tx.Txid,
			BlockHeight: block.Height,
			Timestamp:   timestamp,
			Size:        uint32(tx.Size),
			VSize:       uint32(tx.Vsize),
			Version:     tx.Version,
			LockTime:    tx.LockTime,
			InputCount:  uint16(len(tx.Vin)),
			OutputCount: uint16(len(tx.Vout)),
		})
	}

	s.blockBatcher.Add(model.InsertBlock{
		Block:   block,
		Txs:     txs,
		Outputs: outputs,
	})

	return nil
}

func (s *HistoryIngesterService) convertOutputs(
	tx btcjson.TxRawResult,
	blockHeight uint64,
	blockTime time.Time,
) ([]model.TransactionOutput, error) {
	outputs := make([]model.TransactionOutput, 0, len(tx.Vout))
	for idx, vout := range tx.Vout {
		if vout.Value < 0 {
			return nil, fmt.Errorf("tx %s output %d negative value: %f", tx.Txid, idx, vout.Value)
		}

		value, err := utils.BtcToSatoshis(vout.Value)
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
