package service

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utils"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/batcher"
	"go.uber.org/zap"
)

const (
	defaultBackfillIngesterWorkerCount = 50
	randomUnprocessedHeightsLimit      = 10000
	inputFlushThreshold                = 1000
)

type BackfillIngesterService struct {
	repo         ClickhouseRepository
	rpc          RpcClient
	logger       *zap.Logger
	coin         model.Coin
	network      model.Network
	decoder      *scriptDecoder
	blockBatcher *batcher.Batcher[model.InsertBlock]
	workerCount  int
}

func NewBackfillIngesterService(
	repo ClickhouseRepository,
	rpc RpcClient,
	coin model.Coin,
	network model.Network,
	logger *zap.Logger,
) (*BackfillIngesterService, error) {

	decoder, err := newScriptDecoder(network)
	if err != nil {
		return nil, err
	}

	return &BackfillIngesterService{
		repo:        repo,
		rpc:         rpc,
		logger:      logger,
		coin:        coin,
		network:     network,
		decoder:     decoder,
		workerCount: defaultBackfillIngesterWorkerCount,
		blockBatcher: batcher.New[model.InsertBlock](
			logger.Named("blockBatcher"),
			func(ctx context.Context, insertBlocks []model.InsertBlock) error {
				blocks := make([]model.Block, 0, len(insertBlocks))
				inputs := make([]model.TransactionInput, 0, len(insertBlocks))
				for _, block := range insertBlocks {
					blocks = append(blocks, block.Block)
					inputs = append(inputs, block.Inputs...)
					if len(inputs) >= inputFlushThreshold {
						err = repo.InsertTransactionInputs(ctx, inputs)
						if err != nil {
							return err
						}
						logger.Debug("InsertTransactionInputs")
						inputs = make([]model.TransactionInput, 0, len(insertBlocks))
					}
				}
				err = repo.InsertTransactionInputs(ctx, inputs)
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

func (s *BackfillIngesterService) Run(ctx context.Context) error {
	s.blockBatcher.Start(ctx)
	defer s.blockBatcher.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		maxHaight, err := s.repo.MaxContiguousBlockHeight(ctx, s.coin, s.network)
		if err != nil {
			return err
		}

		heights, err := s.repo.RandomUnprocessedBlockHeights(ctx, s.coin, s.network, maxHaight, randomUnprocessedHeightsLimit)
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

func (s *BackfillIngesterService) processHeightsWithWorkers(ctx context.Context, heights []uint64) error {
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

func (s *BackfillIngesterService) processBlock(
	ctx context.Context,
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
	block := model.Block{
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
	}

	resolver := newTransactionOutputResolver(s.repo, s.coin, s.network)

	totalOutputs := 0
	totalInputs := 0
	for _, tx := range src.Tx {
		totalOutputs += len(tx.Vout)
		totalInputs += len(tx.Vin)
	}

	inputs := make([]model.TransactionInput, 0, totalInputs)

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

		txInputs, err := convertInputs(ctx, resolver, tx, s.coin, s.network, block.Height, timestamp)
		if err != nil {
			return err
		}
		inputs = append(inputs, txInputs...)
	}

	s.blockBatcher.Add(model.InsertBlock{
		Block:  block,
		Inputs: inputs,
	})

	return nil
}

func convertInputs(
	ctx context.Context,
	resolver *transactionOutputResolver,
	tx btcjson.TxRawResult,
	coin model.Coin,
	network model.Network,
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
			Coin:         coin,
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
