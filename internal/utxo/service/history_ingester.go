package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/clock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/batcher"
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
	source       chain.HistorySource
	logger       *zap.Logger
	coin         model.Coin
	network      model.Network
	blockBatcher *batcher.Batcher[model.InsertBlock]
	workerCount  int
}

func NewHistoryIngesterService(
	repo ClickhouseRepository,
	source chain.HistorySource,
	coin model.Coin,
	network model.Network,
	logger *zap.Logger,
) (*HistoryIngesterService, error) {

	return &HistoryIngesterService{
		repo:        repo,
		source:      source,
		logger:      logger,
		coin:        coin,
		network:     network,
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
						err := repo.InsertTransactionOutputs(ctx, outputs)
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

		latestHeight, err := s.source.LatestHeight(ctx)
		if err != nil {
			return err
		}
		heights, err := s.repo.RandomMissingBlockHeights(ctx, s.coin, s.network, uint64(latestHeight), randomMissingHeightsLimit)
		if err != nil {
			return err
		}

		if len(heights) == 0 {
			s.logger.Info("no missing block heights; going idle", zap.Duration("sleep", idleSleepDuration))
			if err := clock.SleepWithContext(ctx, idleSleepDuration); err != nil {
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
		if err := clock.SleepWithContext(ctx, postBatchSleepDuration); err != nil {
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
	ctx context.Context,
	height uint64,
) error {
	block, err := s.source.FetchBlock(ctx, height)
	if err != nil {
		return fmt.Errorf("fetch block %d: %w", height, err)
	}

	s.blockBatcher.Add(model.InsertBlock{
		Block:   block.Block,
		Txs:     block.Txs,
		Outputs: block.Outputs,
	})

	return nil
}
