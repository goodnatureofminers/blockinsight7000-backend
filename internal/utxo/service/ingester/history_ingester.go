package ingester

import (
	"context"
	"errors"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/clock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"go.uber.org/zap"
)

// HistoryIngesterService orchestrates historical block ingestion.
type HistoryIngesterService struct {
	logger            *zap.Logger
	coin              model.Coin
	network           model.Network
	metrics           HistoryIngesterMetrics
	sleep             func(context.Context, time.Duration) error
	sleepDuration     time.Duration
	longSleepDuration time.Duration
	heightFetcher     HeightFetcher
	blockProcessor    BlockProcessor
	blockWriter       BlockWriter
}

// NewHistoryIngesterService builds a HistoryIngesterService with the given dependencies.
func NewHistoryIngesterService(
	repo ClickhouseRepository,
	source HistorySource,
	metrics HistoryIngesterMetrics,
	coin model.Coin,
	network model.Network,
	logger *zap.Logger,
) (*HistoryIngesterService, error) {
	logger = logger.With(
		zap.String("coin", string(coin)),
		zap.String("network", string(network)),
	)
	if metrics == nil {
		return nil, errors.New("history ingester metrics is required")
	}

	bw := newHistoryBlockWriter(repo, logger)

	return &HistoryIngesterService{
		logger:            logger,
		coin:              coin,
		network:           network,
		metrics:           metrics,
		sleep:             clock.SleepWithContext,
		sleepDuration:     sleepDuration,
		longSleepDuration: longSleepDuration,
		heightFetcher: &historyHeightFetcher{
			repository: repo,
			coin:       coin,
			network:    network,
			limit:      randomHeightLimit,
		},
		blockWriter: bw,
		blockProcessor: &historyBlockProcessor{
			workerCount: defaultWorkerCount,
			source:      source,
			blockWriter: bw,
			metrics:     metrics,
			logger:      logger.Named("blockProcessor"),
		},
	}, nil
}

// Run starts the history ingestion loop until the context is canceled.
func (s *HistoryIngesterService) Run(ctx context.Context) error {
	bwCtx, bwCancel := context.WithCancel(ctx)
	s.blockProcessor.SetCancel(bwCancel)

	s.blockWriter.Start(bwCtx)
	defer func() {
		bwCancel()
		s.blockWriter.Stop()
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.run(ctx); err != nil {
			s.logger.Warn("run iteration failed, backing off", zap.Error(err), zap.Duration("sleep", sleepDuration))
			if sleepErr := s.sleep(ctx, s.sleepDuration); sleepErr != nil {
				return sleepErr
			}
		}
	}
}

func (s *HistoryIngesterService) run(ctx context.Context) (err error) {
	started := time.Now()
	heights, err := s.heightFetcher.Fetch(ctx)
	s.metrics.ObserveFetchMissing(err, started)
	if err != nil {
		s.logger.Error("fetch missing heights failed", zap.Error(err))
		return err
	}

	if len(heights) == 0 {
		s.logger.Debug("no missing block heights; sleeping", zap.Duration("sleep", s.longSleepDuration))
		if err = s.sleep(ctx, s.longSleepDuration); err != nil {
			return err
		}
		return nil
	}

	s.logger.Info("processing batch", zap.Int("heights", len(heights)))
	started = time.Now()
	if err = s.blockProcessor.Process(ctx, heights); err != nil {
		s.metrics.ObserveProcessBatch(err, len(heights), started)
		s.logger.Error("process batch failed", zap.Int("heights", len(heights)), zap.Error(err))
		return err
	}
	s.metrics.ObserveProcessBatch(nil, len(heights), started)

	if err = s.sleep(ctx, s.sleepDuration); err != nil {
		return err
	}
	return nil
}
