package ingester

import (
	"context"
	"errors"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/clock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"go.uber.org/zap"
)

type BackfillIngesterService struct {
	logger                 *zap.Logger
	coin                   model.Coin
	network                model.Network
	metrics                BackfillIngesterMetrics
	sleep                  func(context.Context, time.Duration) error
	idleSleepDuration      time.Duration
	postBatchSleepDuration time.Duration
	heightFetcher          HeightFetcher
	blockProcessor         BlockProcessor
	blockWriter            BlockWriter
}

func NewBackfillIngesterService(
	repo ClickhouseRepository,
	source BackfillSource,
	metrics BackfillIngesterMetrics,
	coin model.Coin,
	network model.Network,
	logger *zap.Logger,
) (*BackfillIngesterService, error) {
	logger = logger.With(
		zap.String("coin", string(coin)),
		zap.String("network", string(network)),
	)

	if metrics == nil {
		return nil, errors.New("backfill ingester metrics is required")
	}

	bw := newBackfillBlockWriter(repo, logger)

	return &BackfillIngesterService{
		logger:                 logger,
		coin:                   coin,
		network:                network,
		metrics:                metrics,
		sleep:                  clock.SleepWithContext,
		idleSleepDuration:      idleSleepDuration,
		postBatchSleepDuration: postBatchSleepDuration,
		heightFetcher: &backfillHeightFetcher{
			repository: repo,
			coin:       coin,
			network:    network,
			limit:      randomHeightLimit,
		},
		blockWriter: bw,
		blockProcessor: &backfillBlockProcessor{
			workerCount: defaultWorkerCount,
			source:      source,
			blockWriter: bw,
			metrics:     metrics,
			logger:      logger.Named("blockProcessor"),
		},
	}, nil
}

func (s *BackfillIngesterService) Run(ctx context.Context) error {
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
			if sleepErr := s.sleep(ctx, s.idleSleepDuration); sleepErr != nil {
				return sleepErr
			}
		}
	}
}

func (s *BackfillIngesterService) run(ctx context.Context) error {
	started := time.Now()
	heights, err := s.heightFetcher.Fetch(ctx)
	s.observeFetch(err, started)
	if err != nil {
		s.logger.Error("fetch unprocessed heights failed", zap.Error(err))
		return err
	}

	if len(heights) == 0 {
		s.logger.Info("no missing block heights; going idle", zap.Duration("sleep", s.idleSleepDuration))
		return s.sleep(ctx, s.idleSleepDuration)
	}

	s.logger.Info("processing batch", zap.Int("height_count", len(heights)))
	started = time.Now()
	if err := s.blockProcessor.Process(ctx, heights); err != nil {
		s.observeProcessBatch(err, len(heights), started)
		s.logger.Error("process batch failed", zap.Int("height_count", len(heights)), zap.Error(err))
		return err
	}
	s.observeProcessBatch(nil, len(heights), started)

	return s.sleep(ctx, s.postBatchSleepDuration)
}

func (s *BackfillIngesterService) observeFetch(err error, started time.Time) {
	s.metrics.ObserveFetchMissing(err, started)
}

func (s *BackfillIngesterService) observeProcessBatch(err error, heights int, started time.Time) {
	s.metrics.ObserveProcessBatch(err, heights, started)
}
