package ingester

import (
	"context"
	"errors"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/clock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"go.uber.org/zap"
)

// FollowerIngesterService inserts placeholder blocks for new heights.
type FollowerIngesterService struct {
	logger            *zap.Logger
	coin              model.Coin
	network           model.Network
	metrics           FollowerIngesterMetrics
	sleep             func(context.Context, time.Duration) error
	sleepDuration     time.Duration
	longSleepDuration time.Duration
	heightFetcher     HeightFetcher
	blockProcessor    BlockProcessor
	blockSignal       <-chan struct{}
}

// NewFollowerIngesterService builds a FollowerIngesterService with dependencies.
func NewFollowerIngesterService(
	repo ClickhouseRepository,
	source HistorySource,
	metrics FollowerIngesterMetrics,
	coin model.Coin,
	network model.Network,
	logger *zap.Logger,
	blockSignal <-chan struct{},
) (*FollowerIngesterService, error) {
	logger = logger.With(
		zap.String("coin", string(coin)),
		zap.String("network", string(network)),
	)
	if metrics == nil {
		return nil, errors.New("follower ingester metrics is required")
	}

	return &FollowerIngesterService{
		logger:            logger,
		coin:              coin,
		network:           network,
		metrics:           metrics,
		sleep:             clock.SleepWithContext,
		sleepDuration:     sleepDuration,
		longSleepDuration: longSleepDuration,
		blockSignal:       blockSignal,
		heightFetcher: &followerHeightFetcher{
			source:     source,
			repository: repo,
			coin:       coin,
			network:    network,
			limit:      randomHeightLimit,
		},
		blockProcessor: &followerBlockProcessor{
			repo:    repo,
			coin:    coin,
			network: network,
			metrics: metrics,
			logger:  logger.Named("blockProcessor"),
		},
	}, nil
}

// Run starts the follower ingestion loop until the context is canceled.
func (s *FollowerIngesterService) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.run(ctx); err != nil {
			s.logger.Warn("run iteration failed, backing off", zap.Error(err), zap.Duration("sleep", s.sleepDuration))
			if sleepErr := s.wait(ctx, s.sleepDuration); sleepErr != nil {
				return sleepErr
			}
		}
	}
}

func (s *FollowerIngesterService) run(ctx context.Context) error {
	started := time.Now()
	heights, err := s.heightFetcher.Fetch(ctx)
	s.metrics.ObserveFetchMissing(err, started)
	if err != nil {
		s.logger.Error("fetch follower heights failed", zap.Error(err))
		return err
	}

	if len(heights) == 0 {
		s.logger.Debug("no new heights discovered; sleeping", zap.Duration("sleep", s.longSleepDuration))
		return s.wait(ctx, s.longSleepDuration)
	}

	s.logger.Info("adding placeholder blocks", zap.Int("heights", len(heights)))
	started = time.Now()
	if err = s.blockProcessor.Process(ctx, heights); err != nil {
		s.metrics.ObserveProcessBatch(err, len(heights), started)
		return err
	}
	s.metrics.ObserveProcessBatch(nil, len(heights), started)

	return s.wait(ctx, s.sleepDuration)
}

func (s *FollowerIngesterService) wait(ctx context.Context, d time.Duration) error {
	if s.blockSignal == nil {
		return s.sleep(ctx, d)
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.blockSignal:
		return nil
	case <-timer.C:
		return nil
	}
}
