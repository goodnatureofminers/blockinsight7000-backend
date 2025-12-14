package ingester

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/batcher"
	"go.uber.org/zap"
)

type followerBlockProcessor struct {
	repo    ClickhouseRepository
	coin    model.Coin
	network model.Network
	metrics FollowerIngesterMetrics
	logger  *zap.Logger
	cancel  func()
}

func (p *followerBlockProcessor) SetCancel(cancel func()) {
	p.cancel = cancel
}

func (p *followerBlockProcessor) Process(ctx context.Context, heights []uint64) (err error) {
	started := time.Now()
	defer func() {
		p.observeBatch(err, len(heights), started)
	}()

	if len(heights) == 0 {
		return nil
	}

	placeholder := strings.Repeat("0", 64)
	ts := time.Unix(0, 0).UTC()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var flushErr error
	var flushMu sync.Mutex
	var wg sync.WaitGroup

	recordErr := func(e error) {
		flushMu.Lock()
		defer flushMu.Unlock()
		if flushErr == nil {
			flushErr = e
			cancel()
		}
	}

	b := batcher.New[model.Block](
		p.logger.Named("blockBatcher"),
		func(ctx context.Context, blocks []model.Block) error {
			if err := p.repo.InsertBlocks(ctx, blocks); err != nil {
				if p.logger != nil {
					p.logger.Error("insert placeholder blocks failed", zap.Error(err))
				}
				recordErr(err)
				wg.Add(-len(blocks))
				return err
			}
			wg.Add(-len(blocks))
			return nil
		},
		newBlockBatcherCapacity,
		followerBatcherFlushInterval,
		blockBatcherWorkerCount,
	)
	b.Start(ctx)
	defer b.Stop()

	for _, h := range heights {
		if err := ctx.Err(); err != nil {
			return err
		}
		wg.Add(1)
		block := model.Block{
			Coin:       p.coin,
			Network:    p.network,
			Height:     h,
			Hash:       placeholder,
			Timestamp:  ts,
			Version:    0,
			MerkleRoot: placeholder,
			Bits:       0,
			Nonce:      0,
			Difficulty: 0,
			Size:       0,
			TXCount:    0,
			Status:     model.BlockNew,
		}
		if err := b.Add(ctx, block); err != nil {
			wg.Done()
			return err
		}
	}

	wg.Wait()
	if flushErr != nil {
		return fmt.Errorf("insert placeholder blocks: %w", flushErr)
	}
	return ctx.Err()
}

func (p *followerBlockProcessor) observeBatch(err error, heights int, started time.Time) {
	if p.metrics == nil {
		return
	}
	p.metrics.ObserveProcessBatch(err, heights, started)
}
