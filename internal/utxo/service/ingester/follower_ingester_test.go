package ingester

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
)

func TestFollowerIngesterService_run(t *testing.T) {
	t.Parallel()

	type fields struct {
		logger            *zap.Logger
		metrics           FollowerIngesterMetrics
		sleep             func(context.Context, time.Duration) error
		sleepDuration     time.Duration
		longSleepDuration time.Duration
		heightFetcher     HeightFetcher
		blockProcessor    BlockProcessor
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		prepare func(ctrl *gomock.Controller) (fields, args)
		wantErr bool
	}{
		{
			name: "processes available heights",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				hf := NewMockHeightFetcher(ctrl)
				bp := NewMockBlockProcessor(ctrl)
				metrics := NewMockFollowerIngesterMetrics(ctrl)
				ctx := context.Background()

				hf.EXPECT().Fetch(ctx).Return([]uint64{5, 6}, nil)
				metrics.EXPECT().ObserveFetchMissing(nil, gomock.Any())
				bp.EXPECT().Process(ctx, []uint64{5, 6}).Return(nil)
				metrics.EXPECT().ObserveProcessBatch(nil, 2, gomock.Any())

				return fields{
					logger:            zap.NewNop(),
					metrics:           metrics,
					sleep:             func(context.Context, time.Duration) error { return nil },
					sleepDuration:     time.Millisecond,
					longSleepDuration: time.Millisecond,
					heightFetcher:     hf,
					blockProcessor:    bp,
				}, args{ctx: ctx}
			},
		},
		{
			name: "sleeps when no heights",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				hf := NewMockHeightFetcher(ctrl)
				metrics := NewMockFollowerIngesterMetrics(ctrl)
				ctx := context.Background()
				sleepCalls := 0
				sleep := func(context.Context, time.Duration) error {
					sleepCalls++
					return nil
				}

				hf.EXPECT().Fetch(ctx).Return([]uint64{}, nil)
				metrics.EXPECT().ObserveFetchMissing(nil, gomock.Any())

				return fields{
					logger:            zap.NewNop(),
					metrics:           metrics,
					sleep:             sleep,
					sleepDuration:     time.Millisecond,
					longSleepDuration: time.Millisecond,
					heightFetcher:     hf,
					blockProcessor:    NewMockBlockProcessor(ctrl),
				}, args{ctx: ctx}
			},
		},
		{
			name: "returns fetch error",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				hf := NewMockHeightFetcher(ctrl)
				metrics := NewMockFollowerIngesterMetrics(ctrl)
				ctx := context.Background()
				fetchErr := errors.New("fetch failed")

				hf.EXPECT().Fetch(ctx).Return(nil, fetchErr)
				metrics.EXPECT().ObserveFetchMissing(fetchErr, gomock.Any())

				return fields{
					logger:            zap.NewNop(),
					metrics:           metrics,
					sleep:             func(context.Context, time.Duration) error { return nil },
					sleepDuration:     time.Millisecond,
					longSleepDuration: time.Millisecond,
					heightFetcher:     hf,
					blockProcessor:    NewMockBlockProcessor(ctrl),
				}, args{ctx: ctx}
			},
			wantErr: true,
		},
		{
			name: "returns process error",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				hf := NewMockHeightFetcher(ctrl)
				bp := NewMockBlockProcessor(ctrl)
				metrics := NewMockFollowerIngesterMetrics(ctrl)
				ctx := context.Background()
				processErr := errors.New("process failed")

				hf.EXPECT().Fetch(ctx).Return([]uint64{1}, nil)
				metrics.EXPECT().ObserveFetchMissing(nil, gomock.Any())
				bp.EXPECT().Process(ctx, []uint64{1}).Return(processErr)
				metrics.EXPECT().ObserveProcessBatch(processErr, 1, gomock.Any())

				return fields{
					logger:            zap.NewNop(),
					metrics:           metrics,
					sleep:             func(context.Context, time.Duration) error { return nil },
					sleepDuration:     time.Millisecond,
					longSleepDuration: time.Millisecond,
					heightFetcher:     hf,
					blockProcessor:    bp,
				}, args{ctx: ctx}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			fields, args := tt.prepare(ctrl)
			svc := &FollowerIngesterService{
				logger:            fields.logger,
				metrics:           fields.metrics,
				sleep:             fields.sleep,
				sleepDuration:     fields.sleepDuration,
				longSleepDuration: fields.longSleepDuration,
				heightFetcher:     fields.heightFetcher,
				blockProcessor:    fields.blockProcessor,
			}
			if err := svc.run(args.ctx); (err != nil) != tt.wantErr {
				t.Fatalf("run() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
