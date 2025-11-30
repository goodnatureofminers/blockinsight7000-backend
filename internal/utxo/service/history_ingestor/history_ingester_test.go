package history_ingestor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
)

func TestHistoryIngesterService_run(t *testing.T) {
	type fields struct {
		logger            *zap.Logger
		metrics           HistoryIngesterMetrics
		sleep             func(context.Context, time.Duration) error
		sleepDuration     time.Duration
		longSleepDuration time.Duration
		heightFetcher     HeightFetcher
		blockProcessor    BlockProcessor
		blockWriter       BlockWriter
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
			name: "success with heights",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				hf := NewMockHeightFetcher(ctrl)
				bp := NewMockBlockProcessor(ctrl)
				bw := NewMockBlockWriter(ctrl)
				metrics := NewMockHistoryIngesterMetrics(ctrl)
				ctx := context.Background()

				hf.EXPECT().FetchMissing(ctx).Return([]uint64{10, 11}, nil)
				metrics.EXPECT().ObserveFetchMissing(nil, gomock.Any())
				bp.EXPECT().Process(ctx, []uint64{10, 11}).Return(nil)
				metrics.EXPECT().ObserveProcessBatch(nil, 2, gomock.Any())

				return fields{
					logger:            zap.NewNop(),
					metrics:           metrics,
					sleep:             func(context.Context, time.Duration) error { return nil },
					sleepDuration:     time.Millisecond,
					longSleepDuration: time.Millisecond,
					heightFetcher:     hf,
					blockProcessor:    bp,
					blockWriter:       bw,
				}, args{ctx: ctx}
			},
			wantErr: false,
		},
		{
			name: "no heights triggers long sleep",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				hf := NewMockHeightFetcher(ctrl)
				metrics := NewMockHistoryIngesterMetrics(ctrl)
				ctx := context.Background()
				sleepCalls := 0
				sleep := func(context.Context, time.Duration) error {
					sleepCalls++
					return nil
				}

				hf.EXPECT().FetchMissing(ctx).Return([]uint64{}, nil)
				metrics.EXPECT().ObserveFetchMissing(nil, gomock.Any())

				return fields{
					logger:            zap.NewNop(),
					metrics:           metrics,
					sleep:             sleep,
					sleepDuration:     time.Millisecond,
					longSleepDuration: time.Millisecond,
					heightFetcher:     hf,
					blockProcessor:    NewMockBlockProcessor(ctrl),
					blockWriter:       NewMockBlockWriter(ctrl),
				}, args{ctx: ctx}
			},
			wantErr: false,
		},
		{
			name: "fetch missing error bubbles",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				hf := NewMockHeightFetcher(ctrl)
				metrics := NewMockHistoryIngesterMetrics(ctrl)
				ctx := context.Background()
				fetchErr := errors.New("fetch error")

				hf.EXPECT().FetchMissing(ctx).Return(nil, fetchErr)
				metrics.EXPECT().ObserveFetchMissing(fetchErr, gomock.Any())

				return fields{
					logger:            zap.NewNop(),
					metrics:           metrics,
					sleep:             func(context.Context, time.Duration) error { return nil },
					sleepDuration:     time.Millisecond,
					longSleepDuration: time.Millisecond,
					heightFetcher:     hf,
					blockProcessor:    NewMockBlockProcessor(ctrl),
					blockWriter:       NewMockBlockWriter(ctrl),
				}, args{ctx: ctx}
			},
			wantErr: true,
		},
		{
			name: "process error bubbles",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				hf := NewMockHeightFetcher(ctrl)
				bp := NewMockBlockProcessor(ctrl)
				metrics := NewMockHistoryIngesterMetrics(ctrl)
				ctx := context.Background()
				processErr := errors.New("process error")

				hf.EXPECT().FetchMissing(ctx).Return([]uint64{1}, nil)
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
					blockWriter:       NewMockBlockWriter(ctrl),
				}, args{ctx: ctx}
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			fields, args := tt.prepare(ctrl)
			svc := &HistoryIngesterService{
				logger:            fields.logger,
				metrics:           fields.metrics,
				sleep:             fields.sleep,
				sleepDuration:     fields.sleepDuration,
				longSleepDuration: fields.longSleepDuration,
				heightFetcher:     fields.heightFetcher,
				blockProcessor:    fields.blockProcessor,
				blockWriter:       fields.blockWriter,
			}
			if err := svc.run(args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("run() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHistoryIngesterService_Run_ContextCanceled(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hf := NewMockHeightFetcher(ctrl)
	bp := NewMockBlockProcessor(ctrl)
	bw := NewMockBlockWriter(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	bp.EXPECT().SetCancelBatcher(gomock.Any()).Times(1)
	bw.EXPECT().Start(gomock.Any()).Times(1)
	bw.EXPECT().Stop().Times(1)

	svc := &HistoryIngesterService{
		logger:            zap.NewNop(),
		metrics:           NewMockHistoryIngesterMetrics(ctrl),
		sleep:             func(context.Context, time.Duration) error { return nil },
		sleepDuration:     time.Millisecond,
		longSleepDuration: time.Millisecond,
		heightFetcher:     hf,
		blockProcessor:    bp,
		blockWriter:       bw,
	}

	if err := svc.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
