package ingester

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"go.uber.org/zap"
)

func Test_historyBlockProcessor_processHeight(t *testing.T) {
	type testCase struct {
		name          string
		setupMocks    func(ctx context.Context, ctrl *gomock.Controller) (*historyBlockProcessor, uint64)
		wantErrSubstr string
	}

	tests := []testCase{
		{
			name: "success writes block and records metrics",
			setupMocks: func(ctx context.Context, ctrl *gomock.Controller) (*historyBlockProcessor, uint64) {
				height := uint64(42)
				source := NewMockHistorySource(ctrl)
				writer := NewMockBlockWriter(ctrl)
				metrics := NewMockHistoryIngesterMetrics(ctrl)

				source.EXPECT().
					FetchBlock(ctx, height).
					Return(&chain.HistoryBlock{
						Block: model.Block{Height: height},
						Txs:   []model.Transaction{{TxID: "tx1"}},
						Outputs: []model.TransactionOutput{
							{TxID: "tx1", Index: 0},
						},
					}, nil)
				writer.EXPECT().
					WriteBlock(ctx, model.InsertBlock{
						Block:   model.Block{Height: height},
						Txs:     []model.Transaction{{TxID: "tx1"}},
						Outputs: []model.TransactionOutput{{TxID: "tx1", Index: 0}},
					}).
					Return(nil)
				metrics.EXPECT().
					ObserveProcessHeight(nil, height, gomock.Any())

				return &historyBlockProcessor{
					source:      source,
					blockWriter: writer,
					metrics:     metrics,
					logger:      zap.NewNop(),
				}, height
			},
		},
		{
			name: "fetch block error bubbles with height and metrics",
			setupMocks: func(ctx context.Context, ctrl *gomock.Controller) (*historyBlockProcessor, uint64) {
				height := uint64(7)
				source := NewMockHistorySource(ctrl)
				metrics := NewMockHistoryIngesterMetrics(ctrl)

				source.EXPECT().
					FetchBlock(ctx, height).
					Return(nil, errors.New("fetch failed"))
				metrics.EXPECT().
					ObserveProcessHeight(gomock.Any(), height, gomock.Any())

				return &historyBlockProcessor{
					source:  source,
					metrics: metrics,
					logger:  zap.NewNop(),
				}, height
			},
			wantErrSubstr: "fetch block height 7",
		},
		{
			name: "write block error bubbles with height and metrics",
			setupMocks: func(ctx context.Context, ctrl *gomock.Controller) (*historyBlockProcessor, uint64) {
				height := uint64(9)
				source := NewMockHistorySource(ctrl)
				writer := NewMockBlockWriter(ctrl)
				metrics := NewMockHistoryIngesterMetrics(ctrl)

				source.EXPECT().
					FetchBlock(ctx, height).
					Return(&chain.HistoryBlock{
						Block: model.Block{Height: height},
						Txs:   []model.Transaction{{TxID: "tx9"}},
						Outputs: []model.TransactionOutput{
							{TxID: "tx9", Index: 0},
						},
					}, nil)
				writer.EXPECT().
					WriteBlock(ctx, gomock.Any()).
					Return(errors.New("write failed"))
				metrics.EXPECT().
					ObserveProcessHeight(gomock.Any(), height, gomock.Any())

				return &historyBlockProcessor{
					source:      source,
					blockWriter: writer,
					metrics:     metrics,
					logger:      zap.NewNop(),
				}, height
			},
			wantErrSubstr: "write block height 9",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			p, height := tt.setupMocks(ctx, ctrl)

			err := p.processHeight(ctx, height)
			if tt.wantErrSubstr == "" && err != nil {
				t.Fatalf("processHeight unexpected error: %v", err)
			}
			if tt.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSubstr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErrSubstr, err)
				}
			}
		})
	}
}

func Test_historyBlockProcessor_Process(t *testing.T) {
	t.Parallel()

	type args struct {
		ctx     context.Context
		heights []uint64
	}
	tests := []struct {
		name          string
		prepare       func(ctrl *gomock.Controller) (*historyBlockProcessor, args, *int)
		wantErrSubstr string
		wantCancel    bool
	}{
		{
			name: "processes all heights successfully",
			prepare: func(ctrl *gomock.Controller) (*historyBlockProcessor, args, *int) {
				source := NewMockHistorySource(ctrl)
				writer := NewMockBlockWriter(ctrl)
				heights := []uint64{1, 2, 3}
				ctx := context.Background()

				for _, h := range heights {
					source.EXPECT().
						FetchBlock(gomock.Any(), h).
						Return(&chain.HistoryBlock{
							Block: model.Block{Height: h},
						}, nil)
					writer.EXPECT().
						WriteBlock(gomock.Any(), model.InsertBlock{
							Block: model.Block{Height: h},
						}).
						Return(nil)
				}

				return &historyBlockProcessor{
					workerCount: 2,
					source:      source,
					blockWriter: writer,
					logger:      zap.NewNop(),
				}, args{ctx: ctx, heights: heights}, nil
			},
		},
		{
			name: "write error cancels batcher and returns error",
			prepare: func(ctrl *gomock.Controller) (*historyBlockProcessor, args, *int) {
				source := NewMockHistorySource(ctrl)
				writer := NewMockBlockWriter(ctrl)
				heights := []uint64{5, 6}
				ctx := context.Background()

				source.EXPECT().
					FetchBlock(gomock.Any(), uint64(5)).
					Return(&chain.HistoryBlock{Block: model.Block{Height: 5}}, nil)
				writer.EXPECT().
					WriteBlock(gomock.Any(), model.InsertBlock{Block: model.Block{Height: 5}}).
					Return(fmt.Errorf("boom"))

				source.EXPECT().
					FetchBlock(gomock.Any(), uint64(6)).
					AnyTimes().
					Return(&chain.HistoryBlock{Block: model.Block{Height: 6}}, nil)
				writer.EXPECT().
					WriteBlock(gomock.Any(), model.InsertBlock{Block: model.Block{Height: 6}}).
					AnyTimes().
					Return(nil)

				cancelCalled := 0
				return &historyBlockProcessor{
					workerCount: 2,
					source:      source,
					blockWriter: writer,
					logger:      zap.NewNop(),
					cancel: func() {
						cancelCalled++
					},
				}, args{ctx: ctx, heights: heights}, &cancelCalled
			},
			wantErrSubstr: "write block height 5",
			wantCancel:    true,
		},
		{
			name: "fetch error cancels batcher and returns error",
			prepare: func(ctrl *gomock.Controller) (*historyBlockProcessor, args, *int) {
				source := NewMockHistorySource(ctrl)
				writer := NewMockBlockWriter(ctrl)
				ctx := context.Background()
				heights := []uint64{10}

				source.EXPECT().
					FetchBlock(gomock.Any(), uint64(10)).
					Return(nil, errors.New("fetch failed"))

				cancelCalled := 0
				return &historyBlockProcessor{
					workerCount: 1,
					source:      source,
					blockWriter: writer,
					logger:      zap.NewNop(),
					cancel: func() {
						cancelCalled++
					},
				}, args{ctx: ctx, heights: heights}, &cancelCalled
			},
			wantErrSubstr: "fetch block height 10",
			wantCancel:    true,
		},
		{
			name: "context canceled returns ctx error",
			prepare: func(ctrl *gomock.Controller) (*historyBlockProcessor, args, *int) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				source := NewMockHistorySource(ctrl)
				writer := NewMockBlockWriter(ctrl)
				source.EXPECT().FetchBlock(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, context.Canceled)
				writer.EXPECT().WriteBlock(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

				return &historyBlockProcessor{
					workerCount: 1,
					source:      source,
					blockWriter: writer,
					logger:      zap.NewNop(),
				}, args{ctx: ctx, heights: []uint64{1}}, nil
			},
			wantErrSubstr: context.Canceled.Error(),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			p, args, cancelCnt := tt.prepare(ctrl)
			err := p.Process(args.ctx, args.heights)
			if tt.wantErrSubstr == "" && err != nil {
				t.Fatalf("Process unexpected error: %v", err)
			}
			if tt.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErrSubstr)) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErrSubstr, err)
			}
			if tt.wantCancel && cancelCnt != nil && *cancelCnt == 0 {
				t.Fatalf("expected cancelBatcher to be called")
			}
		})
	}
}
