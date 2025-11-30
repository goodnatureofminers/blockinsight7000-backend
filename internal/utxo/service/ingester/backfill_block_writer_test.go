package ingester

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/batcher"
	"go.uber.org/zap"
)

func Test_backfillBlockWriter_flush(t *testing.T) {
	type fields struct {
		repo         ClickhouseRepository
		logger       *zap.Logger
		blockBatcher *batcher.Batcher[model.InsertBlock]
	}
	type args struct {
		ctx          context.Context
		insertBlocks []model.InsertBlock
	}
	tests := []struct {
		name    string
		prepare func(ctrl *gomock.Controller) (fields, args)
		wantErr bool
	}{
		{
			name: "success flushes blocks/inputs",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				logger := zap.NewNop()
				w := newBackfillBlockWriter(repo, logger)

				insertBlocks := []model.InsertBlock{
					{
						Block: model.Block{Height: 1},
						Inputs: []model.TransactionInput{
							{TxID: "tx1", Index: 0},
						},
					},
					{
						Block: model.Block{Height: 2},
						Inputs: []model.TransactionInput{
							{TxID: "tx2", Index: 0},
						},
					},
				}

				ctx := context.Background()
				repo.EXPECT().InsertTransactionInputs(ctx, []model.TransactionInput{
					{TxID: "tx1", Index: 0},
					{TxID: "tx2", Index: 0},
				}).Return(nil)
				repo.EXPECT().InsertBlocks(ctx, []model.Block{
					{Height: 1},
					{Height: 2},
				}).Return(nil)

				return fields{repo: repo, logger: logger, blockBatcher: w.blockBatcher}, args{ctx: ctx, insertBlocks: insertBlocks}
			},
			wantErr: false,
		},
		{
			name: "flushes inputs in batches when threshold exceeded",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				logger := zap.NewNop()
				w := newBackfillBlockWriter(repo, logger)

				inputs1 := make([]model.TransactionInput, inputFlushThreshold)
				for i := 0; i < inputFlushThreshold; i++ {
					inputs1[i] = model.TransactionInput{TxID: "tx", Index: uint32(i)}
				}

				insertBlocks := []model.InsertBlock{
					{
						Block:  model.Block{Height: 1},
						Inputs: inputs1,
					},
					{
						Block:  model.Block{Height: 2},
						Inputs: []model.TransactionInput{{TxID: "tail", Index: 0}},
					},
				}

				ctx := context.Background()
				gomock.InOrder(
					repo.EXPECT().InsertTransactionInputs(ctx, inputs1).Return(nil),
					repo.EXPECT().InsertTransactionInputs(ctx, []model.TransactionInput{{TxID: "tail", Index: 0}}).Return(nil),
					repo.EXPECT().InsertBlocks(ctx, []model.Block{{Height: 1}, {Height: 2}}).Return(nil),
				)

				return fields{repo: repo, logger: logger, blockBatcher: w.blockBatcher}, args{ctx: ctx, insertBlocks: insertBlocks}
			},
			wantErr: false,
		},
		{
			name: "returns error on failed insert",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				logger := zap.NewNop()
				w := newBackfillBlockWriter(repo, logger)

				insertBlocks := []model.InsertBlock{
					{
						Block: model.Block{Height: 1},
						Inputs: []model.TransactionInput{
							{TxID: "tx1", Index: 0},
						},
					},
				}
				ctx := context.Background()
				repo.EXPECT().InsertTransactionInputs(ctx, gomock.Any()).Return(errors.New("insert inputs failed"))

				return fields{repo: repo, logger: logger, blockBatcher: w.blockBatcher}, args{ctx: ctx, insertBlocks: insertBlocks}
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
			w := &backfillBlockWriter{
				repo:         fields.repo,
				logger:       fields.logger,
				blockBatcher: fields.blockBatcher,
			}
			if err := w.flush(args.ctx, args.insertBlocks); (err != nil) != tt.wantErr {
				t.Errorf("flush() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_backfillBlockWriter_WriteBlock(t *testing.T) {
	type args struct {
		ctx context.Context
		b   model.InsertBlock
	}
	tests := []struct {
		name    string
		prepare func(ctrl *gomock.Controller) (*backfillBlockWriter, args)
		wantErr bool
	}{
		{
			name: "returns ctx error when canceled",
			prepare: func(ctrl *gomock.Controller) (*backfillBlockWriter, args) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				repo := NewMockClickhouseRepository(ctrl)
				w := newBackfillBlockWriter(repo, zap.NewNop())
				return w, args{ctx: ctx, b: model.InsertBlock{}}
			},
			wantErr: true,
		},
		{
			name: "adds block to batcher on success",
			prepare: func(ctrl *gomock.Controller) (*backfillBlockWriter, args) {
				repo := NewMockClickhouseRepository(ctrl)
				w := newBackfillBlockWriter(repo, zap.NewNop())
				return w, args{ctx: context.Background(), b: model.InsertBlock{Block: model.Block{Height: 1}}}
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			w, args := tt.prepare(ctrl)
			if err := w.WriteBlock(args.ctx, args.b); (err != nil) != tt.wantErr {
				t.Errorf("WriteBlock() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
