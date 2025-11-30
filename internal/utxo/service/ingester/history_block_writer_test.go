package ingester

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/batcher"
	"go.uber.org/zap"
)

func Test_historyBlockWriter_flush(t *testing.T) {
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
			name: "success flushes blocks/txs/outputs",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				logger := zap.NewNop()
				w := newHistoryBlockWriter(repo, logger)

				insertBlocks := []model.InsertBlock{
					{
						Block: model.Block{Height: 1},
						Txs:   []model.Transaction{{TxID: "tx1"}},
						Outputs: []model.TransactionOutput{
							{TxID: "tx1", Index: 0},
						},
					},
					{
						Block: model.Block{Height: 2},
						Txs: []model.Transaction{
							{TxID: "tx2a"},
							{TxID: "tx2b"},
						},
						Outputs: []model.TransactionOutput{
							{TxID: "tx2a", Index: 0},
							{TxID: "tx2b", Index: 0},
						},
					},
				}

				ctx := context.Background()
				repo.EXPECT().InsertTransactions(ctx, []model.Transaction{
					{TxID: "tx1"},
					{TxID: "tx2a"},
					{TxID: "tx2b"},
				}).Return(nil)
				repo.EXPECT().InsertTransactionOutputs(ctx, []model.TransactionOutput{
					{TxID: "tx1", Index: 0},
					{TxID: "tx2a", Index: 0},
					{TxID: "tx2b", Index: 0},
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
			name: "flushes in multiple batches when thresholds exceeded",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				logger := zap.NewNop()
				w := newHistoryBlockWriter(repo, logger)

				// block1 has exactly threshold txs/outputs, block2 has a tail of 1.
				block1Txs := make([]model.Transaction, transactionFlushThreshold)
				block1Outputs := make([]model.TransactionOutput, outputFlushThreshold)
				for i := 0; i < transactionFlushThreshold; i++ {
					block1Txs[i] = model.Transaction{TxID: fmt.Sprintf("tx-%d", i)}
					block1Outputs[i] = model.TransactionOutput{TxID: fmt.Sprintf("tx-%d", i), Index: 0}
				}
				block2Txs := []model.Transaction{{TxID: "tx-tail"}}
				block2Outputs := []model.TransactionOutput{{TxID: "tx-tail", Index: 0}}

				insertBlocks := []model.InsertBlock{
					{
						Block:   model.Block{Height: 1},
						Txs:     block1Txs,
						Outputs: block1Outputs,
					},
					{
						Block:   model.Block{Height: 2},
						Txs:     block2Txs,
						Outputs: block2Outputs,
					},
				}

				ctx := context.Background()
				gomock.InOrder(
					repo.EXPECT().InsertTransactions(ctx, block1Txs).Return(nil),
					repo.EXPECT().InsertTransactionOutputs(ctx, block1Outputs).Return(nil),
					repo.EXPECT().InsertTransactions(ctx, block2Txs).Return(nil),
					repo.EXPECT().InsertTransactionOutputs(ctx, block2Outputs).Return(nil),
					repo.EXPECT().InsertBlocks(ctx, []model.Block{{Height: 1}, {Height: 2}}).Return(nil),
				)

				return fields{repo: repo, logger: logger, blockBatcher: w.blockBatcher}, args{ctx: ctx, insertBlocks: insertBlocks}
			},
			wantErr: false,
		},
		{
			name: "returns error on first failed insert and stops",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				logger := zap.NewNop()
				w := newHistoryBlockWriter(repo, logger)

				block1Txs := []model.Transaction{{TxID: "tx1"}}
				block1Outputs := []model.TransactionOutput{{TxID: "tx1", Index: 0}}
				insertBlocks := []model.InsertBlock{
					{
						Block:   model.Block{Height: 1},
						Txs:     block1Txs,
						Outputs: block1Outputs,
					},
				}
				ctx := context.Background()
				repo.EXPECT().InsertTransactions(ctx, block1Txs).Return(errors.New("insert txs failed"))

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
			w := &historyBlockWriter{
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

func Test_historyBlockWriter_WriteBlock(t *testing.T) {
	type args struct {
		ctx context.Context
		b   model.InsertBlock
	}
	tests := []struct {
		name    string
		prepare func(ctrl *gomock.Controller) (*historyBlockWriter, args)
		wantErr bool
	}{
		{
			name: "returns ctx error when canceled",
			prepare: func(ctrl *gomock.Controller) (*historyBlockWriter, args) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				repo := NewMockClickhouseRepository(ctrl)
				w := newHistoryBlockWriter(repo, zap.NewNop())
				return w, args{ctx: ctx, b: model.InsertBlock{}}
			},
			wantErr: true,
		},
		{
			name: "adds block to batcher on success",
			prepare: func(ctrl *gomock.Controller) (*historyBlockWriter, args) {
				repo := NewMockClickhouseRepository(ctrl)
				w := newHistoryBlockWriter(repo, zap.NewNop())
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
