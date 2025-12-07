package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestRepository_InsertTransactions(t *testing.T) {
	ctx := context.Background()
	tx := model.Transaction{
		Coin:        model.BTC,
		Network:     model.Mainnet,
		TxID:        "txid",
		BlockHeight: 5,
		Timestamp:   time.Unix(1700000000, 0),
		Size:        100,
		VSize:       90,
		Version:     2,
		LockTime:    0,
		InputCount:  1,
		OutputCount: 2,
	}

	tests := []struct {
		name    string
		txs     []model.Transaction
		setup   func(t *testing.T) *Repository
		wantErr bool
	}{
		{
			name: "empty input still records metrics",
			txs:  nil,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockMetrics := NewMockMetrics(ctrl)
				mockMetrics.EXPECT().
					Observe("insert_transactions", model.Coin(""), model.Network(""), nil, gomock.AssignableToTypeOf(time.Time{}))

				return &Repository{conn: nil, metrics: mockMetrics}
			},
		},
		{
			name: "prepare batch error",
			txs:  []model.Transaction{tx},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				prepareErr := errors.New("prepare failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionsQuery()).
						Return(nil, prepareErr),
					mockMetrics.EXPECT().
						Observe("insert_transactions", tx.Coin, tx.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, prepareErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr: true,
		},
		{
			name: "append error",
			txs:  []model.Transaction{tx},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				appendErr := errors.New("append failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(tx.Coin),
							string(tx.Network),
							tx.TxID,
							tx.BlockHeight,
							tx.Timestamp,
							tx.Size,
							tx.VSize,
							tx.Version,
							tx.LockTime,
							tx.InputCount,
							tx.OutputCount,
						).
						Return(appendErr),
					mockMetrics.EXPECT().
						Observe("insert_transactions", tx.Coin, tx.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, appendErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr: true,
		},
		{
			name: "send error",
			txs:  []model.Transaction{tx},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				sendErr := errors.New("send failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(tx.Coin),
							string(tx.Network),
							tx.TxID,
							tx.BlockHeight,
							tx.Timestamp,
							tx.Size,
							tx.VSize,
							tx.Version,
							tx.LockTime,
							tx.InputCount,
							tx.OutputCount,
						).
						Return(nil),
					mockBatch.EXPECT().
						Send().
						Return(sendErr),
					mockMetrics.EXPECT().
						Observe("insert_transactions", tx.Coin, tx.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, sendErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr: true,
		},
		{
			name: "success",
			txs:  []model.Transaction{tx},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(tx.Coin),
							string(tx.Network),
							tx.TxID,
							tx.BlockHeight,
							tx.Timestamp,
							tx.Size,
							tx.VSize,
							tx.Version,
							tx.LockTime,
							tx.InputCount,
							tx.OutputCount,
						).
						Return(nil),
					mockBatch.EXPECT().
						Send().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("insert_transactions", tx.Coin, tx.Network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup(t)
			if err := r.InsertTransactions(ctx, tt.txs); (err != nil) != tt.wantErr {
				t.Fatalf("InsertTransactions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func insertTransactionsQuery() string {
	return `
INSERT INTO utxo_transactions (
	coin,
	network,
	txid,
	block_height,
	timestamp,
	size,
	vsize,
	version,
	locktime,
	input_count,
output_count
) VALUES`
}
