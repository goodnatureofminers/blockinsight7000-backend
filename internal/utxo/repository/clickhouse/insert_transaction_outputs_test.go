package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestRepository_InsertTransactionOutputs(t *testing.T) {
	ctx := context.Background()
	output := model.TransactionOutput{
		Coin:        model.BTC,
		Network:     model.Mainnet,
		BlockHeight: 10,
		BlockTime:   time.Unix(1700000000, 0),
		TxID:        "txid",
		Index:       1,
		Value:       50,
		ScriptType:  "p2pkh",
		ScriptHex:   "hex",
		ScriptAsm:   "asm",
		Addresses:   []string{"addr1"},
	}

	tests := []struct {
		name    string
		outputs []model.TransactionOutput
		setup   func(t *testing.T) *Repository
		wantErr bool
	}{
		{
			name:    "empty input still records metrics",
			outputs: nil,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockMetrics := NewMockMetrics(ctrl)
				mockMetrics.EXPECT().
					Observe("insert_transaction_outputs", model.Coin(""), model.Network(""), nil, gomock.AssignableToTypeOf(time.Time{}))

				return &Repository{conn: nil, metrics: mockMetrics}
			},
		},
		{
			name:    "prepare batch error",
			outputs: []model.TransactionOutput{output},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				prepareErr := errors.New("prepare failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionOutputsQuery()).
						Return(nil, prepareErr),
					mockMetrics.EXPECT().
						Observe("insert_transaction_outputs", output.Coin, output.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
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
			name:    "append error",
			outputs: []model.TransactionOutput{output},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				appendErr := errors.New("append failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionOutputsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(output.Coin),
							string(output.Network),
							output.BlockHeight,
							output.BlockTime,
							output.TxID,
							output.Index,
							output.Value,
							output.ScriptType,
							output.ScriptHex,
							output.ScriptAsm,
							output.Addresses,
						).
						Return(appendErr),
					mockMetrics.EXPECT().
						Observe("insert_transaction_outputs", output.Coin, output.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
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
			name:    "send error",
			outputs: []model.TransactionOutput{output},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				sendErr := errors.New("send failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionOutputsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(output.Coin),
							string(output.Network),
							output.BlockHeight,
							output.BlockTime,
							output.TxID,
							output.Index,
							output.Value,
							output.ScriptType,
							output.ScriptHex,
							output.ScriptAsm,
							output.Addresses,
						).
						Return(nil),
					mockBatch.EXPECT().
						Send().
						Return(sendErr),
					mockMetrics.EXPECT().
						Observe("insert_transaction_outputs", output.Coin, output.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
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
			name:    "success",
			outputs: []model.TransactionOutput{output},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionOutputsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(output.Coin),
							string(output.Network),
							output.BlockHeight,
							output.BlockTime,
							output.TxID,
							output.Index,
							output.Value,
							output.ScriptType,
							output.ScriptHex,
							output.ScriptAsm,
							output.Addresses,
						).
						Return(nil),
					mockBatch.EXPECT().
						Send().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("insert_transaction_outputs", output.Coin, output.Network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup(t)
			if err := r.InsertTransactionOutputs(ctx, tt.outputs); (err != nil) != tt.wantErr {
				t.Fatalf("InsertTransactionOutputs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func insertTransactionOutputsQuery() string {
	return `
INSERT INTO utxo_transaction_outputs (
    coin,
	network,
	block_height,
	block_timestamp,
	txid,
	output_index,
	value,
	script_type,
	script_hex,
	script_asm,
addresses
) VALUES`
}
