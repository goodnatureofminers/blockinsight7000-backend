package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestRepository_InsertTransactionInputs(t *testing.T) {
	ctx := context.Background()
	input := model.TransactionInput{
		Coin:         model.BTC,
		Network:      model.Mainnet,
		BlockHeight:  1,
		BlockTime:    time.Unix(1700000000, 0),
		TxID:         "txid",
		Index:        2,
		PrevTxID:     "prev",
		PrevVout:     3,
		Sequence:     4,
		IsCoinbase:   false,
		Value:        100,
		ScriptSigHex: "hex",
		ScriptSigAsm: "asm",
		Witness:      []string{"w1", "w2"},
		Addresses:    []string{"addr1"},
	}

	tests := []struct {
		name    string
		inputs  []model.TransactionInput
		setup   func(t *testing.T) *Repository
		wantErr bool
	}{
		{
			name:   "empty input still records metrics",
			inputs: nil,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockMetrics := NewMockMetrics(ctrl)
				mockMetrics.EXPECT().
					Observe("insert_transaction_inputs", model.Coin(""), model.Network(""), nil, gomock.AssignableToTypeOf(time.Time{}))

				return &Repository{conn: nil, metrics: mockMetrics}
			},
		},
		{
			name:   "prepare batch error",
			inputs: []model.TransactionInput{input},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				prepareErr := errors.New("prepare failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionInputsQuery()).
						Return(nil, prepareErr),
					mockMetrics.EXPECT().
						Observe("insert_transaction_inputs", input.Coin, input.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
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
			name:   "append error",
			inputs: []model.TransactionInput{input},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				appendErr := errors.New("append failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionInputsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(input.Coin),
							string(input.Network),
							input.BlockHeight,
							input.BlockTime,
							input.TxID,
							input.Index,
							input.PrevTxID,
							input.PrevVout,
							input.Sequence,
							input.IsCoinbase,
							input.Value,
							input.ScriptSigHex,
							input.ScriptSigAsm,
							input.Witness,
							input.Addresses,
						).
						Return(appendErr),
					mockMetrics.EXPECT().
						Observe("insert_transaction_inputs", input.Coin, input.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
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
			name:   "send error",
			inputs: []model.TransactionInput{input},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				sendErr := errors.New("send failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionInputsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(input.Coin),
							string(input.Network),
							input.BlockHeight,
							input.BlockTime,
							input.TxID,
							input.Index,
							input.PrevTxID,
							input.PrevVout,
							input.Sequence,
							input.IsCoinbase,
							input.Value,
							input.ScriptSigHex,
							input.ScriptSigAsm,
							input.Witness,
							input.Addresses,
						).
						Return(nil),
					mockBatch.EXPECT().
						Send().
						Return(sendErr),
					mockMetrics.EXPECT().
						Observe("insert_transaction_inputs", input.Coin, input.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
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
			name:   "success",
			inputs: []model.TransactionInput{input},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertTransactionInputsQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(input.Coin),
							string(input.Network),
							input.BlockHeight,
							input.BlockTime,
							input.TxID,
							input.Index,
							input.PrevTxID,
							input.PrevVout,
							input.Sequence,
							input.IsCoinbase,
							input.Value,
							input.ScriptSigHex,
							input.ScriptSigAsm,
							input.Witness,
							input.Addresses,
						).
						Return(nil),
					mockBatch.EXPECT().
						Send().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("insert_transaction_inputs", input.Coin, input.Network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup(t)
			if err := r.InsertTransactionInputs(ctx, tt.inputs); (err != nil) != tt.wantErr {
				t.Fatalf("InsertTransactionInputs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func insertTransactionInputsQuery() string {
	return `
INSERT INTO utxo_transaction_inputs (
	coin,
	network,
	block_height,
	block_timestamp,
	txid,
	input_index,
	prev_txid,
	prev_vout,
	sequence,
	is_coinbase,
	value,
	script_sig_hex,
	script_sig_asm,
	witness,
addresses
) VALUES`
}
