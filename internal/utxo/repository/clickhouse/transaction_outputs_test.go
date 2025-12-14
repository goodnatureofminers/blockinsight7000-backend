package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestRepository_TransactionOutputs(t *testing.T) {
	coin := model.BTC
	network := model.Mainnet
	txid := "txid"

	tests := []struct {
		name     string
		ctx      context.Context
		setup    func(t *testing.T, ctx context.Context) *Repository
		want     []model.TransactionOutput
		wantErr  bool
		wantErrf string
	}{
		{
			name: "query error",
			setup: func(t *testing.T, ctx context.Context) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				queryErr := errors.New("query failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, transactionOutputsQuery(), coin, network, txid).
						Return(nil, queryErr),
					mockMetrics.EXPECT().
						Observe("transaction_outputs", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, queryErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "query transaction outputs",
		},
		{
			name: "context canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			setup: func(t *testing.T, ctx context.Context) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, transactionOutputsQuery(), coin, network, txid).
						Return(nil, context.Canceled),
					mockMetrics.EXPECT().
						Observe("transaction_outputs", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, context.Canceled) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "context canceled",
		},
		{
			name: "scan error",
			setup: func(t *testing.T, ctx context.Context) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				scanErr := errors.New("scan failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, transactionOutputsQuery(), coin, network, txid).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
						).
						Return(scanErr),
					mockRows.EXPECT().
						Close().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("transaction_outputs", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, scanErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "scan transaction output",
		},
		{
			name: "rows error after iteration",
			setup: func(t *testing.T, ctx context.Context) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				rowsErr := errors.New("rows error")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, transactionOutputsQuery(), coin, network, txid).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(false),
					mockRows.EXPECT().
						Err().
						Return(rowsErr),
					mockRows.EXPECT().
						Close().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("transaction_outputs", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, rowsErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "iterate transaction outputs",
		},
		{
			name: "close error",
			setup: func(t *testing.T, ctx context.Context) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				closeErr := errors.New("close failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, transactionOutputsQuery(), coin, network, txid).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(false),
					mockRows.EXPECT().
						Err().
						Return(nil),
					mockRows.EXPECT().
						Close().
						Return(closeErr),
					mockMetrics.EXPECT().
						Observe("transaction_outputs", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, closeErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  false,
			wantErrf: "",
		},
		{
			name: "success",
			setup: func(t *testing.T, ctx context.Context) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, transactionOutputsQuery(), coin, network, txid).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
						).
						Do(func(dest ...any) {
							*dest[0].(*uint64) = 1
							*dest[1].(*time.Time) = time.Unix(0, 0)
							*dest[2].(*uint32) = 0
							*dest[3].(*uint64) = 10
							*dest[4].(*string) = "type"
							*dest[5].(*string) = "hex"
							*dest[6].(*string) = "asm"
							*dest[7].(*[]string) = []string{"a"}
						}).
						Return(nil),
					mockRows.EXPECT().
						Next().
						Return(false),
					mockRows.EXPECT().
						Err().
						Return(nil),
					mockRows.EXPECT().
						Close().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("transaction_outputs", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			want: []model.TransactionOutput{
				{
					Coin:        coin,
					Network:     network,
					TxID:        txid,
					BlockHeight: 1,
					BlockTime:   time.Unix(0, 0),
					Index:       0,
					Value:       10,
					ScriptType:  "type",
					ScriptHex:   "hex",
					ScriptAsm:   "asm",
					Addresses:   []string{"a"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			if ctx == nil {
				ctx = context.Background()
			}

			r := tt.setup(t, ctx)
			got, err := r.TransactionOutputs(ctx, coin, network, txid)
			if (err != nil) != tt.wantErr {
				t.Errorf("TransactionOutputs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErrf != "" && err != nil && !strings.Contains(err.Error(), tt.wantErrf) {
				t.Fatalf("error %v does not contain %q", err, tt.wantErrf)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TransactionOutputs() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRepository_TransactionOutputsByTxIDs(t *testing.T) {
	coin := model.BTC
	network := model.Mainnet

	tests := []struct {
		name     string
		ctx      context.Context
		txids    []string
		setup    func(t *testing.T, ctx context.Context) *Repository
		want     map[string][]model.TransactionOutput
		wantErr  bool
		wantErrf string
	}{
		{
			name:  "empty input returns empty map",
			txids: nil,
			setup: func(t *testing.T, _ context.Context) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockMetrics := NewMockMetrics(ctrl)
				mockMetrics.EXPECT().
					Observe("transaction_outputs_by_txids", coin, network, nil, gomock.AssignableToTypeOf(time.Time{}))

				return &Repository{metrics: mockMetrics}
			},
			want: map[string][]model.TransactionOutput{},
		},
		{
			name:  "success",
			txids: []string{"tx1", "tx2"},
			setup: func(t *testing.T, ctx context.Context) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				txids := []string{"tx1", "tx2"}

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, gomock.Any(), coin, network, txids).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
						).
						Do(func(dest ...any) {
							*dest[0].(*string) = "tx1"
							*dest[1].(*uint32) = 0
							*dest[2].(*uint64) = 10
							*dest[3].(*[]string) = []string{"a"}
						}).
						Return(nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
							gomock.Any(),
						).
						Do(func(dest ...any) {
							*dest[0].(*string) = "tx2"
							*dest[1].(*uint32) = 1
							*dest[2].(*uint64) = 20
							*dest[3].(*[]string) = []string{"b"}
						}).
						Return(nil),
					mockRows.EXPECT().
						Next().
						Return(false),
					mockRows.EXPECT().
						Err().
						Return(nil),
					mockRows.EXPECT().
						Close().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("transaction_outputs_by_txids", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			want: map[string][]model.TransactionOutput{
				"tx1": {{
					Coin:      coin,
					Network:   network,
					TxID:      "tx1",
					Index:     0,
					Value:     10,
					Addresses: []string{"a"},
				}},
				"tx2": {{
					Coin:      coin,
					Network:   network,
					TxID:      "tx2",
					Index:     1,
					Value:     20,
					Addresses: []string{"b"},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.ctx
			if ctx == nil {
				ctx = context.Background()
			}

			r := tt.setup(t, ctx)
			got, err := r.TransactionOutputsByTxIDs(ctx, coin, network, tt.txids)
			if (err != nil) != tt.wantErr {
				t.Errorf("TransactionOutputsByTxIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErrf != "" && err != nil && !strings.Contains(err.Error(), tt.wantErrf) {
				t.Fatalf("error %v does not contain %q", err, tt.wantErrf)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TransactionOutputsByTxIDs() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func transactionOutputsQuery() string {
	return `
SELECT
	block_height,
	block_timestamp,
	output_index,
	value,
	script_type,
	script_hex,
	script_asm,
	addresses
FROM utxo_transaction_outputs
WHERE coin = ? AND network = ? AND txid = CAST(? AS FixedString(64))
ORDER BY output_index ASC`
}
