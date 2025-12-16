package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestRepository_TransactionOutputsLookupByTxIDs(t *testing.T) {
	coin := model.BTC
	network := model.Mainnet

	tests := []struct {
		name     string
		ctx      context.Context
		txids    []string
		setup    func(t *testing.T, ctx context.Context) *Repository
		want     map[string][]model.TransactionOutputLookup
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
					Observe("transaction_outputs_lookup_by_txids", coin, network, nil, gomock.AssignableToTypeOf(time.Time{}))

				return &Repository{metrics: mockMetrics}
			},
			want: map[string][]model.TransactionOutputLookup{},
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
						Observe("transaction_outputs_lookup_by_txids", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			want: map[string][]model.TransactionOutputLookup{
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
			got, err := r.TransactionOutputsLookupByTxIDs(ctx, coin, network, tt.txids)
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
