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

func TestRepository_RandomUnprocessedBlockHeights(t *testing.T) {
	ctx := context.Background()
	coin := model.BTC
	network := model.Mainnet
	maxHeight := uint64(10)
	limit := uint64(2)

	tests := []struct {
		name     string
		limit    uint64
		setup    func(t *testing.T) *Repository
		want     []uint64
		wantErr  bool
		wantErrf string
	}{
		{
			name:  "limit zero",
			limit: 0,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockMetrics := NewMockMetrics(ctrl)
				mockMetrics.EXPECT().
					Observe("random_unprocessed_block_heights", coin, network, nil, gomock.AssignableToTypeOf(time.Time{}))

				return &Repository{conn: nil, metrics: mockMetrics}
			},
			want: nil,
		},
		{
			name:  "query error",
			limit: limit,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				queryErr := errors.New("query failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, randomUnprocessedBlockHeightsQuery(), coin, network, maxHeight, limit).
						Return(nil, queryErr),
					mockMetrics.EXPECT().
						Observe("random_unprocessed_block_heights", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, queryErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "query random unprocessed block heights",
		},
		{
			name:  "scan error",
			limit: limit,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				scanErr := errors.New("scan failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, randomUnprocessedBlockHeightsQuery(), coin, network, maxHeight, limit).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(gomock.Any()).
						Return(scanErr),
					mockRows.EXPECT().
						Close().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("random_unprocessed_block_heights", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, scanErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "scan random unprocessed block height",
		},
		{
			name:  "rows error after iteration",
			limit: limit,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				rowsErr := errors.New("rows err")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, randomUnprocessedBlockHeightsQuery(), coin, network, maxHeight, limit).
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
						Observe("random_unprocessed_block_heights", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, rowsErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "iterate random unprocessed block heights",
		},
		{
			name:  "close error",
			limit: limit,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				closeErr := errors.New("close failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, randomUnprocessedBlockHeightsQuery(), coin, network, maxHeight, limit).
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
						Observe("random_unprocessed_block_heights", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
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
			name:  "success",
			limit: limit,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, randomUnprocessedBlockHeightsQuery(), coin, network, maxHeight, limit).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(gomock.Any()).
						Do(func(dest ...any) {
							ptr := dest[0].(*uint64)
							*ptr = 4
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
						Observe("random_unprocessed_block_heights", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			want: []uint64{4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup(t)
			got, err := r.RandomUnprocessedBlockHeights(ctx, coin, network, maxHeight, tt.limit)
			if (err != nil) != tt.wantErr {
				t.Errorf("RandomUnprocessedBlockHeights() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErrf != "" && err != nil && !strings.Contains(err.Error(), tt.wantErrf) {
				t.Fatalf("error %v does not contain %q", err, tt.wantErrf)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandomUnprocessedBlockHeights() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func randomUnprocessedBlockHeightsQuery() string {
	return `
with a as (
SELECT height, argMax(status, updated_at) as status
FROM utxo_blocks
WHERE 
	coin = ?
	AND network = ?
	AND height <= ?
group by height)
select height from a where status = 'unprocessed'
ORDER BY rand()
LIMIT ?;`
}
