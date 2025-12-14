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

func TestRepository_RandomBlockHeightsByStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	coin := model.BTC
	network := model.Mainnet
	status := model.BlockNew
	maxHeight := uint64(100)
	limit := uint64(3)

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
					Observe("random_block_heights_by_status", coin, network, nil, gomock.AssignableToTypeOf(time.Time{}))

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
						Query(ctx, gomock.Any(), coin, network, status, maxHeight, limit).
						Return(nil, queryErr),
					mockMetrics.EXPECT().
						Observe("random_block_heights_by_status", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, queryErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "query random block heights by status",
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
						Query(ctx, gomock.Any(), coin, network, status, maxHeight, limit).
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
						Observe("random_block_heights_by_status", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, scanErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "scan random block height",
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
				rowsErr := errors.New("rows error")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, gomock.Any(), coin, network, status, maxHeight, limit).
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
						Observe("random_block_heights_by_status", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, rowsErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "iterate random block heights by status",
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
						Query(ctx, gomock.Any(), coin, network, status, maxHeight, limit).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(gomock.Any()).
						Do(func(dest ...any) {
							p := dest[0].(*uint64)
							*p = 2
						}).
						Return(nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(gomock.Any()).
						Do(func(dest ...any) {
							p := dest[0].(*uint64)
							*p = 7
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
						Observe("random_block_heights_by_status", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			want: []uint64{2, 7},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.setup(t)
			got, err := repo.RandomBlockHeightsByStatus(ctx, coin, network, status, maxHeight, tt.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("RandomBlockHeightsByStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.wantErrf != "" && !strings.Contains(err.Error(), tt.wantErrf) {
				t.Fatalf("RandomBlockHeightsByStatus() error = %v, want contains %q", err, tt.wantErrf)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("RandomBlockHeightsByStatus() got = %v, want %v", got, tt.want)
			}
		})
	}
}
