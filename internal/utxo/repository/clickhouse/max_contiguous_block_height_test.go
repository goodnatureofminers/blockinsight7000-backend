package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestRepository_MaxContiguousBlockHeight(t *testing.T) {
	ctx := context.Background()
	coin := model.BTC
	network := model.Mainnet

	tests := []struct {
		name     string
		setup    func(t *testing.T) *Repository
		want     uint64
		wantErr  bool
		wantErrf string
	}{
		{
			name: "query error",
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				queryErr := errors.New("query failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, maxContiguousBlockHeightQuery(), coin, network).
						Return(nil, queryErr),
					mockMetrics.EXPECT().
						Observe("max_contiguous_block_height", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, queryErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "query max contiguous block height",
		},
		{
			name: "no rows",
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, maxContiguousBlockHeightQuery(), coin, network).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(false),
					mockRows.EXPECT().
						Close().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("max_contiguous_block_height", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "not found max contiguous block height",
		},
		{
			name: "scan error",
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				scanErr := errors.New("scan failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, maxContiguousBlockHeightQuery(), coin, network).
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
						Observe("max_contiguous_block_height", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, scanErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "scan max contiguous block height",
		},
		{
			name: "close error does not fail call but reported in metrics",
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)
				closeErr := errors.New("close failed")

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, maxContiguousBlockHeightQuery(), coin, network).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(gomock.Any()).
						Do(func(dest ...any) {
							ptr, _ := dest[0].(*uint64)
							*ptr = 42
						}).
						Return(nil),
					mockRows.EXPECT().
						Close().
						Return(closeErr),
					mockMetrics.EXPECT().
						Observe("max_contiguous_block_height", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, closeErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			want: 42,
		},
		{
			name: "success",
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockRows := NewMockRows(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						Query(ctx, maxContiguousBlockHeightQuery(), coin, network).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(gomock.Any()).
						Do(func(dest ...any) {
							ptr, _ := dest[0].(*uint64)
							*ptr = 100
						}).
						Return(nil),
					mockRows.EXPECT().
						Close().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("max_contiguous_block_height", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			want: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup(t)
			got, err := r.MaxContiguousBlockHeight(ctx, coin, network)
			if (err != nil) != tt.wantErr {
				t.Errorf("MaxContiguousBlockHeight() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErrf != "" && err != nil && !strings.Contains(err.Error(), tt.wantErrf) {
				t.Fatalf("error %v does not contain %q", err, tt.wantErrf)
			}
			if got != tt.want {
				t.Errorf("MaxContiguousBlockHeight() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func maxContiguousBlockHeightQuery() string {
	return `WITH data AS (
    SELECT
        height,
        row_number() OVER (ORDER BY height) - 1 AS rn
    FROM utxo_blocks
    WHERE coin = ? and network =  ?
    group by height
)
SELECT max(height) AS max_contiguous_height
FROM data
WHERE rn = height limit 1;`
}
