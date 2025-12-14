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

func TestRepository_MaxContiguousBlockHeightByStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	coin := model.BTC
	network := model.Mainnet
	statuses := []model.BlockStatus{model.BlockNew}

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
						Query(ctx, gomock.Any(), coin, network, statuses[0]).
						Return(nil, queryErr),
					mockMetrics.EXPECT().
						Observe("max_contiguous_block_height_by_status", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, queryErr) {
								t.Fatalf("unexpected error propagated to metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "query max contiguous block height by status",
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
						Query(ctx, gomock.Any(), coin, network, statuses[0]).
						Return(mockRows, nil),
					mockRows.EXPECT().
						Next().
						Return(true),
					mockRows.EXPECT().
						Scan(gomock.Any()).
						Do(func(dest ...any) {
							p := dest[0].(*uint64)
							*p = 42
						}).
						Return(nil),
					mockRows.EXPECT().
						Err().
						Return(nil),
					mockRows.EXPECT().
						Close().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("max_contiguous_block_height_by_status", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			want:    42,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.setup(t)

			got, err := repo.MaxContiguousBlockHeightByStatuses(ctx, coin, network, statuses)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MaxContiguousBlockHeightByStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.wantErrf != "" && !strings.Contains(err.Error(), tt.wantErrf) {
				t.Fatalf("MaxContiguousBlockHeightByStatus() error = %v, want contains %q", err, tt.wantErrf)
			}
			if got != tt.want {
				t.Fatalf("MaxContiguousBlockHeightByStatus() got = %d, want %d", got, tt.want)
			}
		})
	}
}
