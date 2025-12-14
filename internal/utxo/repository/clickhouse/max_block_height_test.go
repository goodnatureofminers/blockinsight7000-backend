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

func TestRepository_MaxBlockHeight(t *testing.T) {
	t.Parallel()

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
						Query(ctx, maxBlockHeightQuery(), coin, network).
						Return(nil, queryErr),
					mockMetrics.EXPECT().
						Observe("max_block_height", coin, network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, queryErr) {
								t.Fatalf("unexpected error propagated to metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr:  true,
			wantErrf: "query max block height",
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
						Query(ctx, maxBlockHeightQuery(), coin, network).
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
						Observe("max_block_height", coin, network, nil, gomock.AssignableToTypeOf(time.Time{})),
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

			got, err := repo.MaxBlockHeight(ctx, coin, network)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MaxBlockHeight() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.wantErrf != "" && !strings.Contains(err.Error(), tt.wantErrf) {
				t.Fatalf("MaxBlockHeight() error = %v, want contains %q", err, tt.wantErrf)
			}
			if got != tt.want {
				t.Fatalf("MaxBlockHeight() got = %d, want %d", got, tt.want)
			}
		})
	}
}

func maxBlockHeightQuery() string {
	return `
SELECT coalesce(max(height), toUInt64(0)) AS max_height
FROM utxo_blocks
WHERE coin = ? AND network = ?`
}
