package ingester

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func Test_historyHeightFetcher_Fetch(t *testing.T) {
	type fields struct {
		repository ClickhouseRepository
		coin       model.Coin
		network    model.Network
		limit      uint64
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		prepare func(ctrl *gomock.Controller) (fields, args)
		want    []uint64
		wantErr bool
	}{
		{
			name: "returns missing heights",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				ctx := context.Background()
				repo.EXPECT().
					MaxBlockHeight(ctx, model.Coin("btc"), model.Network("mainnet")).
					Return(uint64(0), nil)
				repo.EXPECT().
					RandomBlockHeightsByStatus(ctx, model.Coin("btc"), model.Network("mainnet"), model.BlockNew, uint64(0), uint64(5)).
					Return([]uint64{3, 2, 1}, nil)
				return fields{
					repository: repo,
					coin:       "btc",
					network:    "mainnet",
					limit:      5,
				}, args{ctx: ctx}
			},
			want:    []uint64{1, 2, 3},
			wantErr: false,
		},
		{
			name: "returns first non-empty chunk",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				ctx := context.Background()

				repo.EXPECT().
					MaxBlockHeight(ctx, model.Coin("btc"), model.Network("mainnet")).
					Return(uint64(historyChunkSize*3/2), nil)
				repo.EXPECT().
					RandomBlockHeightsByStatus(ctx, model.Coin("btc"), model.Network("mainnet"), model.BlockNew, uint64(historyChunkSize-1), uint64(5)).
					Return([]uint64{}, nil)
				repo.EXPECT().
					RandomBlockHeightsByStatus(ctx, model.Coin("btc"), model.Network("mainnet"), model.BlockNew, uint64(historyChunkSize*3/2), uint64(5)).
					Return([]uint64{60_000, 70_000}, nil)

				return fields{
					repository: repo,
					coin:       "btc",
					network:    "mainnet",
					limit:      5,
				}, args{ctx: ctx}
			},
			want:    []uint64{60_000, 70_000},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			fields, args := tt.prepare(ctrl)

			f := &historyHeightFetcher{
				repository: fields.repository,
				coin:       fields.coin,
				network:    fields.network,
				limit:      fields.limit,
			}
			got, err := f.Fetch(args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("FetchMissing() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("FetchMissing() len = %d, want %d", len(got), len(tt.want))
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("FetchMissing() got[%d]=%d, want %d", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func Test_historyHeightFetcher_Fetch_errors(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	repo := NewMockClickhouseRepository(ctrl)

	f := &historyHeightFetcher{
		repository: repo,
		coin:       model.Coin("btc"),
		network:    model.Network("mainnet"),
		limit:      5,
	}

	repo.EXPECT().
		MaxBlockHeight(ctx, model.Coin("btc"), model.Network("mainnet")).
		Return(uint64(0), nil)
	repo.EXPECT().
		RandomBlockHeightsByStatus(ctx, model.Coin("btc"), model.Network("mainnet"), model.BlockNew, uint64(0), uint64(5)).
		Return(nil, errors.New("missing failed"))
	if _, err := f.Fetch(ctx); err == nil {
		t.Fatalf("expected error from RandomBlockHeightsByStatus")
	}
}
