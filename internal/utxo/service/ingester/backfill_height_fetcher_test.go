package ingester

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func Test_backfillHeightFetcher_Fetch(t *testing.T) {
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
			name: "returns unprocessed heights",
			prepare: func(ctrl *gomock.Controller) (fields, args) {
				repo := NewMockClickhouseRepository(ctrl)
				ctx := context.Background()
				repo.EXPECT().MaxContiguousBlockHeightByStatuses(ctx, model.Coin("btc"), model.Network("mainnet"), []model.BlockStatus{model.BlockUnprocessed, model.BlockProcessed}).Return(uint64(50), nil)
				repo.EXPECT().RandomUnprocessedBlockHeights(ctx, model.Coin("btc"), model.Network("mainnet"), uint64(50), uint64(5)).
					Return([]uint64{2, 4}, nil)
				return fields{
					repository: repo,
					coin:       "btc",
					network:    "mainnet",
					limit:      5,
				}, args{ctx: ctx}
			},
			want:    []uint64{2, 4},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			fields, args := tt.prepare(ctrl)

			f := &backfillHeightFetcher{
				repository: fields.repository,
				coin:       fields.coin,
				network:    fields.network,
				limit:      fields.limit,
			}
			got, err := f.Fetch(args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Fetch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("Fetch() len = %d, want %d", len(got), len(tt.want))
			}
		})
	}
}

func Test_backfillHeightFetcher_Fetch_errors(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	repo := NewMockClickhouseRepository(ctrl)

	f := &backfillHeightFetcher{
		repository: repo,
		coin:       model.Coin("btc"),
		network:    model.Network("mainnet"),
		limit:      5,
	}

	repo.EXPECT().MaxContiguousBlockHeightByStatuses(ctx, model.Coin("btc"), model.Network("mainnet"), []model.BlockStatus{model.BlockUnprocessed, model.BlockProcessed}).Return(uint64(0), errors.New("max failed"))
	if _, err := f.Fetch(ctx); err == nil {
		t.Fatalf("expected error from MaxContiguousBlockHeight")
	}

	repo.EXPECT().MaxContiguousBlockHeightByStatuses(ctx, model.Coin("btc"), model.Network("mainnet"), []model.BlockStatus{model.BlockUnprocessed, model.BlockProcessed}).Return(uint64(50), nil)
	repo.EXPECT().RandomUnprocessedBlockHeights(ctx, model.Coin("btc"), model.Network("mainnet"), uint64(50), uint64(5)).
		Return(nil, errors.New("unprocessed failed"))
	if _, err := f.Fetch(ctx); err == nil {
		t.Fatalf("expected error from RandomUnprocessedBlockHeights")
	}
}
