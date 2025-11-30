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
		source     HistorySource
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
				source := NewMockHistorySource(ctrl)
				repo := NewMockClickhouseRepository(ctrl)
				ctx := context.Background()
				source.EXPECT().LatestHeight(ctx).Return(uint64(100), nil)
				repo.EXPECT().RandomMissingBlockHeights(ctx, model.Coin("btc"), model.Network("mainnet"), uint64(100), uint64(5)).
					Return([]uint64{1, 2, 3}, nil)
				return fields{
					source:     source,
					repository: repo,
					coin:       "btc",
					network:    "mainnet",
					limit:      5,
				}, args{ctx: ctx}
			},
			want:    []uint64{1, 2, 3},
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
				source:     fields.source,
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
		})
	}
}

func Test_historyHeightFetcher_Fetch_errors(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	source := NewMockHistorySource(ctrl)
	repo := NewMockClickhouseRepository(ctrl)

	f := &historyHeightFetcher{
		source:     source,
		repository: repo,
		coin:       model.Coin("btc"),
		network:    model.Network("mainnet"),
		limit:      5,
	}

	source.EXPECT().LatestHeight(ctx).Return(uint64(0), errors.New("latest failed"))
	if _, err := f.Fetch(ctx); err == nil {
		t.Fatalf("expected error from LatestHeight")
	}

	source.EXPECT().LatestHeight(ctx).Return(uint64(50), nil)
	repo.EXPECT().RandomMissingBlockHeights(ctx, model.Coin("btc"), model.Network("mainnet"), uint64(50), uint64(5)).
		Return(nil, errors.New("missing failed"))
	if _, err := f.Fetch(ctx); err == nil {
		t.Fatalf("expected error from RandomMissingBlockHeights")
	}
}
