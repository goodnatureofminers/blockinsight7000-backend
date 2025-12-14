package ingester

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func Test_followerHeightFetcher_Fetch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(ctrl *gomock.Controller) (*followerHeightFetcher, context.Context)
		want    []uint64
		wantErr bool
	}{
		{
			name: "returns bounded heights",
			prepare: func(ctrl *gomock.Controller) (*followerHeightFetcher, context.Context) {
				source := NewMockHistorySource(ctrl)
				repo := NewMockClickhouseRepository(ctrl)
				ctx := context.Background()

				source.EXPECT().LatestHeight(ctx).Return(uint64(105), nil)
				repo.EXPECT().
					RandomMissingBlockHeights(ctx, model.BTC, model.Mainnet, uint64(105), uint64(3)).
					Return([]uint64{103, 101, 102}, nil)

				return &followerHeightFetcher{
					source:     source,
					repository: repo,
					coin:       model.BTC,
					network:    model.Mainnet,
					limit:      3,
				}, ctx
			},
			want: []uint64{101, 102, 103},
		},
		{
			name: "includes genesis height when starting empty",
			prepare: func(ctrl *gomock.Controller) (*followerHeightFetcher, context.Context) {
				source := NewMockHistorySource(ctrl)
				repo := NewMockClickhouseRepository(ctrl)
				ctx := context.Background()

				source.EXPECT().LatestHeight(ctx).Return(uint64(2), nil)
				repo.EXPECT().
					RandomMissingBlockHeights(ctx, model.BTC, model.Mainnet, uint64(2), uint64(2)).
					Return([]uint64{1, 0}, nil)

				return &followerHeightFetcher{
					source:     source,
					repository: repo,
					coin:       model.BTC,
					network:    model.Mainnet,
					limit:      2,
				}, ctx
			},
			want: []uint64{0, 1},
		},
		{
			name: "no new heights when caught up",
			prepare: func(ctrl *gomock.Controller) (*followerHeightFetcher, context.Context) {
				source := NewMockHistorySource(ctrl)
				repo := NewMockClickhouseRepository(ctrl)
				ctx := context.Background()

				source.EXPECT().LatestHeight(ctx).Return(uint64(50), nil)
				repo.EXPECT().
					RandomMissingBlockHeights(ctx, model.BTC, model.Mainnet, uint64(50), uint64(10)).
					Return(nil, nil)

				return &followerHeightFetcher{
					source:     source,
					repository: repo,
					coin:       model.BTC,
					network:    model.Mainnet,
					limit:      10,
				}, ctx
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			fetcher, ctx := tt.prepare(ctrl)
			got, err := fetcher.Fetch(ctx)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Fetch() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Fetch() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_followerHeightFetcher_Fetch_errors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	source := NewMockHistorySource(ctrl)
	repo := NewMockClickhouseRepository(ctrl)
	ctx := context.Background()

	fetcher := &followerHeightFetcher{
		source:     source,
		repository: repo,
		coin:       model.BTC,
		network:    model.Mainnet,
		limit:      5,
	}

	source.EXPECT().LatestHeight(ctx).Return(uint64(0), errors.New("latest failed"))
	if _, err := fetcher.Fetch(ctx); err == nil {
		t.Fatalf("expected error from LatestHeight")
	}

	source.EXPECT().LatestHeight(ctx).Return(uint64(5), nil)
	repo.EXPECT().
		RandomMissingBlockHeights(ctx, model.BTC, model.Mainnet, uint64(5), uint64(5)).
		Return(nil, errors.New("missing failed"))
	if _, err := fetcher.Fetch(ctx); err == nil {
		t.Fatalf("expected error from RandomMissingBlockHeights")
	}
}
