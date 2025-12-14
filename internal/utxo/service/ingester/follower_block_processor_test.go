package ingester

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"go.uber.org/zap"
)

func Test_followerBlockProcessor_Process(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(ctrl *gomock.Controller) (*followerBlockProcessor, context.Context)
		wantErr bool
	}{
		{
			name: "successfully inserts placeholders",
			prepare: func(ctrl *gomock.Controller) (*followerBlockProcessor, context.Context) {
				repo := NewMockClickhouseRepository(ctrl)
				metrics := NewMockFollowerIngesterMetrics(ctrl)
				ctx := context.Background()

				metrics.EXPECT().ObserveProcessBatch(nil, 2, gomock.Any())
				repo.EXPECT().
					InsertBlocks(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, blocks []model.Block) error {
						if len(blocks) != 2 {
							t.Fatalf("expected 2 blocks, got %d", len(blocks))
						}
						placeholder := strings.Repeat("0", 64)
						expectedHeights := []uint64{101, 102}
						for i, b := range blocks {
							if b.Height != expectedHeights[i] {
								t.Fatalf("unexpected height %d", b.Height)
							}
							if b.Status != model.BlockNew {
								t.Fatalf("expected status new, got %s", b.Status)
							}
							if b.Hash != placeholder || b.MerkleRoot != placeholder {
								t.Fatalf("expected placeholder hashes, got %q and %q", b.Hash, b.MerkleRoot)
							}
							if !b.Timestamp.Equal(time.Unix(0, 0).UTC()) {
								t.Fatalf("unexpected timestamp %v", b.Timestamp)
							}
						}
						return nil
					})

				return &followerBlockProcessor{
					repo:    repo,
					coin:    model.BTC,
					network: model.Mainnet,
					metrics: metrics,
					logger:  zap.NewNop(),
				}, ctx
			},
		},
		{
			name: "propagates insert error",
			prepare: func(ctrl *gomock.Controller) (*followerBlockProcessor, context.Context) {
				repo := NewMockClickhouseRepository(ctrl)
				metrics := NewMockFollowerIngesterMetrics(ctrl)
				ctx := context.Background()

				repo.EXPECT().
					InsertBlocks(gomock.Any(), gomock.Any()).
					Return(errors.New("insert failed"))
				metrics.EXPECT().
					ObserveProcessBatch(gomock.Any(), 2, gomock.Any())

				return &followerBlockProcessor{
					repo:    repo,
					coin:    model.BTC,
					network: model.Mainnet,
					metrics: metrics,
					logger:  zap.NewNop(),
				}, ctx
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			processor, ctx := tt.prepare(ctrl)
			err := processor.Process(ctx, []uint64{101, 102})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Process() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
