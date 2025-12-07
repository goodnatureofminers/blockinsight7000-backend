package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestRepository_FirstCoin(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name  string
		coins model.Coin
		in    any
	}{
		{
			name:  "block",
			coins: model.BTC,
			in: []model.Block{
				{Coin: model.BTC},
			},
		},
		{
			name:  "transaction",
			coins: model.BTC,
			in: []model.Transaction{
				{Coin: model.BTC},
			},
		},
		{
			name:  "transaction input",
			coins: model.BTC,
			in: []model.TransactionInput{
				{Coin: model.BTC},
			},
		},
		{
			name:  "transaction output",
			coins: model.BTC,
			in: []model.TransactionOutput{
				{Coin: model.BTC},
			},
		},
		{
			name:  "empty",
			coins: "",
			in:    []model.Block{},
		},
		{
			name:  "unknown type",
			coins: "",
			in:    []time.Time{now},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch v := tt.in.(type) {
			case []model.Block:
				if got := firstCoin(v); got != tt.coins {
					t.Fatalf("firstCoin() = %v, want %v", got, tt.coins)
				}
			case []model.Transaction:
				if got := firstCoin(v); got != tt.coins {
					t.Fatalf("firstCoin() = %v, want %v", got, tt.coins)
				}
			case []model.TransactionInput:
				if got := firstCoin(v); got != tt.coins {
					t.Fatalf("firstCoin() = %v, want %v", got, tt.coins)
				}
			case []model.TransactionOutput:
				if got := firstCoin(v); got != tt.coins {
					t.Fatalf("firstCoin() = %v, want %v", got, tt.coins)
				}
			case []time.Time:
				if got := firstCoin(v); got != tt.coins {
					t.Fatalf("firstCoin() = %v, want %v", got, tt.coins)
				}
			}
		})
	}
}

func TestRepository_FirstNetwork(t *testing.T) {
	tests := []struct {
		name    string
		network model.Network
		in      any
	}{
		{
			name:    "block",
			network: model.Mainnet,
			in: []model.Block{
				{Network: model.Mainnet},
			},
		},
		{
			name:    "transaction",
			network: model.Mainnet,
			in: []model.Transaction{
				{Network: model.Mainnet},
			},
		},
		{
			name:    "transaction input",
			network: model.Mainnet,
			in: []model.TransactionInput{
				{Network: model.Mainnet},
			},
		},
		{
			name:    "transaction output",
			network: model.Mainnet,
			in: []model.TransactionOutput{
				{Network: model.Mainnet},
			},
		},
		{
			name:    "empty",
			network: "",
			in:      []model.Block{},
		},
		{
			name:    "unknown type",
			network: "",
			in:      []int{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch v := tt.in.(type) {
			case []model.Block:
				if got := firstNetwork(v); got != tt.network {
					t.Fatalf("firstNetwork() = %v, want %v", got, tt.network)
				}
			case []model.Transaction:
				if got := firstNetwork(v); got != tt.network {
					t.Fatalf("firstNetwork() = %v, want %v", got, tt.network)
				}
			case []model.TransactionInput:
				if got := firstNetwork(v); got != tt.network {
					t.Fatalf("firstNetwork() = %v, want %v", got, tt.network)
				}
			case []model.TransactionOutput:
				if got := firstNetwork(v); got != tt.network {
					t.Fatalf("firstNetwork() = %v, want %v", got, tt.network)
				}
			case []int:
				if got := firstNetwork(v); got != tt.network {
					t.Fatalf("firstNetwork() = %v, want %v", got, tt.network)
				}
			}
		})
	}
}

func TestRepository_InsertBlocks(t *testing.T) {
	ctx := context.Background()
	block := model.Block{
		Coin:       model.BTC,
		Network:    model.Mainnet,
		Height:     42,
		Hash:       "hash",
		Timestamp:  time.Unix(1700000000, 0),
		Version:    2,
		MerkleRoot: "root",
		Bits:       1,
		Nonce:      2,
		Difficulty: 3.14,
		Size:       123,
		TXCount:    7,
		Status:     model.BlockProcessed,
	}

	tests := []struct {
		name    string
		blocks  []model.Block
		setup   func(t *testing.T) *Repository
		wantErr bool
	}{
		{
			name:   "empty input still records metrics",
			blocks: nil,
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockMetrics := NewMockMetrics(ctrl)
				mockMetrics.EXPECT().
					Observe("insert_blocks", model.Coin(""), model.Network(""), nil, gomock.AssignableToTypeOf(time.Time{}))

				return &Repository{conn: nil, metrics: mockMetrics}
			},
		},
		{
			name:   "prepare batch error",
			blocks: []model.Block{block},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				prepareErr := errors.New("prepare failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertBlocksQuery()).
						Return(nil, prepareErr),
					mockMetrics.EXPECT().
						Observe("insert_blocks", block.Coin, block.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, prepareErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr: true,
		},
		{
			name:   "append error",
			blocks: []model.Block{block},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				appendErr := errors.New("append failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertBlocksQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(block.Coin),
							string(block.Network),
							block.Height,
							block.Hash,
							block.Timestamp,
							block.Version,
							block.MerkleRoot,
							block.Bits,
							block.Nonce,
							block.Difficulty,
							block.Size,
							block.TXCount,
							string(block.Status),
						).
						Return(appendErr),
					mockMetrics.EXPECT().
						Observe("insert_blocks", block.Coin, block.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, appendErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr: true,
		},
		{
			name:   "send error",
			blocks: []model.Block{block},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				sendErr := errors.New("send failed")

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertBlocksQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(block.Coin),
							string(block.Network),
							block.Height,
							block.Hash,
							block.Timestamp,
							block.Version,
							block.MerkleRoot,
							block.Bits,
							block.Nonce,
							block.Difficulty,
							block.Size,
							block.TXCount,
							string(block.Status),
						).
						Return(nil),
					mockBatch.EXPECT().
						Send().
						Return(sendErr),
					mockMetrics.EXPECT().
						Observe("insert_blocks", block.Coin, block.Network, gomock.Any(), gomock.AssignableToTypeOf(time.Time{})).
						Do(func(_ string, _ model.Coin, _ model.Network, err error, _ time.Time) {
							if !errors.Is(err, sendErr) {
								t.Fatalf("unexpected error in metrics: %v", err)
							}
						}),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
			wantErr: true,
		},
		{
			name:   "success",
			blocks: []model.Block{block},
			setup: func(t *testing.T) *Repository {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockConn := NewMockConn(ctrl)
				mockBatch := NewMockBatch(ctrl)
				mockMetrics := NewMockMetrics(ctrl)

				gomock.InOrder(
					mockConn.EXPECT().
						PrepareBatch(ctx, insertBlocksQuery()).
						Return(mockBatch, nil),
					mockBatch.EXPECT().
						Append(
							string(block.Coin),
							string(block.Network),
							block.Height,
							block.Hash,
							block.Timestamp,
							block.Version,
							block.MerkleRoot,
							block.Bits,
							block.Nonce,
							block.Difficulty,
							block.Size,
							block.TXCount,
							string(block.Status),
						).
						Return(nil),
					mockBatch.EXPECT().
						Send().
						Return(nil),
					mockMetrics.EXPECT().
						Observe("insert_blocks", block.Coin, block.Network, nil, gomock.AssignableToTypeOf(time.Time{})),
				)

				return &Repository{conn: mockConn, metrics: mockMetrics}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.setup(t)
			if err := repo.InsertBlocks(ctx, tt.blocks); (err != nil) != tt.wantErr {
				t.Fatalf("InsertBlocks() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func insertBlocksQuery() string {
	return `
INSERT INTO utxo_blocks (
	coin,
	network,
	height,
	hash,
	timestamp,
	version,
	merkleroot,
	bits,
	nonce,
	difficulty,
	size,
	tx_count,
    status
) VALUES`
}
