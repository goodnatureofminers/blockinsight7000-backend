package bitcoin

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestHistorySource_LatestHeight(t *testing.T) {
	network := model.Testnet

	tests := []struct {
		name    string
		setup   func(t *testing.T) *HistorySource
		want    uint64
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T) *HistorySource {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				rpc := NewMockRPCClient(ctrl)
				rpc.EXPECT().GetBlockCount().Return(int64(50), nil)
				return &HistorySource{rpc: rpc, network: network}
			},
			want: 50,
		},
		{
			name: "rpc error",
			setup: func(t *testing.T) *HistorySource {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				rpc := NewMockRPCClient(ctrl)
				rpc.EXPECT().GetBlockCount().Return(int64(0), context.DeadlineExceeded)
				return &HistorySource{rpc: rpc, network: network}
			},
			wantErr: true,
		},
		{
			name: "overflow",
			setup: func(t *testing.T) *HistorySource {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				rpc := NewMockRPCClient(ctrl)
				rpc.EXPECT().GetBlockCount().Return(int64(-1), nil)
				return &HistorySource{rpc: rpc, network: network}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.setup(t)
			got, err := s.LatestHeight(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("LatestHeight() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("LatestHeight() got = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestHistorySource_FetchBlock(t *testing.T) {
	network := model.Testnet

	tests := []struct {
		name    string
		setup   func(t *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock)
		wantErr bool
	}{
		{
			name: "happy path",
			setup: func(t *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockOutputs := NewMockOutputConverter(ctrl)
				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
				mockRPC.EXPECT().GetBlockHash(int64(7)).Return(blockHash, nil)
				mockRPC.EXPECT().GetBlockVerboseTx(blockHash).Return(&btcjson.GetBlockVerboseTxResult{
					Hash:       blockHash.String(),
					Height:     7,
					Time:       1_700_000_300,
					Version:    1,
					Size:       1500,
					Bits:       "1d00ffff",
					Nonce:      1,
					Difficulty: 1.0,
					MerkleRoot: "mr",
					Tx: []btcjson.TxRawResult{
						{
							Txid: "txa",
							Vin:  []btcjson.Vin{{Coinbase: "cb"}},
							Vout: []btcjson.Vout{{Value: 0.5}},
							Size: 10, Vsize: 10, Version: 1,
						},
						{
							Txid: "txb",
							Vin:  []btcjson.Vin{{Txid: "prev", Vout: 0}},
							Vout: []btcjson.Vout{{Value: 0.7}},
							Size: 11, Vsize: 11, Version: 1,
						},
					},
				}, nil)

				wantTime := time.Unix(1_700_000_300, 0).UTC()

				mockOutputs.EXPECT().
					Convert(gomock.Any(), uint64(7), wantTime).
					DoAndReturn(func(tx btcjson.TxRawResult, _ uint64, _ time.Time) ([]model.TransactionOutput, error) {
						switch tx.Txid {
						case "txa":
							return []model.TransactionOutput{{
								Coin:        model.BTC,
								Network:     network,
								TxID:        "txa",
								Index:       0,
								Value:       50_000_000,
								Addresses:   []string{"addrA"},
								BlockHeight: 7,
								BlockTime:   wantTime,
							}}, nil
						case "txb":
							return []model.TransactionOutput{{
								Coin:        model.BTC,
								Network:     network,
								TxID:        "txb",
								Index:       0,
								Value:       70_000_000,
								Addresses:   []string{"addrB"},
								BlockHeight: 7,
								BlockTime:   wantTime,
							}}, nil
						default:
							return nil, nil
						}
					}).Times(2)

				s := &HistorySource{
					rpc:             mockRPC,
					outputConverter: mockOutputs,
					network:         network,
				}

				want := &chain.HistoryBlock{
					Block: model.Block{
						Coin:       model.BTC,
						Network:    network,
						Height:     7,
						Hash:       blockHash.String(),
						Timestamp:  wantTime,
						Version:    1,
						MerkleRoot: "mr",
						Bits:       0x1d00ffff,
						Nonce:      1,
						Difficulty: 1.0,
						Size:       1500,
						TXCount:    2,
						Status:     model.BlockUnprocessed,
					},
					Txs: []model.Transaction{
						{
							Coin:        model.BTC,
							Network:     network,
							TxID:        "txa",
							BlockHeight: 7,
							Timestamp:   wantTime,
							Size:        10,
							VSize:       10,
							Version:     1,
							LockTime:    0,
							InputCount:  1,
							OutputCount: 1,
						},
						{
							Coin:        model.BTC,
							Network:     network,
							TxID:        "txb",
							BlockHeight: 7,
							Timestamp:   wantTime,
							Size:        11,
							VSize:       11,
							Version:     1,
							LockTime:    0,
							InputCount:  1,
							OutputCount: 1,
						},
					},
					Outputs: []model.TransactionOutput{
						{
							Coin:        model.BTC,
							Network:     network,
							BlockHeight: 7,
							BlockTime:   wantTime,
							TxID:        "txa",
							Index:       0,
							Value:       50_000_000,
							Addresses:   []string{"addrA"},
						},
						{
							Coin:        model.BTC,
							Network:     network,
							BlockHeight: 7,
							BlockTime:   wantTime,
							TxID:        "txb",
							Index:       0,
							Value:       70_000_000,
							Addresses:   []string{"addrB"},
						},
					},
				}

				return s, context.Background(), 7, want
			},
		},
		{
			name: "height overflow",
			setup: func(_ *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock) {
				return &HistorySource{}, context.Background(), math.MaxInt64 + 1, nil
			},
			wantErr: true,
		},
		{
			name: "context canceled",
			setup: func(_ *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return &HistorySource{}, ctx, 1, nil
			},
			wantErr: true,
		},
		{
			name: "rpc get block hash error",
			setup: func(t *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockRPC.EXPECT().GetBlockHash(int64(2)).Return(nil, context.DeadlineExceeded)
				return &HistorySource{rpc: mockRPC, network: network}, context.Background(), 2, nil
			},
			wantErr: true,
		},
		{
			name: "rpc verbose tx error",
			setup: func(t *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000004")
				mockRPC.EXPECT().GetBlockHash(int64(4)).Return(blockHash, nil)
				mockRPC.EXPECT().GetBlockVerboseTx(blockHash).Return(nil, context.Canceled)
				return &HistorySource{rpc: mockRPC, network: network}, context.Background(), 4, nil
			},
			wantErr: true,
		},
		{
			name: "output converter error",
			setup: func(t *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockOutputs := NewMockOutputConverter(ctrl)
				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000005")
				mockRPC.EXPECT().GetBlockHash(int64(5)).Return(blockHash, nil)
				mockRPC.EXPECT().GetBlockVerboseTx(blockHash).Return(&btcjson.GetBlockVerboseTxResult{
					Hash:   blockHash.String(),
					Height: 5,
					Time:   1_700_000_400,
					Bits:   "1",
					Tx: []btcjson.TxRawResult{
						{Txid: "oops", Vout: []btcjson.Vout{{Value: 0.1}}},
					},
				}, nil)
				mockOutputs.EXPECT().Convert(btcjson.TxRawResult{Txid: "oops", Vout: []btcjson.Vout{{Value: 0.1}}}, uint64(5), time.Unix(1_700_000_400, 0).UTC()).
					Return(nil, errors.New("convert fail"))
				return &HistorySource{
					rpc:             mockRPC,
					outputConverter: mockOutputs,
					network:         network,
				}, context.Background(), 5, nil
			},
			wantErr: true,
		},
		{
			name: "vin count overflow",
			setup: func(t *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockOutputs := NewMockOutputConverter(ctrl)
				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000006")
				mockRPC.EXPECT().GetBlockHash(int64(6)).Return(blockHash, nil)
				mockRPC.EXPECT().GetBlockVerboseTx(blockHash).Return(&btcjson.GetBlockVerboseTxResult{
					Hash:   blockHash.String(),
					Height: 6,
					Time:   1_700_000_500,
					Bits:   "1",
					Tx: []btcjson.TxRawResult{
						// Use a fabricated slice header to simulate an overflow without allocating huge memory.
						{Txid: "tx", Vin: makeLargeSlice[btcjson.Vin](int(math.MaxUint32) + 1)},
					},
				}, nil)
				// Output converter should not be called due to vin overflow.
				return &HistorySource{
					rpc:             mockRPC,
					outputConverter: mockOutputs,
					network:         network,
				}, context.Background(), 6, nil
			},
			wantErr: true,
		},
		{
			name: "tx size overflow",
			setup: func(t *testing.T) (*HistorySource, context.Context, uint64, *chain.HistoryBlock) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockOutputs := NewMockOutputConverter(ctrl)
				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000007")
				mockRPC.EXPECT().GetBlockHash(int64(7)).Return(blockHash, nil)
				mockRPC.EXPECT().GetBlockVerboseTx(blockHash).Return(&btcjson.GetBlockVerboseTxResult{
					Hash:   blockHash.String(),
					Height: 7,
					Time:   1_700_000_600,
					Bits:   "1",
					Tx: []btcjson.TxRawResult{
						{Txid: "tx", Vin: []btcjson.Vin{}, Vout: []btcjson.Vout{}, Size: -1, Vsize: 1, Version: 1},
					},
				}, nil)
				return &HistorySource{
					rpc:             mockRPC,
					outputConverter: mockOutputs,
					network:         network,
				}, context.Background(), 7, nil
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, ctx, height, want := tt.setup(t)
			got, err := s.FetchBlock(ctx, height)
			if (err != nil) != tt.wantErr {
				t.Fatalf("FetchBlock() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, want) {
				t.Fatalf("FetchBlock() got = %#v, want %#v", got, want)
			}
		})
	}
}

// makeLargeSlice constructs a slice header with the provided length without allocating backing memory.
// Safe here because callers only use len(...) and never access elements.
func makeLargeSlice[T any](length int) []T {
	ptr := new(T)
	//nolint:gosec // Unsafe slice used only to simulate an oversized length; elements are never accessed.
	return unsafe.Slice(ptr, length)
}
