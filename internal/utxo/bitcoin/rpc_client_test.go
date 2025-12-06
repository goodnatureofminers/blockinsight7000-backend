package bitcoin

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/golang/mock/gomock"
)

func Test_rpcClient_GetBlockCount(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) *rpcClient
		want    int64
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T) *rpcClient {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockMetrics := NewMockRPCMetrics(ctrl)

				mockRPC.EXPECT().GetBlockCount().Return(int64(101), nil)
				mockMetrics.EXPECT().Observe("get_block_count", nil, gomock.AssignableToTypeOf(time.Time{}))

				return &rpcClient{
					client:     mockRPC,
					rpcMetrics: mockMetrics,
				}
			},
			want: 101,
		},
		{
			name: "rpc error",
			setup: func(t *testing.T) *rpcClient {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockMetrics := NewMockRPCMetrics(ctrl)

				wantErr := errors.New("boom")
				mockRPC.EXPECT().GetBlockCount().Return(int64(0), wantErr)
				mockMetrics.EXPECT().Observe("get_block_count", wantErr, gomock.AssignableToTypeOf(time.Time{}))

				return &rpcClient{
					client:     mockRPC,
					rpcMetrics: mockMetrics,
				}
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup(t)
			gotCount, err := r.GetBlockCount()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBlockCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && gotCount != tt.want {
				t.Errorf("GetBlockCount() gotCount = %v, want %v", gotCount, tt.want)
			}
		})
	}
}

func Test_rpcClient_GetBlockHash(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) (*rpcClient, int64)
		want    *chainhash.Hash
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T) (*rpcClient, int64) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockMetrics := NewMockRPCMetrics(ctrl)

				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
				mockRPC.EXPECT().GetBlockHash(int64(7)).Return(blockHash, nil)
				mockMetrics.EXPECT().Observe("get_block_hash", nil, gomock.AssignableToTypeOf(time.Time{}))

				return &rpcClient{
					client:     mockRPC,
					rpcMetrics: mockMetrics,
				}, 7
			},
			want: func() *chainhash.Hash {
				h, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
				return h
			}(),
		},
		{
			name: "rpc error",
			setup: func(t *testing.T) (*rpcClient, int64) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockMetrics := NewMockRPCMetrics(ctrl)

				wantErr := errors.New("no hash")
				mockRPC.EXPECT().GetBlockHash(int64(8)).Return((*chainhash.Hash)(nil), wantErr)
				mockMetrics.EXPECT().Observe("get_block_hash", wantErr, gomock.AssignableToTypeOf(time.Time{}))

				return &rpcClient{
					client:     mockRPC,
					rpcMetrics: mockMetrics,
				}, 8
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, height := tt.setup(t)
			gotHash, err := r.GetBlockHash(height)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBlockHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(gotHash, tt.want) {
				t.Errorf("GetBlockHash() gotHash = %v, want %v", gotHash, tt.want)
			}
		})
	}
}

func Test_rpcClient_GetBlockVerboseTx(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) (*rpcClient, *chainhash.Hash)
		want    *btcjson.GetBlockVerboseTxResult
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T) (*rpcClient, *chainhash.Hash) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockMetrics := NewMockRPCMetrics(ctrl)

				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
				wantRes := &btcjson.GetBlockVerboseTxResult{
					Hash:   blockHash.String(),
					Height: 10,
					Tx:     []btcjson.TxRawResult{{Txid: "abc"}},
				}

				mockRPC.EXPECT().GetBlockVerboseTx(blockHash).Return(wantRes, nil)
				mockMetrics.EXPECT().Observe("get_block_verbose_tx", nil, gomock.AssignableToTypeOf(time.Time{}))

				return &rpcClient{
					client:     mockRPC,
					rpcMetrics: mockMetrics,
				}, blockHash
			},
			want: func() *btcjson.GetBlockVerboseTxResult {
				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
				return &btcjson.GetBlockVerboseTxResult{
					Hash:   blockHash.String(),
					Height: 10,
					Tx:     []btcjson.TxRawResult{{Txid: "abc"}},
				}
			}(),
		},
		{
			name: "rpc error",
			setup: func(t *testing.T) (*rpcClient, *chainhash.Hash) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockMetrics := NewMockRPCMetrics(ctrl)

				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
				wantErr := errors.New("broken")

				mockRPC.EXPECT().GetBlockVerboseTx(blockHash).Return((*btcjson.GetBlockVerboseTxResult)(nil), wantErr)
				mockMetrics.EXPECT().Observe("get_block_verbose_tx", wantErr, gomock.AssignableToTypeOf(time.Time{}))

				return &rpcClient{
					client:     mockRPC,
					rpcMetrics: mockMetrics,
				}, blockHash
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, hash := tt.setup(t)
			gotRes, err := r.GetBlockVerboseTx(hash)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBlockVerboseTx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(gotRes, tt.want) {
				t.Errorf("GetBlockVerboseTx() gotRes = %v, want %v", gotRes, tt.want)
			}
		})
	}
}
