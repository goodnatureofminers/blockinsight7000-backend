package bitcoin

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/chain"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestBackfillSource_LatestHeight(t *testing.T) {
	network := model.Testnet

	tests := []struct {
		name    string
		setup   func(t *testing.T) *BackfillSource
		want    uint64
		wantErr bool
	}{
		{
			name: "success returns height",
			setup: func(t *testing.T) *BackfillSource {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				rpc := NewMockRPCClient(ctrl)
				rpc.EXPECT().GetBlockCount().Return(int64(123), nil)
				return &BackfillSource{rpc: rpc, network: network}
			},
			want: 123,
		},
		{
			name: "rpc error returned",
			setup: func(t *testing.T) *BackfillSource {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				rpc := NewMockRPCClient(ctrl)
				rpc.EXPECT().GetBlockCount().Return(int64(0), fmt.Errorf("rpc fail"))
				return &BackfillSource{rpc: rpc, network: network}
			},
			wantErr: true,
		},
		{
			name: "negative count causes overflow error",
			setup: func(t *testing.T) *BackfillSource {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				rpc := NewMockRPCClient(ctrl)
				rpc.EXPECT().GetBlockCount().Return(int64(-1), nil)
				return &BackfillSource{rpc: rpc, network: network}
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

type stubOutputConverter struct {
	outputs map[string][]model.TransactionOutput
	err     error
}

func (s *stubOutputConverter) Convert(tx btcjson.TxRawResult, blockHeight uint64) ([]model.TransactionOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := s.outputs[tx.Txid]
	// stamp block metadata since backfill source expects it
	for i := range out {
		out[i].BlockHeight = blockHeight
	}
	return out, nil
}

func (s *stubOutputConverter) ConvertLookup(tx btcjson.TxRawResult) ([]model.TransactionOutputLookup, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := s.outputs[tx.Txid]
	res := make([]model.TransactionOutputLookup, len(out))
	for i := range out {
		res[i] = model.TransactionOutputLookup{
			Coin:      out[i].Coin,
			Network:   out[i].Network,
			TxID:      out[i].TxID,
			Index:     out[i].Index,
			Value:     out[i].Value,
			Addresses: out[i].Addresses,
		}
	}
	return res, nil
}

func TestBackfillSource_FetchBlock(t *testing.T) {
	network := model.Testnet

	tests := []struct {
		name    string
		setup   func(t *testing.T) (*BackfillSource, context.Context, uint64, *chain.BackfillBlock)
		wantErr bool
	}{
		{
			name: "happy path",
			setup: func(t *testing.T) (*BackfillSource, context.Context, uint64, *chain.BackfillBlock) {
				ctrl := gomock.NewController(t)
				t.Cleanup(ctrl.Finish)

				mockRPC := NewMockRPCClient(ctrl)
				mockResolver := NewMockTransactionOutputResolver(ctrl)

				blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")

				mockRPC.EXPECT().GetBlockHash(int64(10)).Return(blockHash, nil)
				mockRPC.EXPECT().GetBlockVerboseTx(blockHash).Return(&btcjson.GetBlockVerboseTxResult{
					Hash:       blockHash.String(),
					Height:     10,
					Time:       1_700_000_200,
					Version:    1,
					Size:       2000,
					Bits:       "1d00ffff",
					Nonce:      1,
					Difficulty: 1.0,
					MerkleRoot: "mr",
					Tx: []btcjson.TxRawResult{
						{
							Txid: "coinbase",
							Vin: []btcjson.Vin{{
								Coinbase: "cb",
								Sequence: 1,
							}},
							Vout:  []btcjson.Vout{{Value: 1.5}},
							Size:  100,
							Vsize: 100,
						},
						{
							Txid: "spend",
							Vin: []btcjson.Vin{{
								Txid:     "ext",
								Vout:     0,
								Sequence: 2,
							}},
							Vout:  []btcjson.Vout{{Value: 0.2}},
							Size:  120,
							Vsize: 120,
						},
					},
				}, nil)

				mockResolver.EXPECT().
					ResolveBatch(gomock.Any(), []string{"ext"}).
					Return(map[string][]model.TransactionOutputLookup{
						"ext": {{
							Value:     5000000000,
							Addresses: []string{"extAddr"},
						}},
					}, nil)

				outputs := &stubOutputConverter{
					outputs: map[string][]model.TransactionOutput{
						"coinbase": {{
							Coin:      model.BTC,
							Network:   network,
							TxID:      "coinbase",
							Index:     0,
							Value:     150000000,
							Addresses: []string{"addr1"},
						}},
						"spend": {{
							Coin:      model.BTC,
							Network:   network,
							TxID:      "spend",
							Index:     0,
							Value:     20000000,
							Addresses: []string{"addr2"},
						}},
					},
				}

				s := &BackfillSource{
					rpc:             mockRPC,
					resolver:        mockResolver,
					outputConverter: outputs,
					network:         network,
				}

				wantBlockTime := time.Unix(1_700_000_200, 0).UTC()
				want := &chain.BackfillBlock{
					Block: model.Block{
						Coin:       model.BTC,
						Network:    network,
						Height:     10,
						Hash:       blockHash.String(),
						Timestamp:  wantBlockTime,
						Version:    1,
						MerkleRoot: "mr",
						Bits:       0x1d00ffff,
						Nonce:      1,
						Difficulty: 1.0,
						Size:       2000,
						TXCount:    2,
						Status:     model.BlockProcessed,
					},
					Inputs: []model.TransactionInput{
						{
							Coin:        model.BTC,
							Network:     network,
							BlockHeight: 10,
							TxID:        "coinbase",
							Index:       0,
							PrevTxID:    "",
							PrevVout:    0,
							Sequence:    1,
							IsCoinbase:  true,
						},
						{
							Coin:        model.BTC,
							Network:     network,
							BlockHeight: 10,
							TxID:        "spend",
							Index:       0,
							PrevTxID:    "ext",
							PrevVout:    0,
							Sequence:    2,
							IsCoinbase:  false,
							Addresses:   []string{"extAddr"},
							Value:       5000000000,
						},
					},
				}

				return s, context.Background(), 10, want
			},
		},
		{
			name: "height overflow",
			setup: func(_ *testing.T) (*BackfillSource, context.Context, uint64, *chain.BackfillBlock) {
				return &BackfillSource{}, context.Background(), math.MaxInt64 + 1, nil
			},
			wantErr: true,
		},
		{
			name: "context canceled",
			setup: func(_ *testing.T) (*BackfillSource, context.Context, uint64, *chain.BackfillBlock) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return &BackfillSource{}, ctx, 1, nil
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

func TestBackfillSource_convertInputs(t *testing.T) {
	network := model.Testnet

	tests := []struct {
		name    string
		args    func(t *testing.T) (context.Context, *BackfillSource, btcjson.TxRawResult, map[string][]model.TransactionOutputLookup)
		want    []model.TransactionInput
		wantErr bool
	}{
		{
			name: "coinbase input",
			args: func(_ *testing.T) (context.Context, *BackfillSource, btcjson.TxRawResult, map[string][]model.TransactionOutputLookup) {
				tx := btcjson.TxRawResult{
					Txid: "coinbase-tx",
					Vin: []btcjson.Vin{{
						Coinbase: "abcd",
						Sequence: 123,
					}},
				}
				src := &BackfillSource{network: network}
				return context.Background(), src, tx, nil
			},
			want: []model.TransactionInput{{
				Coin:        model.BTC,
				Network:     network,
				BlockHeight: 100,
				TxID:        "coinbase-tx",
				Index:       0,
				PrevTxID:    "",
				PrevVout:    0,
				Sequence:    123,
				IsCoinbase:  true,
				Witness:     nil,
			}},
		},
		{
			name: "resolves previous output",
			args: func(_ *testing.T) (context.Context, *BackfillSource, btcjson.TxRawResult, map[string][]model.TransactionOutputLookup) {
				tx := btcjson.TxRawResult{
					Txid: "current",
					Vin: []btcjson.Vin{{
						Txid:     "prev-tx",
						Vout:     0,
						Sequence: 456,
						Witness:  []string{"w1"},
					}},
				}
				src := &BackfillSource{network: network}
				resolved := map[string][]model.TransactionOutputLookup{
					"prev-tx": {{
						Value:     42,
						Addresses: []string{"addr1"},
					}},
				}
				return context.Background(), src, tx, resolved
			},
			want: []model.TransactionInput{{
				Coin:         model.BTC,
				Network:      network,
				BlockHeight:  100,
				TxID:         "current",
				Index:        0,
				PrevTxID:     "prev-tx",
				PrevVout:     0,
				Sequence:     456,
				IsCoinbase:   false,
				Witness:      []string{"w1"},
				Addresses:    []string{"addr1"},
				Value:        42,
				ScriptSigHex: "",
				ScriptSigAsm: "",
			}},
		},
		{
			name: "missing previous output index returns error",
			args: func(_ *testing.T) (context.Context, *BackfillSource, btcjson.TxRawResult, map[string][]model.TransactionOutputLookup) {
				tx := btcjson.TxRawResult{
					Txid: "bad",
					Vin: []btcjson.Vin{{
						Txid: "prev",
						Vout: 1,
					}},
				}
				src := &BackfillSource{network: network}
				return context.Background(), src, tx, map[string][]model.TransactionOutputLookup{
					"prev": {},
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, s, tx, resolved := tt.args(t)

			got, err := s.convertInputs(ctx, tx, 100, resolved)
			if (err != nil) != tt.wantErr {
				t.Fatalf("convertInputs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("convertInputs() got = %#v, want %#v", got, tt.want)
			}
		})
	}
}
