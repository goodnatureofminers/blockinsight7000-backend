package bitcoin

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/golang/mock/gomock"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestOutputConverter_Convert(t *testing.T) {
	network := model.Testnet
	blockTime := time.Unix(1700000001, 0).UTC()

	tests := []struct {
		name        string
		args        btcjson.TxRawResult
		setupExpect func(mockDecoder *MockScriptDecoder)
		want        []model.TransactionOutput
		wantErr     bool
	}{
		{
			name: "successfully converts outputs",
			args: btcjson.TxRawResult{
				Txid: "tx1",
				Vout: []btcjson.Vout{{
					Value: 1.5,
					ScriptPubKey: btcjson.ScriptPubKeyResult{
						Type:      "pubkeyhash",
						Hex:       "76a914000000000000000000000000000000000000000088ac",
						Asm:       "asm",
						Addresses: []string{"addr1"},
					},
				}},
			},
			setupExpect: func(mockDecoder *MockScriptDecoder) {
				mockDecoder.EXPECT().
					decodeAddresses(btcjson.Vout{
						Value: 1.5,
						ScriptPubKey: btcjson.ScriptPubKeyResult{
							Type:      "pubkeyhash",
							Hex:       "76a914000000000000000000000000000000000000000088ac",
							Asm:       "asm",
							Addresses: []string{"addr1"},
						},
					}).
					Return([]string{"addr1"}, nil)
			},
			want: []model.TransactionOutput{{
				Coin:        model.BTC,
				Network:     network,
				BlockHeight: 200,
				BlockTime:   blockTime,
				TxID:        "tx1",
				Index:       0,
				Value:       150000000,
				ScriptType:  "pubkeyhash",
				ScriptHex:   "76a914000000000000000000000000000000000000000088ac",
				ScriptAsm:   "asm",
				Addresses:   []string{"addr1"},
			}},
		},
		{
			name: "negative value returns error",
			args: btcjson.TxRawResult{
				Txid: "bad",
				Vout: []btcjson.Vout{{
					Value: -0.1,
				}},
			},
			wantErr: true,
		},
		{
			name: "btc to sat error bubbles",
			args: btcjson.TxRawResult{
				Txid: "overflow",
				Vout: []btcjson.Vout{{
					Value: math.Inf(1),
				}},
			},
			wantErr: true,
		},
		{
			name: "decode addresses error bubbles",
			args: btcjson.TxRawResult{
				Txid: "tx2",
				Vout: []btcjson.Vout{{
					Value: 0.1,
				}},
			},
			setupExpect: func(mockDecoder *MockScriptDecoder) {
				mockDecoder.EXPECT().
					decodeAddresses(btcjson.Vout{Value: 0.1}).
					Return(nil, errors.New("decode boom"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			mockDecoder := NewMockScriptDecoder(ctrl)
			if tt.setupExpect != nil {
				tt.setupExpect(mockDecoder)
			}

			converter := NewOutputConverter(mockDecoder, network)
			got, err := converter.Convert(tt.args, 200, blockTime)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Convert() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Convert() got = %#v, want %#v", got, tt.want)
			}
		})
	}
}
