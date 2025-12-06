package bitcoin

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func Test_scriptDecoder_decodeAddresses(t *testing.T) {
	type fields struct {
		params *chaincfg.Params
	}
	type args struct {
		vout btcjson.Vout
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{
			name:   "prefers script pub key addresses",
			fields: fields{params: &chaincfg.MainNetParams},
			args: args{vout: btcjson.Vout{
				ScriptPubKey: btcjson.ScriptPubKeyResult{
					Addresses: []string{"addr1", "addr2"},
				},
			}},
			want: []string{"addr1", "addr2"},
		},
		{
			name:   "fallback to address field",
			fields: fields{params: &chaincfg.MainNetParams},
			args: args{vout: btcjson.Vout{
				ScriptPubKey: btcjson.ScriptPubKeyResult{
					Address: "single",
				},
			}},
			want: []string{"single"},
		},
		{
			name:   "empty hex returns nil",
			fields: fields{params: &chaincfg.MainNetParams},
			args:   args{vout: btcjson.Vout{ScriptPubKey: btcjson.ScriptPubKeyResult{Hex: ""}}},
			want:   nil,
		},
		{
			name: "decode from hex script",
			fields: fields{
				params: &chaincfg.TestNet3Params,
			},
			args: func() args {
				pkh := make([]byte, 20)
				pkh[19] = 1
				addr, _ := btcutil.NewAddressPubKeyHash(pkh, &chaincfg.TestNet3Params)
				script, _ := txscript.PayToAddrScript(addr)
				return args{vout: btcjson.Vout{
					ScriptPubKey: btcjson.ScriptPubKeyResult{
						Hex: hex.EncodeToString(script),
					},
				}}
			}(),
			want: func() []string {
				pkh := make([]byte, 20)
				pkh[19] = 1
				addr, _ := btcutil.NewAddressPubKeyHash(pkh, &chaincfg.TestNet3Params)
				return []string{addr.EncodeAddress()}
			}(),
		},
		{
			name:   "invalid hex",
			fields: fields{params: &chaincfg.MainNetParams},
			args: args{vout: btcjson.Vout{
				ScriptPubKey: btcjson.ScriptPubKeyResult{Hex: "zz"},
			}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &scriptDecoder{
				params: tt.fields.params,
			}
			got, err := d.decodeAddresses(tt.args.vout)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeAddresses() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeAddresses() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_chainParamsForNetwork(t *testing.T) {
	tests := []struct {
		name    string
		network string
		want    *chaincfg.Params
		wantErr bool
	}{
		{name: "main aliases", network: "mainnet", want: &chaincfg.MainNetParams},
		{name: "bitcoin alias", network: "bitcoin", want: &chaincfg.MainNetParams},
		{name: "testnet", network: "testnet", want: &chaincfg.TestNet3Params},
		{name: "regtest", network: "regtest", want: &chaincfg.RegressionNetParams},
		{name: "signet", network: "signet", want: &chaincfg.SigNetParams},
		{name: "unsupported", network: "unknown", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := chainParamsForNetwork(model.Network(tt.network))
			if (err != nil) != tt.wantErr {
				t.Fatalf("chainParamsForNetwork() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("chainParamsForNetwork() = %v, want %v", got, tt.want)
			}
		})
	}
}
