package bitcoin

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

func TestBtcToSatoshis(t *testing.T) {
	tests := []struct {
		name    string
		value   float64
		want    uint64
		wantErr bool
	}{
		{
			name:  "one btc",
			value: 1.0,
			want:  100_000_000,
		},
		{
			name:  "fractional",
			value: 0.00000001,
			want:  1,
		},
		{
			name:    "negative returns error",
			value:   -0.1,
			wantErr: true,
		},
		{
			name:    "invalid infinite value returns error",
			value:   math.Inf(1),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BtcToSatoshis(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("BtcToSatoshis() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("BtcToSatoshis() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseBits(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    uint32
		wantErr bool
	}{
		{
			name:  "valid hex",
			value: "1d00ffff",
			want:  0x1d00ffff,
		},
		{
			name:    "invalid hex returns error",
			value:   "zzzz",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBits(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBits() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseBits() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildBlockFromVerbose(t *testing.T) {
	network := model.Testnet

	tests := []struct {
		name    string
		src     btcjson.GetBlockVerboseTxResult
		status  model.BlockStatus
		want    model.Block
		wantErr bool
	}{
		{
			name: "converts fields successfully",
			src: btcjson.GetBlockVerboseTxResult{
				Hash:       "hash",
				Height:     5,
				Time:       1_700_000_010,
				Version:    2,
				Size:       1234,
				Bits:       "1d00ffff",
				Nonce:      9,
				Difficulty: 1.0,
				MerkleRoot: "root",
				Tx:         []btcjson.TxRawResult{{}, {}},
			},
			status: model.BlockProcessed,
			want: model.Block{
				Coin:       model.BTC,
				Network:    network,
				Height:     5,
				Hash:       "hash",
				Timestamp:  time.Unix(1_700_000_010, 0).UTC(),
				Version:    2,
				MerkleRoot: "root",
				Bits:       0x1d00ffff,
				Nonce:      9,
				Difficulty: 1.0,
				Size:       1234,
				TXCount:    2,
				Status:     model.BlockProcessed,
			},
		},
		{
			name: "honors provided status",
			src: btcjson.GetBlockVerboseTxResult{
				Hash:       "hash2",
				Height:     6,
				Time:       1_700_000_020,
				Version:    1,
				Size:       2000,
				Bits:       "1",
				Nonce:      1,
				Difficulty: 2.0,
				MerkleRoot: "root2",
				Tx:         []btcjson.TxRawResult{{}},
			},
			status: model.BlockUnprocessed,
			want: model.Block{
				Coin:       model.BTC,
				Network:    network,
				Height:     6,
				Hash:       "hash2",
				Timestamp:  time.Unix(1_700_000_020, 0).UTC(),
				Version:    1,
				MerkleRoot: "root2",
				Bits:       1,
				Nonce:      1,
				Difficulty: 2.0,
				Size:       2000,
				TXCount:    1,
				Status:     model.BlockUnprocessed,
			},
		},
		{
			name: "invalid bits returns error",
			src: btcjson.GetBlockVerboseTxResult{
				Bits: "zzzz",
			},
			wantErr: true,
		},
		{
			name: "height overflow returns error",
			src: btcjson.GetBlockVerboseTxResult{
				Height: int64(math.MaxUint32) + 10,
				Bits:   "1",
			},
			status:  model.BlockUnprocessed,
			wantErr: true,
		},
		{
			name: "version negative error",
			src: btcjson.GetBlockVerboseTxResult{
				Version: -1,
				Bits:    "1",
			},
			status:  model.BlockProcessed,
			wantErr: true,
		},
		{
			name: "size negative error",
			src: btcjson.GetBlockVerboseTxResult{
				Size: -1,
				Bits: "1",
			},
			status:  model.BlockProcessed,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildBlockFromVerbose(tt.src, network, tt.status)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildBlockFromVerbose() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("BuildBlockFromVerbose() got = %#v, want %#v", got, tt.want)
			}
		})
	}
}
