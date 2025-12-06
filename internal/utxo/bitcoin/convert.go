// Package bitcoin implements Bitcoin-specific chain logic.
package bitcoin

import (
	"fmt"
	"strconv"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/pkg/safe"
)

// BtcToSatoshis converts BTC amount to satoshis with overflow checks.
func BtcToSatoshis(value float64) (uint64, error) {
	amt, err := btcutil.NewAmount(value)
	if err != nil {
		return 0, err
	}
	if amt < 0 {
		return 0, fmt.Errorf("negative amount: %d", amt)
	}
	return safe.Uint64(int64(amt))
}

// ParseBits parses a bits string into a 32-bit value.
func ParseBits(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(parsed), nil
}

// BuildBlockFromVerbose maps btcjson block result into a model.Block with status.
func BuildBlockFromVerbose(src btcjson.GetBlockVerboseTxResult, network model.Network, status model.BlockStatus) (model.Block, error) {
	bits, err := ParseBits(src.Bits)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d bits parse: %w", src.Height, err)
	}
	timestamp := time.Unix(src.Time, 0).UTC()
	height, err := safe.Uint32(src.Height)
	if err != nil {
		return model.Block{}, fmt.Errorf("block height %d overflow: %w", src.Height, err)
	}
	version, err := safe.Uint32(src.Version)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d version overflow: %w", src.Height, err)
	}
	size, err := safe.Uint32(src.Size)
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d size overflow: %w", src.Height, err)
	}
	txCount, err := safe.Uint32(len(src.Tx))
	if err != nil {
		return model.Block{}, fmt.Errorf("block %d tx count overflow: %w", src.Height, err)
	}

	return model.Block{
		Coin:       model.BTC,
		Network:    network,
		Height:     uint64(height),
		Hash:       src.Hash,
		Timestamp:  timestamp,
		Version:    version,
		MerkleRoot: src.MerkleRoot,
		Bits:       bits,
		Nonce:      src.Nonce,
		Difficulty: src.Difficulty,
		Size:       size,
		TXCount:    txCount,
		Status:     status,
	}, nil
}
