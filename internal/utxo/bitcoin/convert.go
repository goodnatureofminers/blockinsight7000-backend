// Package bitcoin implements Bitcoin-specific chain logic.
package bitcoin

import (
	"fmt"
	"strconv"

	"github.com/btcsuite/btcd/btcutil"
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
