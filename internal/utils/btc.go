package utils

import (
	"fmt"
	"strconv"

	"github.com/btcsuite/btcd/btcutil"
)

func BtcToSatoshis(value float64) (uint64, error) {
	amt, err := btcutil.NewAmount(value)
	if err != nil {
		return 0, err
	}
	if amt < 0 {
		return 0, fmt.Errorf("negative amount: %d", amt)
	}
	return uint64(amt), nil
}

func ParseBits(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(parsed), nil
}
