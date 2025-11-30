// Package safe provides helpers for safe numeric conversions with overflow checks.
package safe

import (
	"fmt"
	"math"
)

// Uint32 converts signed or unsigned integers to uint32 with range validation.
func Uint32[T ~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64](v T) (uint32, error) {
	switch value := any(v).(type) {
	case int:
		if value < 0 || int64(value) > math.MaxUint32 {
			return 0, fmt.Errorf("value %d out of uint32 range", v)
		}
	case int32:
		if value < 0 || int64(value) > math.MaxUint32 {
			return 0, fmt.Errorf("value %d out of uint32 range", v)
		}
	case int64:
		if value < 0 || value > math.MaxUint32 {
			return 0, fmt.Errorf("value %d out of uint32 range", v)
		}
	case uint:
		if uint64(value) > math.MaxUint32 {
			return 0, fmt.Errorf("value %d out of uint32 range", v)
		}
	case uint32:
		if uint64(value) > math.MaxUint32 {
			return 0, fmt.Errorf("value %d out of uint32 range", v)
		}
	case uint64:
		if value > math.MaxUint32 {
			return 0, fmt.Errorf("value %d out of uint32 range", v)
		}
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
	return uint32(v), nil
}

// Uint64 converts signed or unsigned integers to uint64 while guarding against negatives.
func Uint64[T ~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64](v T) (uint64, error) {
	switch value := any(v).(type) {
	case int:
		if value < 0 {
			return 0, fmt.Errorf("value %d out of uint64 range", v)
		}
		return uint64(value), nil
	case int32:
		if value < 0 {
			return 0, fmt.Errorf("value %d out of uint64 range", v)
		}
		return uint64(value), nil
	case int64:
		if value < 0 {
			return 0, fmt.Errorf("value %d out of uint64 range", v)
		}
		return uint64(value), nil
	case uint:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case uint64:
		return value, nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}
