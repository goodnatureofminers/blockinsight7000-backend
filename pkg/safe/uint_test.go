package safe

import (
	"math"
	"testing"
)

type uint32Args[T interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}] struct {
	v T
}

type uint32TestCase[T interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}] struct {
	name    string
	args    uint32Args[T]
	want    uint32
	wantErr bool
}

func runUint32Case[T interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}](t *testing.T, tc uint32TestCase[T]) {
	t.Helper()

	t.Run(tc.name, func(t *testing.T) {
		got, err := Uint32(tc.args.v)
		if (err != nil) != tc.wantErr {
			t.Errorf("Uint32() error = %v, wantErr %v", err, tc.wantErr)
			return
		}
		if got != tc.want {
			t.Errorf("Uint32() got = %v, want %v", got, tc.want)
		}
	})
}

func TestUint32(t *testing.T) {
	runUint32Case(t, uint32TestCase[int]{name: "int within range", args: uint32Args[int]{v: 42}, want: 42})
	runUint32Case(t, uint32TestCase[int]{name: "int negative", args: uint32Args[int]{v: -1}, wantErr: true})
	runUint32Case(t, uint32TestCase[int64]{name: "int64 overflow", args: uint32Args[int64]{v: int64(math.MaxUint32) + 1}, wantErr: true})
	runUint32Case(t, uint32TestCase[int64]{name: "int64 boundary ok", args: uint32Args[int64]{v: int64(math.MaxUint32)}, want: math.MaxUint32})
	runUint32Case(t, uint32TestCase[uint64]{name: "uint64 overflow", args: uint32Args[uint64]{v: math.MaxUint32 + 1}, wantErr: true})
	runUint32Case(t, uint32TestCase[uint32]{name: "uint32 max", args: uint32Args[uint32]{v: math.MaxUint32}, want: math.MaxUint32})
	runUint32Case(t, uint32TestCase[uint]{name: "uint small", args: uint32Args[uint]{v: 7}, want: 7})
	runUint32Case(t, uint32TestCase[int32]{name: "int32 negative", args: uint32Args[int32]{v: -5}, wantErr: true})
	runUint32Case(t, uint32TestCase[int32]{name: "int32 positive", args: uint32Args[int32]{v: 123}, want: 123})
	runUint32Case(t, uint32TestCase[uint64]{name: "uint64 boundary ok", args: uint32Args[uint64]{v: math.MaxUint32}, want: math.MaxUint32})
	runUint32Case(t, uint32TestCase[int64]{name: "zero", args: uint32Args[int64]{v: 0}, want: 0})
}

type uint64Args[T interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}] struct {
	v T
}

type uint64TestCase[T interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}] struct {
	name    string
	args    uint64Args[T]
	want    uint64
	wantErr bool
}

func runUint64Case[T interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}](t *testing.T, tc uint64TestCase[T]) {
	t.Helper()

	t.Run(tc.name, func(t *testing.T) {
		got, err := Uint64(tc.args.v)
		if (err != nil) != tc.wantErr {
			t.Errorf("Uint64() error = %v, wantErr %v", err, tc.wantErr)
			return
		}
		if got != tc.want {
			t.Errorf("Uint64() got = %v, want %v", got, tc.want)
		}
	})
}

func TestUint64(t *testing.T) {
	runUint64Case(t, uint64TestCase[int]{name: "int positive", args: uint64Args[int]{v: 99}, want: 99})
	runUint64Case(t, uint64TestCase[int]{name: "int negative", args: uint64Args[int]{v: -1}, wantErr: true})
	runUint64Case(t, uint64TestCase[int64]{name: "int64 negative", args: uint64Args[int64]{v: -100}, wantErr: true})
	runUint64Case(t, uint64TestCase[int64]{name: "int64 large positive", args: uint64Args[int64]{v: math.MaxInt64}, want: math.MaxInt64})
	runUint64Case(t, uint64TestCase[uint]{name: "uint small", args: uint64Args[uint]{v: 5}, want: 5})
	runUint64Case(t, uint64TestCase[uint32]{name: "uint32 value", args: uint64Args[uint32]{v: math.MaxUint32}, want: math.MaxUint32})
	runUint64Case(t, uint64TestCase[uint64]{name: "uint64 value", args: uint64Args[uint64]{v: math.MaxUint64}, want: math.MaxUint64})
	runUint64Case(t, uint64TestCase[int32]{name: "int32 zero", args: uint64Args[int32]{v: 0}, want: 0})
}
