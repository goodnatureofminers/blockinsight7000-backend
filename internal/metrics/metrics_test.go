package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func delta(t *testing.T, collector prometheus.Collector, observe func()) float64 {
	t.Helper()

	before := testutil.ToFloat64(collector)
	observe()
	after := testutil.ToFloat64(collector)
	return after - before
}

func TestBackfillIngesterRecords(t *testing.T) {
	m := NewBackfillIngester("", "")
	start := time.Now().Add(-time.Second)

	if inc := delta(t, backfillFetchMissingTotal.WithLabelValues("unknown", "unknown", "success"), func() {
		m.ObserveFetchMissing(nil, start)
	}); inc != 1 {
		t.Fatalf("expected fetch missing counter increment, got %v", inc)
	}

	m.ObserveFetchMissing(nil, start)

	if errInc := delta(t, backfillProcessBatchTotal.WithLabelValues("unknown", "unknown", "error"), func() {
		m.ObserveProcessBatch(errors.New("boom"), 5, start)
	}); errInc != 1 {
		t.Fatalf("expected process batch error counter increment, got %v", errInc)
	}

	m.ObserveProcessBatch(nil, 3, start)
	m.ObserveProcessHeight(nil, 42, start)
}

func TestHistoryIngesterRecords(t *testing.T) {
	m := NewHistoryIngester("btc", "testnet")
	start := time.Now().Add(-500 * time.Millisecond)

	if inc := delta(t, historyFetchMissingTotal.WithLabelValues("btc", "testnet", "error"), func() {
		m.ObserveFetchMissing(errors.New("fail"), start)
	}); inc != 1 {
		t.Fatalf("expected history fetch missing error increment, got %v", inc)
	}

	if inc := delta(t, historyProcessBatchTotal.WithLabelValues("btc", "testnet", "success"), func() {
		m.ObserveProcessBatch(nil, 2, start)
	}); inc != 1 {
		t.Fatalf("expected history process batch success increment, got %v", inc)
	}

	m.ObserveProcessBatch(nil, 2, start)
	m.ObserveProcessHeight(nil, 7, start)
}

func TestRPCClientRecords(t *testing.T) {
	m := NewRPCClient("", "")
	start := time.Now().Add(-200 * time.Millisecond)

	if inc := delta(t, btcRPCRequestsTotal.WithLabelValues("call", "unknown", "unknown", "success"), func() {
		m.Observe("call", nil, start)
	}); inc != 1 {
		t.Fatalf("expected rpc call counter increment, got %v", inc)
	}

	m.Observe("call", errors.New("oops"), start)
}
