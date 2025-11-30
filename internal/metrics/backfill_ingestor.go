// Package metrics exposes application metrics collectors.
package metrics

import (
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	backfillFetchMissingTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "backfill_ingestor",
		Name:      "fetch_missing_total",
		Help:      "Count of attempts to fetch unprocessed block heights.",
	}, []string{"coin", "network", "status"})

	backfillFetchMissingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "backfill_ingestor",
		Name:      "fetch_missing_duration_seconds",
		Help:      "Duration of fetching unprocessed block heights.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"coin", "network", "status"})

	backfillProcessBatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "backfill_ingestor",
		Name:      "process_batch_total",
		Help:      "Count of processed batches.",
	}, []string{"coin", "network", "status"})

	backfillProcessBatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "backfill_ingestor",
		Name:      "process_batch_duration_seconds",
		Help:      "Duration of processing a batch of heights.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"coin", "network", "status"})

	backfillProcessBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "backfill_ingestor",
		Name:      "process_batch_size",
		Help:      "Number of heights processed per batch.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 12), // 1..2048
	}, []string{"coin", "network"})

	backfillProcessHeightDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "backfill_ingestor",
		Name:      "process_height_duration_seconds",
		Help:      "Duration of processing a single height.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"coin", "network", "status"})
)

// BackfillIngester tracks metrics for the backfill ingester pipeline.
type BackfillIngester struct {
	coin    model.Coin
	network model.Network
}

// NewBackfillIngester constructs a BackfillIngester with sane defaults.
func NewBackfillIngester(coin model.Coin, network model.Network) *BackfillIngester {
	if coin == "" {
		coin = "unknown"
	}
	if network == "" {
		network = "unknown"
	}
	return &BackfillIngester{coin: coin, network: network}
}

// ObserveFetchMissing records a fetch-missing attempt outcome and duration.
func (m BackfillIngester) ObserveFetchMissing(err error, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	backfillFetchMissingTotal.WithLabelValues(string(m.coin), string(m.network), status).Inc()
	backfillFetchMissingDuration.WithLabelValues(string(m.coin), string(m.network), status).
		Observe(time.Since(started).Seconds())
}

// ObserveProcessBatch records processing of a batch of heights.
func (m BackfillIngester) ObserveProcessBatch(err error, heights int, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	backfillProcessBatchTotal.WithLabelValues(string(m.coin), string(m.network), status).Inc()
	backfillProcessBatchDuration.WithLabelValues(string(m.coin), string(m.network), status).
		Observe(time.Since(started).Seconds())
	backfillProcessBatchSize.WithLabelValues(string(m.coin), string(m.network)).Observe(float64(heights))
}

// ObserveProcessHeight records processing of a single height.
func (m BackfillIngester) ObserveProcessHeight(err error, _ uint64, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	backfillProcessHeightDuration.WithLabelValues(string(m.coin), string(m.network), status).
		Observe(time.Since(started).Seconds())
}
