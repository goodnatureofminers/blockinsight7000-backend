package metrics

import (
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	historyFetchMissingTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "history_ingestor",
		Name:      "fetch_missing_total",
		Help:      "Count of attempts to fetch missing block heights.",
	}, []string{"coin", "network", "status"})

	historyFetchMissingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "history_ingestor",
		Name:      "fetch_missing_duration_seconds",
		Help:      "Duration of fetching missing block heights.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"coin", "network", "status"})

	historyProcessBatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "history_ingestor",
		Name:      "process_batch_total",
		Help:      "Count of processed batches.",
	}, []string{"coin", "network", "status"})

	historyProcessBatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "history_ingestor",
		Name:      "process_batch_duration_seconds",
		Help:      "Duration of processing a batch of heights.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"coin", "network", "status"})

	historyProcessBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "history_ingestor",
		Name:      "process_batch_size",
		Help:      "Number of heights processed per batch.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 12), // 1..2048
	}, []string{"coin", "network"})

	historyProcessHeightDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "history_ingestor",
		Name:      "process_height_duration_seconds",
		Help:      "Duration of processing a single height.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"coin", "network", "status"})
)

type HistoryIngester struct {
	coin    model.Coin
	network model.Network
}

func NewHistoryIngester(coin model.Coin, network model.Network) *HistoryIngester {
	if coin == "" {
		coin = "unknown"
	}
	if network == "" {
		network = "unknown"
	}
	return &HistoryIngester{coin: coin, network: network}
}

func (m HistoryIngester) ObserveFetchMissing(err error, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	historyFetchMissingTotal.WithLabelValues(string(m.coin), string(m.network), status).Inc()
	historyFetchMissingDuration.WithLabelValues(string(m.coin), string(m.network), status).
		Observe(time.Since(started).Seconds())
}

func (m HistoryIngester) ObserveProcessBatch(err error, heights int, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	historyProcessBatchTotal.WithLabelValues(string(m.coin), string(m.network), status).Inc()
	historyProcessBatchDuration.WithLabelValues(string(m.coin), string(m.network), status).
		Observe(time.Since(started).Seconds())
	historyProcessBatchSize.WithLabelValues(string(m.coin), string(m.network)).Observe(float64(heights))
}

func (m HistoryIngester) ObserveProcessHeight(err error, height uint64, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	historyProcessHeightDuration.WithLabelValues(string(m.coin), string(m.network), status).
		Observe(time.Since(started).Seconds())
}
