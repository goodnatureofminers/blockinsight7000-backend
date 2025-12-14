package metrics

import (
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	followerFetchMissingTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "follower_ingestor",
		Name:      "fetch_missing_total",
		Help:      "Count of attempts to fetch new follower heights.",
	}, []string{"coin", "network", "status"})

	followerFetchMissingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "follower_ingestor",
		Name:      "fetch_missing_duration_seconds",
		Help:      "Duration of fetching follower heights.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"coin", "network", "status"})

	followerProcessBatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "follower_ingestor",
		Name:      "process_batch_total",
		Help:      "Count of follower batches processed.",
	}, []string{"coin", "network", "status"})

	followerProcessBatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "follower_ingestor",
		Name:      "process_batch_duration_seconds",
		Help:      "Duration of processing a follower batch.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"coin", "network", "status"})

	followerProcessBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "follower_ingestor",
		Name:      "process_batch_size",
		Help:      "Number of heights processed per follower batch.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"coin", "network"})
)

// FollowerIngester tracks metrics for the follower ingester pipeline.
type FollowerIngester struct {
	coin    model.Coin
	network model.Network
}

// NewFollowerIngester constructs a FollowerIngester with defaults.
func NewFollowerIngester(coin model.Coin, network model.Network) *FollowerIngester {
	if coin == "" {
		coin = "unknown"
	}
	if network == "" {
		network = "unknown"
	}
	return &FollowerIngester{coin: coin, network: network}
}

// ObserveFetchMissing records a fetch attempt outcome and duration.
func (m FollowerIngester) ObserveFetchMissing(err error, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	followerFetchMissingTotal.WithLabelValues(string(m.coin), string(m.network), status).Inc()
	followerFetchMissingDuration.WithLabelValues(string(m.coin), string(m.network), status).
		Observe(time.Since(started).Seconds())
}

// ObserveProcessBatch records processing of a placeholder batch.
func (m FollowerIngester) ObserveProcessBatch(err error, heights int, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	followerProcessBatchTotal.WithLabelValues(string(m.coin), string(m.network), status).Inc()
	followerProcessBatchDuration.WithLabelValues(string(m.coin), string(m.network), status).
		Observe(time.Since(started).Seconds())
	followerProcessBatchSize.WithLabelValues(string(m.coin), string(m.network)).
		Observe(float64(heights))
}
