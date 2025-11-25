package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	btcRepoRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "btc_repository",
		Name:      "operations_total",
		Help:      "Count of BTC repository operations.",
	}, []string{"operation", "network", "status"})
	btcRepoRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "btc_repository",
		Name:      "operation_duration_seconds",
		Help:      "Duration of BTC repository operations.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "network", "status"})
)

func ObserveBTCRepository(operation, network string, err error, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}

	if network == "" {
		network = "unknown"
	}

	btcRepoRequestsTotal.WithLabelValues(operation, network, status).Inc()
	btcRepoRequestDuration.WithLabelValues(operation, network, status).Observe(time.Since(started).Seconds())
}
