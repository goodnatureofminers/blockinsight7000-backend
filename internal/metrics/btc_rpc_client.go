package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	btcRPCRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "btc_rpc_client",
		Name:      "operations_total",
		Help:      "Count of BTC node RPC operations.",
	}, []string{"operation", "network", "status"})
	btcRPCRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "btc_rpc_client",
		Name:      "operation_duration_seconds",
		Help:      "Duration of BTC node RPC operations.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "network", "status"})
)

func ObserveBTCRPC(operation, network string, err error, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	if network == "" {
		network = "unknown"
	}

	btcRPCRequestsTotal.WithLabelValues(operation, network, status).Inc()
	btcRPCRequestDuration.WithLabelValues(operation, network, status).Observe(time.Since(started).Seconds())
}
