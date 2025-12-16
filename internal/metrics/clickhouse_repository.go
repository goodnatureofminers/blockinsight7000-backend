package metrics

import (
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	clickhouseRepositoryRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "clickhouse_repository",
		Name:      "operations_total",
		Help:      "Count of repository operations.",
	}, []string{"operation", "coin", "network", "status"})
	clickhouseRepositoryRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "clickhouse_repository",
		Name:      "operation_duration_seconds",
		Help:      "Duration of repository operations.",
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 15, 20, 30},
	}, []string{"operation", "coin", "network", "status"})
)

// ClickhouseRepository tracks metrics for ClickHouse repository operations.
type ClickhouseRepository struct{}

// NewClickhouseRepository creates a ClickhouseRepository metrics collector.
func NewClickhouseRepository() *ClickhouseRepository {
	return &ClickhouseRepository{}
}

// Observe records duration and status of a repository operation.
func (m ClickhouseRepository) Observe(operation string, coin model.Coin, network model.Network, err error, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}
	if coin == "" {
		coin = "unknown"
	}
	if network == "" {
		network = "unknown"
	}

	clickhouseRepositoryRequestsTotal.WithLabelValues(operation, string(coin), string(network), status).Inc()
	clickhouseRepositoryRequestDuration.WithLabelValues(operation, string(coin), string(network), status).Observe(time.Since(started).Seconds())
}
