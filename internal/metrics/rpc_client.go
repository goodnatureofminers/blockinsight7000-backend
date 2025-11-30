package metrics

import (
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	btcRPCRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockinsight7000",
		Subsystem: "rpc_client",
		Name:      "operations_total",
		Help:      "Count of node RPC operations.",
	}, []string{"operation", "coin", "network", "status"})
	btcRPCRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockinsight7000",
		Subsystem: "rpc_client",
		Name:      "operation_duration_seconds",
		Help:      "Duration of node RPC operations.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "coin", "network", "status"})
)

// RPCClient tracks metrics for RPC calls to blockchain nodes.
type RPCClient struct {
	coin    model.Coin
	network model.Network
}

// NewRPCClient constructs a metrics collector for RPC calls.
func NewRPCClient(coin model.Coin, network model.Network) *RPCClient {
	if coin == "" {
		coin = "unknown"
	}
	if network == "" {
		network = "unknown"
	}
	return &RPCClient{coin: coin, network: network}
}

// Observe records a single RPC call outcome and duration.
func (m RPCClient) Observe(operation string, err error, started time.Time) {
	status := "success"
	if err != nil {
		status = "error"
	}

	btcRPCRequestsTotal.WithLabelValues(operation, string(m.coin), string(m.network), status).Inc()
	btcRPCRequestDuration.WithLabelValues(operation, string(m.coin), string(m.network), status).Observe(time.Since(started).Seconds())
}
