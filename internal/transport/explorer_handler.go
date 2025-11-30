// Package transport exposes gRPC/HTTP handlers.
package transport

import (
	"context"

	blockinsight7000v1 "github.com/goodnatureofminers/blockinsight7000-proto/pkg/blockinsight7000/v1"
)

// ExplorerHandler implements ExplorerServiceServer.
type ExplorerHandler struct {
	blockinsight7000v1.UnimplementedExplorerServiceServer
}

// NewExplorerHandler returns an ExplorerHandler instance.
func NewExplorerHandler() blockinsight7000v1.ExplorerServiceServer {
	return &ExplorerHandler{}
}

// Health reports server health.
func (h *ExplorerHandler) Health(_ context.Context, _ *blockinsight7000v1.HealthRequest) (*blockinsight7000v1.HealthResponse, error) {
	return &blockinsight7000v1.HealthResponse{
		Status:      blockinsight7000v1.HealthStatus_HEALTH_STATUS_HEALTHY,
		Description: "",
	}, nil
}
