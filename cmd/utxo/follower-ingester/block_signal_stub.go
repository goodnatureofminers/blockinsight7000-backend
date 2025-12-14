//go:build !zmq

package main

import (
	"context"

	"go.uber.org/zap"
)

func startBlockSignal(_ context.Context, _ string, _ *zap.Logger) (<-chan struct{}, error) {
	return nil, nil
}
