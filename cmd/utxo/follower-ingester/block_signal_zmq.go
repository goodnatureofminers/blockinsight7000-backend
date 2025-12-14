//go:build zmq

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pebbe/zmq4"
	"go.uber.org/zap"
)

func startBlockSignal(ctx context.Context, addr string, logger *zap.Logger) (<-chan struct{}, error) {
	if addr == "" {
		return nil, nil
	}

	sub, err := newSubscriber(addr, "hashblock")
	if err != nil {
		return nil, fmt.Errorf("connect zmq: %w", err)
	}

	notify := make(chan struct{}, 1)

	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msgParts, err := sub.RecvMessageBytes(0)
			if err != nil {
				logger.Warn("zmq recv failed", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			if len(msgParts) < 2 {
				logger.Warn("skip malformed zmq message", zap.Int("parts", len(msgParts)))
				continue
			}

			select {
			case notify <- struct{}{}:
			default:
			}
		}
	}()

	return notify, nil
}

func newSubscriber(addr string, topics ...string) (*zmq4.Socket, error) {
	sub, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		return nil, err
	}

	for _, topic := range topics {
		if err := sub.SetSubscribe(topic); err != nil {
			sub.Close()
			return nil, err
		}
	}

	if err := sub.Connect(addr); err != nil {
		sub.Close()
		return nil, err
	}
	return sub, nil
}
