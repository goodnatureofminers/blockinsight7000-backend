package clickhouse

import (
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

type (
	Metrics interface {
		Observe(operation string, coin model.Coin, network model.Network, err error, started time.Time)
	}
)

type Repository struct {
	conn    clickhouse.Conn
	metrics Metrics
}

func NewRepository(dsn string, metrics Metrics) (*Repository, error) {
	if dsn == "" {
		return nil, errors.New("clickhouse dsn is required")
	}

	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}

	return &Repository{conn: conn, metrics: metrics}, nil
}
