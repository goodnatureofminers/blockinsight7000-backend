// Package clickhouse provides ClickHouse repository implementations.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
)

//go:generate mockgen -source=$GOFILE -destination=mocks_test.go -package=$GOPACKAGE

type (
	// Metrics records repository operation metrics.
	Metrics interface {
		Observe(operation string, coin model.Coin, network model.Network, err error, started time.Time)
	}
	// Conn abstracts a ClickHouse connection for querying and batching.
	Conn interface {
		Query(ctx context.Context, query string, args ...any) (Rows, error)
		PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (Batch, error)
	}
	// Batch wraps ClickHouse batch insert operations.
	Batch interface {
		Append(values ...any) error
		Send() error
	}
	// Rows represents an iterator over query results.
	Rows interface {
		Next() bool
		Close() error
		Err() error
		Scan(dest ...any) error
	}
)

type batchAdapter struct {
	driver.Batch
}

func (b *batchAdapter) Append(values ...any) error {
	return b.Batch.Append(values...)
}

func (b *batchAdapter) Send() error {
	return b.Batch.Send()
}

type rowsAdapter struct {
	driver.Rows
}

func (r *rowsAdapter) Next() bool {
	return r.Rows.Next()
}

func (r *rowsAdapter) Close() error {
	return r.Rows.Close()
}

func (r *rowsAdapter) Err() error {
	return r.Rows.Err()
}

func (r *rowsAdapter) Scan(dest ...any) error {
	return r.Rows.Scan(dest...)
}

type connAdapter struct {
	conn clickhouse.Conn
}

func (c connAdapter) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &rowsAdapter{rows}, nil
}

func (c connAdapter) PrepareBatch(
	ctx context.Context,
	query string,
	opts ...driver.PrepareBatchOption,
) (Batch, error) {
	batch, err := c.conn.PrepareBatch(ctx, query, opts...)
	if err != nil {
		return nil, err
	}
	return &batchAdapter{batch}, nil
}

// Repository implements ClickHouse-backed storage for UTXO data.
type Repository struct {
	conn    Conn
	metrics Metrics
}

// NewRepository initializes a ClickHouse repository with metrics.
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

	connAdapter := connAdapter{
		conn: conn,
	}

	return &Repository{conn: connAdapter, metrics: metrics}, nil
}
