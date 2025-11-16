package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jessevdk/go-flags"
)

type config struct {
	ClickhouseDSN string `long:"clickhouse-dsn" env:"MIGRATIONS_CLICKHOUSE_DSN" default:"clickhouse://localhost:9000/default" description:"ClickHouse DSN (clickhouse://user:pass@host:port/db)"`
	MigrationsDir string `long:"migrations-dir" env:"MIGRATIONS_DIR" default:"migrations/clickhouse" description:"Path to ClickHouse migration files"`
}

func main() {
	cfg := config{}
	if _, err := flags.Parse(&cfg); err != nil {
		var ferr *flags.Error
		if errors.As(err, &ferr) && ferr.Type == flags.ErrHelp {
			return
		}
		log.Fatalf("failed to parse flags: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runMigrations(ctx, cfg); err != nil {
		log.Fatalf("migration run failed: %v", err)
	}
}

func runMigrations(ctx context.Context, cfg config) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dir, err := filepath.Abs(cfg.MigrationsDir)
	if err != nil {
		return fmt.Errorf("resolve migrations dir: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("stat migrations dir %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}

	sourceURL := fmt.Sprintf("file://%s", filepath.ToSlash(dir))
	m, err := migrate.New(sourceURL, cfg.ClickhouseDSN)
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer func() {
		srcErr, dbErr := m.Close()
		if srcErr != nil {
			log.Printf("migration source close error: %v", srcErr)
		}
		if dbErr != nil {
			log.Printf("migration database close error: %v", dbErr)
		}
	}()

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			log.Println("no migrations to apply")
			return nil
		}
		return err
	}

	log.Println("migrations applied successfully")
	return nil
}
