package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/jessevdk/go-flags"
	"go.uber.org/zap"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/repository"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/service"
)

type config struct {
	ClickhouseDSN string        `long:"clickhouse-dsn" env:"BTC_HISTORY_CLICKHOUSE_DSN" description:"ClickHouse DSN"`
	Network       string        `long:"network" env:"BTC_HISTORY_NETWORK" description:"network name" required:"true"`
	RPCURL        string        `long:"rpc-url" env:"BTC_HISTORY_RPC_URL" description:"Bitcoin RPC URL" default:"http://127.0.0.1:8332"`
	RPCUser       string        `long:"rpc-user" env:"BTC_HISTORY_RPC_USER" description:"Bitcoin RPC username"`
	RPCPassword   string        `long:"rpc-password" env:"BTC_HISTORY_RPC_PASSWORD" description:"Bitcoin RPC password"`
	HTTPTimeout   time.Duration `long:"http-timeout" env:"BTC_HISTORY_HTTP_TIMEOUT" description:"HTTP timeout for RPC requests" default:"30s"`
}

func main() {
	cfg := config{}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic("can't initialize zap logger: " + err.Error())
	}
	defer func() {
		_ = logger.Sync()
	}()

	if _, err := flags.ParseArgs(&cfg, os.Args); err != nil {
		var ferr *flags.Error
		if errors.As(err, &ferr) && ferr.Type == flags.ErrHelp {
			return
		}
		logger.Fatal("failed to parse flags", zap.Error(err))
	}

	if cfg.ClickhouseDSN == "" {
		logger.Fatal("ClickHouse DSN is required")
	}

	if err := run(ctx, cfg, logger); err != nil {
		logger.Fatal("btc history ingester failed", zap.Error(err))
	}
}

func run(ctx context.Context, cfg config, logger *zap.Logger) error {
	repo, err := repository.NewBTCRepository(cfg.ClickhouseDSN)
	if err != nil {
		return fmt.Errorf("init repository: %w", err)
	}
	rpc, err := newRPCClient(cfg.RPCURL, cfg.RPCUser, cfg.RPCPassword, cfg.HTTPTimeout)
	if err != nil {
		return fmt.Errorf("init btc rpc client: %w", err)
	}
	defer func() {
		rpc.Shutdown()
		rpc.WaitForShutdown()
	}()
	svc, err := service.NewBTCHistorySyncService(
		repo,
		rpc,
		cfg.Network,
		logger,
	)
	if err != nil {
		return err
	}
	return svc.Run(ctx)
}

func newRPCClient(rawURL, user, password string, timeout time.Duration) (*rpcclient.Client, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse rpc url: %w", err)
	}
	if parsed.Scheme != "http" {
		return nil, fmt.Errorf("rpc url scheme %q not supported, use http", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("rpc url missing host")
	}

	cfg := &rpcclient.ConnConfig{
		Host:         parsed.Host,
		User:         user,
		Pass:         password,
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	//if timeout > 0 {
	//	HTTPTimeout = timeout
	//}

	return rpcclient.New(cfg, nil)
}
