package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/metrics"
	rpcclient2 "github.com/goodnatureofminers/blockinsight7000-backend/internal/pkg/btcd/rpcclient"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/model"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/repository/clickhouse"
	"github.com/goodnatureofminers/blockinsight7000-backend/internal/utxo/service"
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type config struct {
	ClickhouseDSN string        `long:"clickhouse-dsn" env:"BTC_HISTORY_CLICKHOUSE_DSN" description:"ClickHouse DSN"`
	Coin          model.Coin    `long:"coin" env:"BTC_HISTORY_COIN" description:"coin name" required:"true"`
	Network       model.Network `long:"network" env:"BTC_HISTORY_NETWORK" description:"network name" required:"true"`
	RPCURL        string        `long:"rpc-url" env:"BTC_HISTORY_RPC_URL" description:"Bitcoin RPC URL" default:"http://127.0.0.1:8332"`
	RPCUser       string        `long:"rpc-user" env:"BTC_HISTORY_RPC_USER" description:"Bitcoin RPC username"`
	RPCPassword   string        `long:"rpc-password" env:"BTC_HISTORY_RPC_PASSWORD" description:"Bitcoin RPC password"`
	HTTPTimeout   time.Duration `long:"http-timeout" env:"BTC_HISTORY_HTTP_TIMEOUT" description:"HTTP timeout for RPC requests" default:"30s"`
	MetricsAddr   string        `long:"metrics-addr" env:"BTC_HISTORY_METRICS_ADDR" description:"address for metrics server" default:":2112"`
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
		logger.Fatal("utxo history ingester failed", zap.Error(err))
	}
}

func run(ctx context.Context, cfg config, logger *zap.Logger) error {
	startMetricsServer(ctx, cfg.MetricsAddr, logger)

	repo, err := clickhouse.NewRepository(cfg.ClickhouseDSN, metrics.NewClickhouseRepository())
	if err != nil {
		return fmt.Errorf("init repository: %w", err)
	}
	rpcClient, err := newRPCClient(cfg.RPCURL, cfg.RPCUser, cfg.RPCPassword, cfg.HTTPTimeout)
	if err != nil {
		return fmt.Errorf("init utxo rpc client: %w", err)
	}
	defer func() {
		rpcClient.Shutdown()
		rpcClient.WaitForShutdown()
	}()
	rpc := rpcclient2.NewObservedClient(rpcClient, metrics.NewRpcClient(cfg.Coin, cfg.Network))
	svc, err := service.NewBackfillIngesterService(
		repo,
		rpc,
		cfg.Coin,
		cfg.Network,
		logger,
	)
	if err != nil {
		return err
	}
	return svc.Run(ctx)
}

func startMetricsServer(ctx context.Context, addr string, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("starting metrics server", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", zap.Error(err))
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("failed to shutdown metrics server", zap.Error(err))
		}
	}()
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
