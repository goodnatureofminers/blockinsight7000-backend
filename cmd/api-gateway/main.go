package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/goodnatureofminers/blockinsight7000-backend/internal/transport"
	"github.com/goodnatureofminers/blockinsight7000-proto/pkg/blockinsight7000/v1"
	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcRecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpcCtxTags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcPrometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var config struct {
	Addr     string `long:"addr" env:"API_GATEWAY_ADDR" description:"addr" default:":8000"`
	RestAddr string `long:"rest-addr" env:"API_GATEWAY_REST_ADDR" description:"rest addr" default:":8001"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic("can't initialize zap logger: " + err.Error())
	}
	defer func() {
		_ = logger.Sync()
	}()
	grpcZap.ReplaceGrpcLoggerV2(logger)
	if _, err := flags.ParseArgs(&config, os.Args); err != nil {
		logger.Fatal("Failed to parse arguments", zap.Error(err))
	}

	chain := []grpc.UnaryServerInterceptor{
		grpcRecovery.UnaryServerInterceptor(),
		grpcCtxTags.UnaryServerInterceptor(),
		grpcPrometheus.UnaryServerInterceptor,
		grpcZap.UnaryServerInterceptor(logger),
	}
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcMiddleware.ChainUnaryServer(chain...)),
	)
	grpcPrometheus.EnableHandlingTimeHistogram()
	grpcPrometheus.Register(grpcServer)

	blockinsight7000v1.RegisterExplorerServiceServer(grpcServer, transport.NewExplorerHandler())

	socket, err := net.Listen("tcp", config.Addr)
	if err != nil {
		logger.Fatal("net.Listen error", zap.Error(err))
	}
	go func() {
		if serveErr := grpcServer.Serve(socket); serveErr != nil {
			logger.Fatal("Start GRPC server", zap.Error(serveErr))
		}
	}()
	go func() {
		<-ctx.Done()
		logger.Info("Shutting down gRPC server")
		grpcServer.GracefulStop()
	}()

	mux := http.NewServeMux()

	gw := gwruntime.NewServeMux()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if err := blockinsight7000v1.RegisterExplorerServiceHandlerFromEndpoint(ctx, gw, config.Addr, opts); err != nil {
		logger.Fatal("Register explorer handler", zap.Error(err))
	}

	mux.Handle("/", gw)
	mux.Handle("/metrics", promhttp.Handler())

	s := &http.Server{
		Addr:              config.RestAddr,
		Handler:           cors.Default().Handler(mux),
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
	}
	go func() {
		<-ctx.Done()
		logger.Info("Shutting down the http server")
		if err := s.Shutdown(context.Background()); err != nil {
			logger.Error("Failed to shutdown http server", zap.Error(err))
		}
	}()

	logger.Info("Starting HTTP server", zap.String("addr", config.RestAddr))
	if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		logger.Error("Failed to listen and serve", zap.Error(err))
	}
}
