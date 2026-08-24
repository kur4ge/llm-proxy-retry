package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"llm-proxy-retry/internal/config"
	"llm-proxy-retry/internal/logging"
	"llm-proxy-retry/internal/proxy"
)

func main() {
	configFile := flag.String("config", "config.yaml", "path to the YAML configuration file")
	flag.Parse()

	bootstrapLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(*configFile)
	if err != nil {
		bootstrapLogger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger, err := logging.New(os.Stdout, cfg.Logging.Level, cfg.Logging.Format)
	if err != nil {
		bootstrapLogger.Error("failed to initialize logging", "error", err)
		os.Exit(1)
	}
	logger = logger.With("service", "llm-proxy")
	slog.SetDefault(logger)

	handler, err := proxy.New(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize proxy", "error", err)
		os.Exit(1)
	}
	defer handler.CloseIdleConnections()

	server := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration,
		IdleTimeout:       cfg.Server.IdleTimeout.Duration,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	shutdownSignal, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	logger.Info("proxy listening",
		"address", cfg.Server.Listen,
		"log_level", cfg.Logging.Level,
		"log_format", cfg.Logging.Format,
	)
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
		return
	case <-shutdownSignal.Done():
	}
	stopSignals()

	logger.Info("shutting down")
	shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		_ = server.Close()
	}
	if err := <-serverErrors; err != nil && err != http.ErrServerClosed {
		logger.Error("server stopped with an error", "error", err)
	}
}
