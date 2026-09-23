package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"worldtradefuture/indexer/internal/api"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/persistence"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting WTF On-Chain Monitoring REST API")

	if err := godotenv.Load(); err != nil {
		logger.Info("info: .env file not loaded from cwd, using environment variables", "error", err.Error())
	}

	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration loading failed", "error", err.Error())
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := persistence.NewPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("database connection failed", "error", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	// Run migrations if available
	migrationsDir := "./migrations"
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		migrationsDir = "../../migrations"
	}
	if _, err := os.Stat(migrationsDir); err == nil {
		if err := db.RunMigrations(ctx, migrationsDir); err != nil {
			logger.Warn("migration execution check warning", "error", err.Error())
		}
	}

	var rdb *redis.Client
	if cfg.RedisURL != "" {
		redisCtx, redisCancel := context.WithTimeout(context.Background(), 5*time.Second)
		var redisErr error
		rdb, redisErr = persistence.NewRedisClient(redisCtx, cfg.RedisURL, "", 0)
		redisCancel()
		if redisErr != nil {
			logger.Warn("redis connection not established, running without redis cache/pubsub", "error", redisErr.Error())
		} else {
			defer rdb.Close()
			logger.Info("connected to redis", "url", cfg.RedisURL)
		}
	}

	router := api.NewRouter(db.Pool(), &cfg, logger, rdb)

	addr := fmt.Sprintf("%s:%d", cfg.APIHost, cfg.APIPort)
	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("REST API server listening",
			"address", addr,
			"chain_id", cfg.ChainID,
			"environment", cfg.DeploymentEnvironment,
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	// Listen for shutdown signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err := <-serverErrors:
		logger.Error("server startup/runtime error", "error", err.Error())
		os.Exit(1)
	case sig := <-shutdown:
		logger.Info("shutdown signal received, initiating graceful shutdown", "signal", sig.String())

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful server shutdown failed, forcing close", "error", err.Error())
			_ = server.Close()
		}

		logger.Info("server shutdown completed successfully")
	}
}
