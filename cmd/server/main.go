// Command server runs the task queue's REST API.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/KabileshRajaselvan/task-queue-system/internal/api"
	"github.com/KabileshRajaselvan/task-queue-system/internal/config"
	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
	"github.com/KabileshRajaselvan/task-queue-system/internal/job/handlers"
	"github.com/KabileshRajaselvan/task-queue-system/internal/metrics"
	"github.com/KabileshRajaselvan/task-queue-system/internal/queue"
	"github.com/KabileshRajaselvan/task-queue-system/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}
	logger := config.NewLogger(cfg.LogFormat, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer s.Close()

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Error("invalid REDIS_URL", "error", err)
		os.Exit(1)
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()
	q := queue.NewRedisQueue(redisClient)

	registry := job.NewRegistry()
	registry.Register("data_transform", &handlers.DataTransformHandler{})
	registry.Register("image_resize", &handlers.ImageResizeHandler{})
	registry.Register("email_send", &handlers.EmailSendHandler{})

	m := metrics.New(prometheus.DefaultRegisterer)
	h := api.NewHandler(s, q, registry, m, logger)

	router := api.NewRouter(h, api.RouterConfig{
		RateLimitPerMin: cfg.RateLimitPerMin,
		RateLimitBurst:  cfg.RateLimitBurst,
		CORSOrigins:     corsOrigins(),
	}, logger)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.APIPort),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("api server listening", "port", cfg.APIPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down api server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownGraceSeconds)*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}

func corsOrigins() []string {
	if v := os.Getenv("CORS_ORIGINS"); v != "" {
		return []string{v}
	}
	return nil
}
