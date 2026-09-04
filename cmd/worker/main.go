// Command worker runs the worker pool that dequeues and executes jobs.
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/KabileshRajaselvan/task-queue-system/internal/config"
	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
	"github.com/KabileshRajaselvan/task-queue-system/internal/job/handlers"
	"github.com/KabileshRajaselvan/task-queue-system/internal/metrics"
	"github.com/KabileshRajaselvan/task-queue-system/internal/queue"
	"github.com/KabileshRajaselvan/task-queue-system/internal/store"
	"github.com/KabileshRajaselvan/task-queue-system/internal/worker"
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

	metricsSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.WorkerMetricsPort),
		Handler:           promhttp.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("worker metrics server listening", "port", cfg.WorkerMetricsPort)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server failed", "error", err)
		}
	}()

	pool := worker.NewPool(worker.Config{
		Size:                 cfg.WorkerCount,
		JobExecTimeout:       time.Duration(cfg.JobExecTimeoutSeconds) * time.Second,
		ShutdownGrace:        time.Duration(cfg.ShutdownGraceSeconds) * time.Second,
		StaleReclaimAfter:    time.Duration(cfg.StaleReclaimAfterMin) * time.Minute,
		StaleReclaimInterval: time.Duration(cfg.StaleReclaimIntervalSc) * time.Second,
		BackoffBase:          time.Duration(cfg.BackoffBaseSeconds * float64(time.Second)),
		BackoffMax:           time.Duration(cfg.BackoffMaxSeconds * float64(time.Second)),
	}, q, s, registry, m, logger)

	pool.Start(ctx)
	logger.Info("worker pool started", "size", cfg.WorkerCount)

	<-ctx.Done()
	logger.Info("shutting down worker pool")
	pool.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
}
