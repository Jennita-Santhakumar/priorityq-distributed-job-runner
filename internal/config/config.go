// Package config loads process configuration from environment variables,
// shared by cmd/server, cmd/worker, and cmd/migrate.
package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL string `env:"DATABASE_URL,required"`
	RedisURL    string `env:"REDIS_URL,required"`

	// Server
	APIPort         int `env:"API_PORT" envDefault:"8080"`
	RateLimitPerMin int `env:"RATE_LIMIT_PER_MIN" envDefault:"100"`
	RateLimitBurst  int `env:"RATE_LIMIT_BURST" envDefault:"100"`

	// Worker
	WorkerCount            int `env:"WORKER_COUNT" envDefault:"4"`
	WorkerMetricsPort      int `env:"WORKER_METRICS_PORT" envDefault:"9103"`
	JobExecTimeoutSeconds  int `env:"JOB_EXEC_TIMEOUT_SECONDS" envDefault:"300"`
	ShutdownGraceSeconds   int `env:"SHUTDOWN_GRACE_SECONDS" envDefault:"30"`
	StaleReclaimAfterMin   int `env:"STALE_RECLAIM_AFTER_MINUTES" envDefault:"10"`
	StaleReclaimIntervalSc int `env:"STALE_RECLAIM_INTERVAL_SECONDS" envDefault:"60"`

	// Retry/backoff
	BackoffBaseSeconds float64 `env:"BACKOFF_BASE_SECONDS" envDefault:"1"`
	BackoffMaxSeconds  float64 `env:"BACKOFF_MAX_SECONDS" envDefault:"300"`

	LogLevel  string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat string `env:"LOG_FORMAT" envDefault:"json"`
}

// Load loads a .env file if present (local dev convenience only — in Docker
// env vars come from compose directly, so a missing .env is not an error),
// then parses process environment into Config.
func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	return cfg, nil
}
