// Package store implements PostgreSQL persistence for jobs, their history,
// and the dead-letter queue.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sony/gobreaker/v2"
)

type Store struct {
	pool    *pgxpool.Pool
	breaker *gobreaker.CircuitBreaker[any]
}

// New creates a Store backed by a pgx connection pool, with a circuit
// breaker wrapping every write path so a struggling database fails fast
// instead of piling up blocked goroutines (PRD's own error-handling table
// calls for "circuit breaker pattern" on DB connection loss).
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	settings := gobreaker.Settings{
		Name:        "postgres",
		MaxRequests: 3,
		Interval:    30 * time.Second,
		Timeout:     15 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	}

	return &Store{
		pool:    pool,
		breaker: gobreaker.NewCircuitBreaker[any](settings),
	}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Exec runs an arbitrary statement, bypassing the circuit breaker. It
// exists for test setup/teardown (e.g. TRUNCATE between integration
// tests) — application code should use the typed methods elsewhere in
// this package instead.
func (s *Store) Exec(ctx context.Context, sql string) error {
	_, err := s.pool.Exec(ctx, sql)
	return err
}

// withBreaker runs fn through the circuit breaker, translating gobreaker's
// generic-return API into a plain error for callers that don't need a value.
func (s *Store) withBreaker(ctx context.Context, fn func() error) error {
	_, err := s.breaker.Execute(func() (any, error) {
		return nil, fn()
	})
	return err
}

var ErrCircuitOpen = gobreaker.ErrOpenState
