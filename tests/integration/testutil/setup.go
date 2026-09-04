// Package testutil provides shared setup for integration tests: it
// connects to real Postgres/Redis using the same DATABASE_URL/REDIS_URL
// env var convention as the sibling ab-testing-framework project's
// integration tests, and truncates tables between tests for isolation.
package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/KabileshRajaselvan/task-queue-system/internal/store"
)

func RequireEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s not set; skipping integration test", key)
	}
	return v
}

func NewStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	s, err := store.New(ctx, RequireEnv(t, "DATABASE_URL"))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	TruncateAll(t, s)
	return s
}

func NewRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	opts, err := redis.ParseURL(RequireEnv(t, "REDIS_URL"))
	require.NoError(t, err)
	client := redis.NewClient(opts)
	t.Cleanup(func() { client.Close() })
	require.NoError(t, client.FlushDB(context.Background()).Err())
	return client
}

// TruncateAll clears all job-related tables so each test starts from a
// clean slate, without needing a fresh database per test.
func TruncateAll(t *testing.T, s *store.Store) {
	t.Helper()
	require.NoError(t, s.Exec(context.Background(),
		"TRUNCATE TABLE dead_letter_queue, job_history, jobs RESTART IDENTITY CASCADE"))
}
