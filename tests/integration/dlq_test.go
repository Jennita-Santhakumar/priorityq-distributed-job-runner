//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
	"github.com/KabileshRajaselvan/task-queue-system/internal/job/handlers"
	"github.com/KabileshRajaselvan/task-queue-system/internal/queue"
	"github.com/KabileshRajaselvan/task-queue-system/tests/integration/testutil"
)

func TestRetryThenDLQ(t *testing.T) {
	s := testutil.NewStore(t)
	redisClient := testutil.NewRedisClient(t)
	q := queue.NewRedisQueue(redisClient)
	ctx := context.Background()

	registry := job.NewRegistry()
	registry.Register("data_transform", &handlers.DataTransformHandler{})

	j := &job.Job{
		Type:       "data_transform",
		Payload:    json.RawMessage(`{"numbers":[1,2],"operation":"sum","fail_rate":1.0}`),
		Status:     job.StatusPending,
		Priority:   5,
		MaxRetries: 2,
	}
	require.NoError(t, s.CreateJob(ctx, j))
	require.NoError(t, q.Enqueue(ctx, j))

	pool := newTestPool(s, q, registry)
	poolCtx, cancel := context.WithCancel(ctx)
	pool.Start(poolCtx)
	defer func() { cancel(); pool.Stop() }()

	failed := waitForStatus(t, s, j.ID, job.StatusFailed, 15*time.Second)
	require.Equal(t, 2, failed.RetryCount)

	history, err := s.ListHistory(ctx, j.ID)
	require.NoError(t, err)
	retriedCount := 0
	for _, h := range history {
		if h.Status == string(job.StatusRetried) {
			retriedCount++
		}
	}
	require.Equal(t, 2, retriedCount, "expected exactly max_retries retried history rows")

	dlq, err := s.GetDLQEntry(ctx, j.ID)
	require.NoError(t, err)
	require.NotEmpty(t, dlq.Reason)
}

func TestBackoffDelayRespected(t *testing.T) {
	s := testutil.NewStore(t)
	ctx := context.Background()

	j := &job.Job{
		Type:       "data_transform",
		Payload:    json.RawMessage(`{"numbers":[1],"operation":"sum"}`),
		Status:     job.StatusPending,
		Priority:   5,
		MaxRetries: 5,
	}
	require.NoError(t, s.CreateJob(ctx, j))

	before := time.Now()
	scheduledAt := before.Add(2 * time.Second)
	require.NoError(t, s.UpdateJobRetried(ctx, j.ID, 1, scheduledAt, "simulated failure"))

	fetched, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	require.Equal(t, job.StatusRetried, fetched.Status)
	require.NotNil(t, fetched.ScheduledAt)
	require.WithinDuration(t, scheduledAt, *fetched.ScheduledAt, 100*time.Millisecond)
	require.True(t, fetched.ScheduledAt.After(before), "retry must be scheduled in the future, not redelivered immediately")
}
