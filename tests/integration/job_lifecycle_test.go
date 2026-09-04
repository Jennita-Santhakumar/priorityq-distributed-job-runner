//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
	"github.com/KabileshRajaselvan/task-queue-system/internal/job/handlers"
	"github.com/KabileshRajaselvan/task-queue-system/internal/metrics"
	"github.com/KabileshRajaselvan/task-queue-system/internal/queue"
	"github.com/KabileshRajaselvan/task-queue-system/internal/store"
	"github.com/KabileshRajaselvan/task-queue-system/internal/worker"
	"github.com/KabileshRajaselvan/task-queue-system/tests/integration/testutil"
)

func newTestPool(s *store.Store, q queue.Queue, registry *job.Registry) *worker.Pool {
	m := metrics.New(prometheus.NewRegistry())
	logger := slog.Default()
	return worker.NewPool(worker.Config{
		Size:                 1,
		JobExecTimeout:       10 * time.Second,
		ShutdownGrace:        2 * time.Second,
		StaleReclaimAfter:    10 * time.Minute,
		StaleReclaimInterval: time.Hour, // effectively disabled for these tests
		BackoffBase:          50 * time.Millisecond,
		BackoffMax:           2 * time.Second,
	}, q, s, registry, m, logger)
}

func waitForStatus(t *testing.T, s *store.Store, jobID string, want job.Status, timeout time.Duration) *job.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := s.GetJob(context.Background(), jobID)
		require.NoError(t, err)
		if j.Status == want {
			return j
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach status %s within %s", jobID, want, timeout)
	return nil
}

func TestCreateAndRetrieveJob(t *testing.T) {
	s := testutil.NewStore(t)
	ctx := context.Background()

	j := &job.Job{
		Type:       "data_transform",
		Payload:    json.RawMessage(`{"numbers":[1,2,3],"operation":"sum"}`),
		Status:     job.StatusPending,
		Priority:   5,
		MaxRetries: 3,
	}
	require.NoError(t, s.CreateJob(ctx, j))
	require.NotEmpty(t, j.ID)

	fetched, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	require.Equal(t, "data_transform", fetched.Type)
	require.Equal(t, job.StatusPending, fetched.Status)
}

func TestJobExecutionEndToEnd(t *testing.T) {
	s := testutil.NewStore(t)
	redisClient := testutil.NewRedisClient(t)
	q := queue.NewRedisQueue(redisClient)

	registry := job.NewRegistry()
	registry.Register("data_transform", &handlers.DataTransformHandler{})

	ctx := context.Background()
	j := &job.Job{
		Type:       "data_transform",
		Payload:    json.RawMessage(`{"numbers":[1,2,3,4],"operation":"sum","fail_rate":0}`),
		Status:     job.StatusPending,
		Priority:   5,
		MaxRetries: 3,
	}
	require.NoError(t, s.CreateJob(ctx, j))
	require.NoError(t, q.Enqueue(ctx, j))

	pool := newTestPool(s, q, registry)
	poolCtx, cancel := context.WithCancel(ctx)
	pool.Start(poolCtx)
	defer func() { cancel(); pool.Stop() }()

	completed := waitForStatus(t, s, j.ID, job.StatusCompleted, 5*time.Second)
	var result struct {
		Result float64 `json:"result"`
	}
	require.NoError(t, json.Unmarshal(completed.Result, &result))
	require.Equal(t, float64(10), result.Result)

	history, err := s.ListHistory(ctx, j.ID)
	require.NoError(t, err)
	var sawCompleted bool
	for _, h := range history {
		if h.Status == string(job.StatusCompleted) {
			sawCompleted = true
		}
	}
	require.True(t, sawCompleted, "expected a completed row in job_history")
}

func TestCancelBeforePickup(t *testing.T) {
	s := testutil.NewStore(t)
	redisClient := testutil.NewRedisClient(t)
	q := queue.NewRedisQueue(redisClient)
	ctx := context.Background()

	j := &job.Job{
		Type:       "data_transform",
		Payload:    json.RawMessage(`{"numbers":[1],"operation":"sum"}`),
		Status:     job.StatusPending,
		Priority:   5,
		MaxRetries: 3,
	}
	require.NoError(t, s.CreateJob(ctx, j))

	_, err := s.CancelJob(ctx, j.ID)
	require.NoError(t, err)

	fetched, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	require.Equal(t, job.StatusCancelled, fetched.Status)

	// A cancelled job was never enqueued, so a worker pool running for a
	// moment must not produce any processing/completed history for it.
	registry := job.NewRegistry()
	registry.Register("data_transform", &handlers.DataTransformHandler{})
	pool := newTestPool(s, q, registry)
	poolCtx, cancel := context.WithCancel(ctx)
	pool.Start(poolCtx)
	time.Sleep(300 * time.Millisecond)
	cancel()
	pool.Stop()

	history, err := s.ListHistory(ctx, j.ID)
	require.NoError(t, err)
	require.Empty(t, history)
}

func TestCancelAfterPickupReturns409(t *testing.T) {
	s := testutil.NewStore(t)
	ctx := context.Background()

	j := &job.Job{
		Type:       "data_transform",
		Payload:    json.RawMessage(`{"numbers":[1],"operation":"sum"}`),
		Status:     job.StatusPending,
		Priority:   5,
		MaxRetries: 3,
	}
	require.NoError(t, s.CreateJob(ctx, j))
	require.NoError(t, s.UpdateJobProcessing(ctx, j.ID, "worker-test"))

	_, err := s.CancelJob(ctx, j.ID)
	require.ErrorIs(t, err, store.ErrCannotCancel)
}
