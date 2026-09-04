package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
)

func newTestQueue(t *testing.T) *RedisQueue {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return NewRedisQueue(client)
}

func TestEnqueueDequeuePriorityOrdering(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	ids := map[string]int{}
	for _, p := range []struct {
		id       string
		priority int
	}{
		{"low", 2}, {"high", 8}, {"mid", 5},
	} {
		j := &job.Job{ID: p.id, Type: "test_type", Priority: p.priority}
		require.NoError(t, q.Enqueue(ctx, j))
		ids[p.id] = p.priority
	}

	var order []string
	for i := 0; i < 3; i++ {
		j, err := q.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, j)
		order = append(order, j.ID)
	}

	require.Equal(t, []string{"high", "mid", "low"}, order)
}

func TestDelayedJobNotDequeuedBeforeDue(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	future := time.Now().Add(1 * time.Hour)
	j := &job.Job{ID: "delayed", Type: "test_type", Priority: 5, ScheduledAt: &future}
	require.NoError(t, q.Enqueue(ctx, j))

	got, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.Nil(t, got, "a job scheduled an hour from now must not be dequeued yet")

	depth, err := q.Depth(ctx, "test_type")
	require.NoError(t, err)
	require.Equal(t, int64(1), depth, "the not-yet-due job must be put back, not dropped")
}

func TestDelayedJobDequeuedOncePastDue(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	past := time.Now().Add(-1 * time.Minute)
	j := &job.Job{ID: "ready-now", Type: "test_type", Priority: 5, ScheduledAt: &past}
	require.NoError(t, q.Enqueue(ctx, j))

	got, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "ready-now", got.ID)
}

func TestRoundRobinAcrossTypes(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, &job.Job{ID: "a1", Type: "type_a", Priority: 5}))
	require.NoError(t, q.Enqueue(ctx, &job.Job{ID: "a2", Type: "type_a", Priority: 5}))
	require.NoError(t, q.Enqueue(ctx, &job.Job{ID: "b1", Type: "type_b", Priority: 5}))
	require.NoError(t, q.Enqueue(ctx, &job.Job{ID: "b2", Type: "type_b", Priority: 5}))

	seenTypes := map[string]int{}
	for i := 0; i < 4; i++ {
		j, err := q.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, j)
		seenTypes[j.Type]++
	}

	require.Equal(t, 2, seenTypes["type_a"])
	require.Equal(t, 2, seenTypes["type_b"])
}

func TestDequeueEmptyQueueReturnsNilNil(t *testing.T) {
	q := newTestQueue(t)
	j, err := q.Dequeue(context.Background())
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestRegisteredTypesSetTracksOnlyEverEnqueuedTypes(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, &job.Job{ID: "x", Type: "email_send", Priority: 5}))
	depths, err := q.DepthByType(ctx)
	require.NoError(t, err)
	require.Contains(t, depths, "email_send")
	require.Len(t, depths, 1)
}

func TestWorkerHeartbeat(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	count, err := q.WorkerHeartbeatCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count, "no heartbeat published yet")

	require.NoError(t, q.PublishWorkerHeartbeat(ctx, 4))
	count, err = q.WorkerHeartbeatCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, count)
}
